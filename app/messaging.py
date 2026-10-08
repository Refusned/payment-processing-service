"""Топология RabbitMQ и надёжная публикация.

    exchange payments (direct)
      payments.new          -> consumer
      payments.retry.2s     TTL 2 c, по истечении -> payments.new
      payments.retry.4s     TTL 4 c, по истечении -> payments.new
    exchange payments.dlx (direct)
      payments.dlq          сообщения, отклонённые после последней попытки

Задержка повтора делается временем жизни сообщения в очереди без потребителя.
Своя очередь на каждую задержку, а не одна общая с TTL на сообщении: в общей
очереди сообщение с длинной задержкой в голове держало бы все следующие.

Все очереди quorum со стратегией dead-lettering at-least-once. У классических
очередей dead-lettering at-most-once, и сообщение может потеряться при переходе
из retry-очереди в рабочую или из рабочей в DLQ.
"""

import uuid
from collections.abc import Mapping
from typing import Any

from aio_pika.abc import HeadersType
from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel
from faststream.rabbit.schemas.queue import QuorumQueueArgs

from app.config import settings

ATTEMPT_HEADER = "x-attempt"
EVENT_ID_HEADER = "x-event-id"

NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.dlq"

RETRY_DELAYS = (2, 4)
MAX_ATTEMPTS = len(RETRY_DELAYS) + 1

exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)


def _dead_lettering(target_exchange: str, routing_key: str) -> QuorumQueueArgs:
    return {
        "x-dead-letter-exchange": target_exchange,
        "x-dead-letter-routing-key": routing_key,
        "x-dead-letter-strategy": "at-least-once",
        # at-least-once работает только вместе с reject-publish.
        "x-overflow": "reject-publish",
    }


def _retry_queue(delay: int) -> RabbitQueue:
    arguments = _dead_lettering(exchange.name, NEW_ROUTING_KEY)
    arguments["x-message-ttl"] = delay * 1000
    return RabbitQueue(
        f"payments.retry.{delay}s",
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=f"payments.retry.{delay}s",
        arguments=arguments,
    )


new_queue = RabbitQueue(
    "payments.new",
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments=_dead_lettering(dead_letter_exchange.name, DLQ_ROUTING_KEY),
)
retry_queues = tuple(_retry_queue(delay) for delay in RETRY_DELAYS)

dead_letter_queue = RabbitQueue(
    "payments.dlq", queue_type=QueueType.QUORUM, durable=True, routing_key=DLQ_ROUTING_KEY
)


def build_broker(prefetch: int | None = None) -> RabbitBroker:
    # on_return_raises: сообщение, которое не попало ни в одну очередь (нет binding),
    # приводит к исключению, а не тихо возвращается. Иначе outbox пометил бы его отправленным.
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=prefetch, on_return_raises=True),
    )


async def declare_topology(broker: RabbitBroker) -> None:
    """Объявить обменники, очереди и привязки. Идемпотентно, вызывают и api, и consumer."""
    main = await broker.declare_exchange(exchange)
    dlx = await broker.declare_exchange(dead_letter_exchange)

    # declare_queue не создаёт binding, его надо делать явно.
    for queue in (new_queue, *retry_queues):
        declared = await broker.declare_queue(queue)
        await declared.bind(main, routing_key=queue.routing_key)

    declared = await broker.declare_queue(dead_letter_queue)
    await declared.bind(dlx, routing_key=DLQ_ROUTING_KEY)


def attempt_of(headers: Mapping[str, Any]) -> int:
    try:
        return max(int(headers.get(ATTEMPT_HEADER, 1)), 1)
    except (TypeError, ValueError):
        return 1


def retry_queue_after(attempt: int) -> RabbitQueue | None:
    """Очередь, через которую пойдёт следующая попытка, или None, если попытки исчерпаны."""
    if attempt < MAX_ATTEMPTS:
        return retry_queues[attempt - 1]
    return None


async def reliable_publish(
    broker: RabbitBroker, body: dict[str, Any], routing_key: str, headers: HeadersType
) -> None:
    """Опубликовать сообщение и дождаться подтверждения брокера.

    Бросает исключение, если брокер не подтвердил приём, сообщение не нашло очередь
    или подтверждение не пришло за publish_timeout.
    """
    await broker.publish(
        body,
        exchange=exchange,
        routing_key=routing_key,
        headers=headers,
        persist=True,
        mandatory=True,
        timeout=settings.publish_timeout,
        # Транспортный id уникален на каждую публикацию: aiormq сопоставляет возвраты
        # по message_id и путает одновременные публикации с одинаковым id.
        # Стабильный id события едет в заголовке x-event-id.
        message_id=uuid.uuid4().hex,
    )
