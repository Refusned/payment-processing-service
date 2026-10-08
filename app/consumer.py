import asyncio
import logging

import httpx
from faststream import FastStream
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import RabbitMessage

from app.config import settings
from app.db import engine, session_factory
from app.gateway import EmulatedGateway
from app.messaging import (
    ATTEMPT_HEADER,
    EVENT_ID_HEADER,
    MAX_ATTEMPTS,
    attempt_of,
    build_broker,
    declare_topology,
    exchange,
    new_queue,
    reliable_publish,
    retry_queue_after,
)
from app.processing import PaymentNotFound, PaymentProcessor, WebhookAttemptFailed
from app.schemas import PaymentCreatedEvent
from app.webhooks import WebhookSender

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("app.consumer")

# Самая долгая попытка: шлюз 5 c, webhook 10 c, публикация повтора 5 c.
broker = build_broker(prefetch=settings.consumer_prefetch, graceful_timeout=25)
app = FastStream(broker)

http_client = httpx.AsyncClient(
    timeout=httpx.Timeout(settings.webhook_read_timeout, connect=settings.webhook_connect_timeout),
    follow_redirects=False,
)
processor = PaymentProcessor(
    session_factory,
    EmulatedGateway.from_settings(settings),
    WebhookSender(http_client, settings.webhook_deadline),
)


@app.on_startup
async def setup_topology() -> None:
    # Retry-очереди и DLQ должны существовать до того, как подписчик возьмёт первое сообщение.
    await broker.connect()
    await declare_topology(broker)


@app.after_shutdown
async def close_resources() -> None:
    await http_client.aclose()
    await engine.dispose()


# Политика подтверждения по умолчанию (REJECT_ON_ERROR): успешный выход из обработчика
# даёт ack, RejectMessage и любое необработанное исключение дают reject без requeue,
# и брокер переносит сообщение в payments.dlq. Сюда же попадает сообщение, которое
# не разобралось в PaymentCreatedEvent.
@broker.subscriber(new_queue, exchange)
async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
    logger.info("processing payment %s, event %s", event.payment_id, event.event_id)
    try:
        await processor.process(event.payment_id)
    except PaymentNotFound:
        logger.error("payment %s not found, event %s goes to DLQ", event.payment_id, event.event_id)
        raise RejectMessage() from None
    except WebhookAttemptFailed as exc:
        # Номер попытки берётся из БД: его делят все копии события.
        await _retry_or_reject(event, exc.attempt, str(exc))
    except Exception as exc:
        # До попытки доставки не дошли (например, недоступна БД): считаем по заголовку сообщения.
        logger.exception("payment %s: processing failed", event.payment_id)
        await _retry_or_reject(event, attempt_of(message.headers), f"{type(exc).__name__}: {exc}")


async def _retry_or_reject(event: PaymentCreatedEvent, attempt: int, error: str) -> None:
    retry_queue = retry_queue_after(attempt)
    if retry_queue is None:
        logger.error(
            "payment %s: attempt %d/%d failed (%s), event %s goes to DLQ",
            event.payment_id,
            attempt,
            MAX_ATTEMPTS,
            error,
            event.event_id,
        )
        raise RejectMessage()

    logger.warning(
        "payment %s: attempt %d/%d failed (%s), retrying via %s",
        event.payment_id,
        attempt,
        MAX_ATTEMPTS,
        error,
        retry_queue.name,
    )
    try:
        # Сначала подтверждённая публикация копии, потом ack оригинала (выходом из обработчика).
        # Падение между ними даст дубль, а не потерю.
        await reliable_publish(
            broker,
            event.model_dump(mode="json"),
            retry_queue.routing_key,
            {ATTEMPT_HEADER: str(attempt + 1), EVENT_ID_HEADER: str(event.event_id)},
        )
    except Exception:
        # Повтор не запланирован, возвращаем сообщение в очередь. Попытка доставки уже
        # учтена в БД, поэтому повторная доставка не увеличит общее число обращений.
        logger.exception("could not schedule retry for payment %s, requeueing", event.payment_id)
        await asyncio.sleep(1)
        raise NackMessage() from None
