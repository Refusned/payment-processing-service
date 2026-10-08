import asyncio
import logging

import httpx
from aio_pika.abc import HeadersType
from faststream import FastStream
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import RabbitMessage

from app.config import settings
from app.db import engine, session_factory
from app.gateway import EmulatedGateway
from app.messaging import (
    ATTEMPT_HEADER,
    EVENT_ID_HEADER,
    INFRA_RETRIES_HEADER,
    MAX_ATTEMPTS,
    MAX_INFRA_RETRIES,
    RETRY_DELAYS,
    build_broker,
    declare_topology,
    exchange,
    header_int,
    new_queue,
    reliable_publish,
    retry_queue_for,
)
from app.processing import AttemptFailed, Delivered, GaveUp, NotDue, PaymentNotFound, PaymentProcessor
from app.schemas import PaymentCreatedEvent
from app.webhooks import WebhookSender

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("app.consumer")

# Самая долгая обработка: шлюз 5 c, webhook 10 c, публикация повтора 5 c.
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
    attempt_lease=settings.webhook_deadline + 5,
)


@app.on_startup
async def setup_topology() -> None:
    # Retry-очереди и DLQ должны существовать до того, как подписчик возьмёт первое сообщение.
    async with asyncio.timeout(30):
        await broker.connect()
        await declare_topology(broker)


@app.after_shutdown
async def close_resources() -> None:
    await http_client.aclose()
    await engine.dispose()


# REJECT_ON_ERROR (по умолчанию): выход из обработчика даёт ack, RejectMessage и исключение
# дают reject, и сообщение уходит в payments.dlq. Туда же попадает тело, которое не разобралось.
@broker.subscriber(new_queue, exchange)
async def handle_payment_created(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
    payment_id = event.payment_id
    attempt = header_int(message.headers, ATTEMPT_HEADER, 1)
    logger.info("processing payment %s, event %s", payment_id, event.event_id)
    try:
        outcome = await processor.process(payment_id)
    except PaymentNotFound:
        logger.error("payment %s not found, event %s goes to DLQ", payment_id, event.event_id)
        raise RejectMessage() from None
    except Exception as exc:
        # До решения о доставке дело не дошло (например, недоступна БД). Попытки доставки
        # на это не тратим, ждём восстановления со своим лимитом.
        logger.exception("payment %s: processing failed", payment_id)
        retries = header_int(message.headers, INFRA_RETRIES_HEADER, 0)
        if retries >= MAX_INFRA_RETRIES:
            logger.error("payment %s: %s, event %s goes to DLQ", payment_id, exc, event.event_id)
            raise RejectMessage() from None
        await _wake_up_later(event, RETRY_DELAYS[-1], attempt, infra_retries=retries + 1)
        return

    match outcome:
        case Delivered(already=False):
            logger.info("payment %s: webhook delivered", payment_id)
        case Delivered(already=True):
            logger.info("payment %s: webhook already delivered, duplicate event acked", payment_id)
        case GaveUp(error=error):
            logger.error(
                "payment %s: webhook not delivered (%s), event %s goes to DLQ",
                payment_id,
                error,
                event.event_id,
            )
            raise RejectMessage()
        case NotDue(delay=delay):
            logger.info("payment %s: next webhook attempt is not due yet, waiting %.1fs", payment_id, delay)
            await _wake_up_later(event, delay, attempt)
        case AttemptFailed(attempt=failed, error=error, retry_in=delay):
            logger.warning(
                "payment %s: webhook attempt %d/%d failed (%s), next in %ss",
                payment_id,
                failed,
                MAX_ATTEMPTS,
                error,
                delay,
            )
            await _wake_up_later(event, delay, failed + 1)


async def _wake_up_later(
    event: PaymentCreatedEvent, delay: float, attempt: int, infra_retries: int = 0
) -> None:
    """Вернуть событие в обработку не раньше чем через delay секунд, через retry-очередь."""
    queue = retry_queue_for(delay)
    headers: HeadersType = {
        ATTEMPT_HEADER: str(attempt),
        EVENT_ID_HEADER: str(event.event_id),
        INFRA_RETRIES_HEADER: str(infra_retries),
    }
    try:
        # Сначала подтверждённая публикация копии, потом ack оригинала (выходом из обработчика).
        # Падение между ними даст дубль, а не потерю.
        await reliable_publish(broker, event.model_dump(mode="json"), queue.routing_key, headers)
    except Exception:
        # Возвращаем сообщение в очередь. Это не новая попытка: время следующей записано
        # в БД, и раньше него запрос к получателю не уйдёт.
        logger.exception("could not schedule payment %s via %s, requeueing", event.payment_id, queue.name)
        await asyncio.sleep(1)
        raise NackMessage() from None
