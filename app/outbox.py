import asyncio
import contextlib
import logging
import time
from datetime import UTC, datetime, timedelta

from aiormq.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.messaging import (
    ATTEMPT_HEADER,
    EVENT_ID_HEADER,
    NEW_ROUTING_KEY,
    declare_topology,
    reliable_publish,
)
from app.models import OutboxEvent

logger = logging.getLogger(__name__)

MAX_EVENT_BACKOFF = 60
MAX_LOOP_BACKOFF = 30
CONNECT_TIMEOUT = 30
# Цикл обновляет heartbeat на каждой итерации. Если он молчит дольше, релей считается зависшим.
STALL_AFTER = 60


class OutboxRelay:
    """Переносит события из таблицы outbox в RabbitMQ.

    Событие помечается опубликованным только после подтверждения брокера и в той же
    транзакции, которая держит строку. Падение между подтверждением и коммитом
    приведёт к повторной публикации: гарантия at-least-once, consumer к дублям готов.

    Подключение к брокеру тоже делает релей, с повторами. Поэтому api стартует и принимает
    платежи, даже если RabbitMQ в этот момент недоступен.
    """

    def __init__(self, broker: RabbitBroker, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._broker = broker
        self._session_factory = session_factory
        self._task: asyncio.Task[None] | None = None
        self._heartbeat = time.monotonic()
        self._connected = False

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="outbox-relay")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task

    def is_alive(self) -> bool:
        return (
            self._task is not None
            and not self._task.done()
            and time.monotonic() - self._heartbeat < STALL_AFTER
        )

    async def _run(self) -> None:
        failures = 0
        while True:
            self._heartbeat = time.monotonic()
            try:
                if not self._connected:
                    # Без срока подключение к узлу, который принял TCP, но молчит, висело бы вечно.
                    async with asyncio.timeout(CONNECT_TIMEOUT):
                        await self._broker.connect()
                        await declare_topology(self._broker)
                    self._connected = True
                published = await self.publish_batch()
            except Exception:
                failures += 1
                delay = min(2**failures, MAX_LOOP_BACKOFF)
                logger.exception("outbox relay iteration failed, next try in %ss", delay)
                await asyncio.sleep(delay)
                continue
            failures = 0
            if published == 0:
                await asyncio.sleep(settings.outbox_poll_interval)

    async def publish_batch(self) -> int:
        async with self._session_factory() as session, session.begin():
            events = (
                await session.scalars(
                    select(OutboxEvent)
                    .where(OutboxEvent.published_at.is_(None), OutboxEvent.next_attempt_at <= func.now())
                    .order_by(OutboxEvent.created_at)
                    .limit(settings.outbox_batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()

            published = 0
            for event in events:
                self._heartbeat = time.monotonic()
                try:
                    await reliable_publish(
                        self._broker,
                        event.payload,
                        NEW_ROUTING_KEY,
                        {ATTEMPT_HEADER: "1", EVENT_ID_HEADER: str(event.id)},
                    )
                except DeliveryError as exc:
                    # Брокер отказался принять именно это сообщение. Остальные пробуем.
                    self._postpone(event, exc)
                except Exception as exc:
                    # Брокер недоступен или не ответил: перебирать пачку дальше бессмысленно.
                    self._postpone(event, exc)
                    break
                else:
                    event.published_at = datetime.now(UTC)
                    published += 1
        return published

    @staticmethod
    def _postpone(event: OutboxEvent, exc: Exception) -> None:
        event.attempts += 1
        event.last_error = f"{type(exc).__name__}: {exc}"[:1000]
        delay = min(2**event.attempts, MAX_EVENT_BACKOFF)
        event.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
        logger.warning(
            "outbox event %s not published (attempt %d), retry in %ss: %s",
            event.id,
            event.attempts,
            delay,
            event.last_error,
        )
