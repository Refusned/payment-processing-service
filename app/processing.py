import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway import ChargeResult, PaymentGateway
from app.messaging import MAX_ATTEMPTS, RETRY_DELAYS
from app.models import Payment, PaymentStatus, WebhookStatus
from app.webhooks import WebhookDeliveryError, WebhookSender, payment_event

logger = logging.getLogger(__name__)


class PaymentNotFound(Exception):
    pass


@dataclass(frozen=True)
class Delivered:
    already: bool = False


@dataclass(frozen=True)
class GaveUp:
    """Попытки доставки исчерпаны, событие должно уйти в DLQ."""

    error: str | None


@dataclass(frozen=True)
class NotDue:
    """Следующую попытку начинать рано: идёт пауза после ошибки или чужая попытка."""

    delay: float


@dataclass(frozen=True)
class AttemptFailed:
    attempt: int
    error: str
    retry_in: float


Outcome = Delivered | GaveUp | NotDue | AttemptFailed


class PaymentProcessor:
    """Доводит платёж до результата и уведомляет клиента. Безопасен для повторной доставки события:
    результат эмулятора пишется один раз, расписание попыток webhook хранится в строке платежа.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhooks: WebhookSender,
        attempt_lease: float,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks
        # Сколько попытка считается идущей. Если процесс упал посреди запроса, следующая
        # копия события начнёт новую попытку не раньше, чем истечёт этот срок.
        self._lease = timedelta(seconds=attempt_lease)

    async def process(self, payment_id: uuid.UUID) -> Outcome:
        payment = await self._load(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)

        if payment.status is PaymentStatus.PENDING:
            # Шлюз отвечает 2-5 c, поэтому вызывается вне транзакции.
            result = await self._gateway.charge(payment)
            await self._save_charge_result(payment_id, result)

        claimed = await self._claim_attempt(payment_id)
        if not isinstance(claimed, Payment):
            return claimed

        attempt = claimed.webhook_attempts
        try:
            await self._webhooks.send(claimed.webhook_url, payment_event(claimed))
        except WebhookDeliveryError as exc:
            return await self._record_failure(payment_id, attempt, str(exc))

        await self._record_delivery(payment_id)
        return Delivered()

    async def _load(self, payment_id: uuid.UUID) -> Payment | None:
        async with self._session_factory() as session:
            return await session.scalar(select(Payment).where(Payment.id == payment_id))

    async def _save_charge_result(self, payment_id: uuid.UUID, result: ChargeResult) -> None:
        # Если копия события успела первой, UPDATE ничего не изменит и останется её результат.
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(
                    status=PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED,
                    failure_reason=result.failure_reason,
                    processed_at=func.now(),
                    updated_at=func.now(),
                )
            )

    async def _claim_attempt(self, payment_id: uuid.UUID) -> Payment | Delivered | GaveUp | NotDue:
        async with self._session_factory() as session, session.begin():
            row = (
                await session.execute(
                    select(Payment, func.clock_timestamp()).where(Payment.id == payment_id).with_for_update()
                )
            ).one_or_none()
            if row is None:
                raise PaymentNotFound(payment_id)
            payment, now = row

            if payment.webhook_status is WebhookStatus.DELIVERED:
                return Delivered(already=True)
            if payment.webhook_status is WebhookStatus.FAILED:
                return GaveUp(payment.webhook_last_error)
            if payment.status is PaymentStatus.PENDING:
                raise RuntimeError(f"payment {payment_id} has no result yet")

            if payment.webhook_next_attempt_at is not None and payment.webhook_next_attempt_at > now:
                return NotDue((payment.webhook_next_attempt_at - now).total_seconds())

            if payment.webhook_attempts >= MAX_ATTEMPTS:
                # Последняя попытка началась, но её результат не записан (процесс упал).
                payment.webhook_status = WebhookStatus.FAILED
                payment.updated_at = now
                return GaveUp(payment.webhook_last_error)

            payment.webhook_attempts += 1
            payment.webhook_next_attempt_at = now + self._lease
            payment.updated_at = now
            return payment

    async def _record_failure(self, payment_id: uuid.UUID, attempt: int, error: str) -> Outcome:
        final = attempt >= MAX_ATTEMPTS
        delay = 0 if final else RETRY_DELAYS[attempt - 1]
        values = {
            "webhook_last_error": error,
            "webhook_next_attempt_at": func.clock_timestamp() + timedelta(seconds=delay),
            "updated_at": func.now(),
        }
        if final:
            values["webhook_status"] = WebhookStatus.FAILED

        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.webhook_status == WebhookStatus.PENDING,
                    # Пока шёл запрос, срок попытки мог истечь и начаться следующая.
                    # Тогда этот результат устарел, и записывать его нельзя.
                    Payment.webhook_attempts == attempt,
                )
                .values(**values)
            )
        if result.rowcount == 0:  # type: ignore[attr-defined]
            return NotDue(RETRY_DELAYS[0])
        if final:
            return GaveUp(error)
        return AttemptFailed(attempt, error, retry_in=delay)

    async def _record_delivery(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.webhook_status == WebhookStatus.PENDING)
                .values(
                    webhook_status=WebhookStatus.DELIVERED,
                    webhook_delivered_at=func.now(),
                    webhook_last_error=None,
                    webhook_next_attempt_at=None,
                    updated_at=func.now(),
                )
            )
        if result.rowcount == 0:  # type: ignore[attr-defined]
            # Запрос шёл дольше срока попытки, и другая копия события уже завершила доставку.
            logger.warning("payment %s: webhook delivered after the attempt lease expired", payment_id)
