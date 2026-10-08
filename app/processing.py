import uuid
from datetime import UTC, datetime

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway import ChargeResult, PaymentGateway
from app.messaging import MAX_ATTEMPTS
from app.models import Payment, PaymentStatus, WebhookStatus
from app.webhooks import WebhookDeliveryError, WebhookSender, payment_event


class PaymentNotFound(Exception):
    pass


class WebhookAttemptFailed(Exception):
    """Попытка доставки не удалась и уже учтена в БД."""

    def __init__(self, attempt: int, error: str) -> None:
        super().__init__(error)
        self.attempt = attempt


class PaymentProcessor:
    """Доводит платёж до финального статуса и уведомляет клиента.

    Обработчик может получить одно событие дважды (outbox публикует at-least-once).
    Списание фиксируется условным UPDATE, так что статус меняется ровно один раз.
    Webhook отправляется под блокировкой строки платежа, а счётчик попыток хранится
    в той же строке: параллельные дубли отправляют по очереди и делят общий бюджет
    из MAX_ATTEMPTS попыток.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        webhooks: WebhookSender,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks

    async def process(self, payment_id: uuid.UUID) -> None:
        payment = await self._load(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)

        if payment.status is PaymentStatus.PENDING:
            # Шлюз отвечает 2-5 c, поэтому вызывается вне транзакции.
            result = await self._gateway.charge(payment)
            await self._save_charge_result(payment_id, result)

        await self._notify(payment_id)

    async def _load(self, payment_id: uuid.UUID) -> Payment | None:
        async with self._session_factory() as session:
            return await session.scalar(select(Payment).where(Payment.id == payment_id))

    async def _save_charge_result(self, payment_id: uuid.UUID, result: ChargeResult) -> None:
        # Если дубль события успел первым, UPDATE ничего не изменит и останется его результат.
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

    async def _notify(self, payment_id: uuid.UUID) -> None:
        failure: WebhookAttemptFailed | None = None

        # Блокировка строки держится на время запроса к получателю (не дольше webhook_deadline):
        # это право на попытку. Дубль ждёт и после разблокировки видит итог предыдущей.
        # Если процесс упадёт, соединение с БД закроется и блокировка снимется сама.
        async with self._session_factory() as session, session.begin():
            payment = await session.scalar(select(Payment).where(Payment.id == payment_id).with_for_update())
            if payment is None:
                raise PaymentNotFound(payment_id)
            if payment.webhook_status is not WebhookStatus.PENDING:
                return
            if payment.status is PaymentStatus.PENDING:
                raise RuntimeError(f"payment {payment_id} has no result yet")

            payment.webhook_attempts += 1
            payment.updated_at = datetime.now(UTC)
            try:
                await self._webhooks.send(payment.webhook_url, payment_event(payment))
            except WebhookDeliveryError as exc:
                payment.webhook_last_error = str(exc)
                if payment.webhook_attempts >= MAX_ATTEMPTS:
                    payment.webhook_status = WebhookStatus.FAILED
                failure = WebhookAttemptFailed(payment.webhook_attempts, str(exc))
            else:
                payment.webhook_status = WebhookStatus.DELIVERED
                payment.webhook_delivered_at = datetime.now(UTC)
                payment.webhook_last_error = None

        if failure is not None:
            raise failure
