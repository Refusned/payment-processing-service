import uuid
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway import ChargeResult, PaymentGateway
from app.models import Payment, PaymentStatus, WebhookStatus
from app.webhooks import WebhookSender, payment_event


class PaymentNotFound(Exception):
    pass


class PaymentProcessor:
    """Доводит платёж до финального статуса и уведомляет клиента.

    Повторный вызов для того же платежа безопасен: результат списания фиксируется
    условным UPDATE ровно один раз, а webhook не отправляется, если доставка уже
    завершена (успешно или окончательно неуспешно).

    Каждый шаг работает в своей короткой транзакции. Вызов шлюза (2-5 с) и HTTP-запрос
    к получателю идут вне транзакций, чтобы не держать соединение и блокировку строки.
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
            result = await self._gateway.charge(payment)
            # None означает, что другой обработчик того же события успел первым:
            # берём его результат, свой отбрасываем.
            payment = await self._save_charge_result(payment_id, result) or await self._load(payment_id)
            if payment is None:
                raise PaymentNotFound(payment_id)

        if payment.webhook_status is not WebhookStatus.PENDING:
            return

        event = payment_event(payment)
        await self._update_webhook(payment_id, webhook_attempts=Payment.webhook_attempts + 1)
        await self._webhooks.send(payment.webhook_url, event)
        await self._update_webhook(
            payment_id,
            webhook_status=WebhookStatus.DELIVERED,
            webhook_delivered_at=func.now(),
            webhook_last_error=None,
        )

    async def record_failure(self, payment_id: uuid.UUID, error: str, *, final: bool) -> None:
        values: dict[str, Any] = {"webhook_last_error": error[:1000]}
        if final:
            values["webhook_status"] = WebhookStatus.FAILED
        await self._update_webhook(payment_id, **values)

    async def _load(self, payment_id: uuid.UUID) -> Payment | None:
        async with self._session_factory() as session:
            return await session.scalar(select(Payment).where(Payment.id == payment_id))

    async def _save_charge_result(self, payment_id: uuid.UUID, result: ChargeResult) -> Payment | None:
        async with self._session_factory() as session, session.begin():
            return await session.scalar(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(
                    status=PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED,
                    failure_reason=result.failure_reason,
                    processed_at=func.now(),
                    updated_at=func.now(),
                )
                .returning(Payment)
            )

    async def _update_webhook(self, payment_id: uuid.UUID, **values: Any) -> None:
        # Состояние доставки меняется только из pending: delivered и failed терминальные,
        # поздний дубль события не может их перезаписать.
        async with self._session_factory() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.webhook_status == WebhookStatus.PENDING)
                .values(**values, updated_at=func.now())
            )
