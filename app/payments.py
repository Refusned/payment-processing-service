import hashlib
import json
import uuid
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment, PaymentStatus
from app.schemas import PaymentAccepted, PaymentCreate, PaymentCreatedEvent

PAYMENT_CREATED = "payment.created"


class IdempotencyKeyReused(Exception):
    """Ключ уже использован с другим телом запроса."""


@dataclass(frozen=True)
class CreateResult:
    response: PaymentAccepted
    replayed: bool


def request_fingerprint(body: PaymentCreate) -> str:
    data = body.model_dump(mode="json")
    # "100", 100 и "100.00" это одна и та же сумма, хэш не должен их различать.
    data["amount"] = str(body.amount.quantize(Decimal("0.01")))
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(session: AsyncSession, body: PaymentCreate, idempotency_key: str) -> CreateResult:
    fingerprint = request_fingerprint(body)
    payment_id = uuid.uuid4()

    created_at = (
        await session.execute(
            insert(Payment)
            .values(
                id=payment_id,
                amount=body.amount,
                currency=body.currency,
                description=body.description,
                payment_metadata=body.metadata,
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                webhook_url=str(body.webhook_url),
            )
            # Параллельный запрос с тем же ключом ждёт здесь коммита первого и получает
            # пустой результат, без IntegrityError и отката транзакции.
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment.created_at)
        )
    ).scalar_one_or_none()

    if created_at is not None:
        # Событие пишется в той же транзакции, что и платёж: либо есть оба, либо ничего.
        event_id = uuid.uuid4()
        session.add(
            OutboxEvent(
                id=event_id,
                aggregate_type="payment",
                aggregate_id=payment_id,
                event_type=PAYMENT_CREATED,
                payload=PaymentCreatedEvent(event_id=event_id, payment_id=payment_id).model_dump(mode="json"),
            )
        )
        await session.commit()
        accepted = PaymentAccepted(payment_id=payment_id, status=PaymentStatus.PENDING, created_at=created_at)
        return CreateResult(accepted, replayed=False)

    existing = (
        await session.execute(
            select(Payment.id, Payment.request_hash, Payment.created_at).where(
                Payment.idempotency_key == idempotency_key
            )
        )
    ).one()
    if existing.request_hash != fingerprint:
        raise IdempotencyKeyReused(idempotency_key)

    # Повтор получает исходный ответ. Текущий статус платежа отдаёт GET.
    accepted = PaymentAccepted(
        payment_id=existing.id, status=PaymentStatus.PENDING, created_at=existing.created_at
    )
    return CreateResult(accepted, replayed=True)
