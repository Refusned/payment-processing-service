import secrets
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_session
from app.models import Payment
from app.payments import IdempotencyKeyReused, create_payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut

MAX_IDEMPOTENCY_KEY_LENGTH = 255

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(api_key: Annotated[str | None, Security(api_key_header)]) -> None:
    if api_key is None or not secrets.compare_digest(api_key.encode(), settings.api_key.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")


router = APIRouter(prefix="/api/v1", tags=["payments"], dependencies=[Depends(require_api_key)])

SessionDep = Annotated[AsyncSession, Depends(get_session)]


@router.post(
    "/payments",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={
        400: {"description": "Missing or invalid Idempotency-Key header"},
        401: {"description": "Invalid or missing API key"},
        422: {"description": "Invalid body, or Idempotency-Key reused with a different body"},
    },
)
async def create_payment_endpoint(
    body: PaymentCreate,
    response: Response,
    session: SessionDep,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> PaymentAccepted:
    if not idempotency_key or len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Idempotency-Key header is required (1-{MAX_IDEMPOTENCY_KEY_LENGTH} characters)",
        )
    try:
        result = await create_payment(session, body, idempotency_key)
    except IdempotencyKeyReused:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "Idempotency-Key was already used with a different request body",
        ) from None

    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return result.response


@router.get(
    "/payments/{payment_id}",
    response_model=PaymentOut,
    responses={401: {"description": "Invalid or missing API key"}, 404: {"description": "Not found"}},
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return payment
