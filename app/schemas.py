import json
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field, HttpUrl, UrlConstraints

from app.models import Currency, PaymentStatus, WebhookStatus

MAX_METADATA_BYTES = 16 * 1024


def _empty_if_null(value: Any) -> Any:
    return {} if value is None else value


def _limit_size(value: dict[str, Any]) -> dict[str, Any]:
    if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
        raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} bytes")
    return value


Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, examples=["199.99"])]
Metadata = Annotated[dict[str, Any], BeforeValidator(_empty_if_null), AfterValidator(_limit_size)]
WebhookUrl = Annotated[HttpUrl, UrlConstraints(max_length=2048)]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str | None = Field(default=None, max_length=1024)
    metadata: Metadata = Field(default_factory=dict)
    webhook_url: WebhookUrl


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] = Field(validation_alias="payment_metadata")
    status: PaymentStatus
    failure_reason: str | None
    idempotency_key: str
    webhook_url: str
    webhook_status: WebhookStatus
    webhook_attempts: int
    created_at: datetime
    processed_at: datetime | None
    updated_at: datetime


class PaymentCreatedEvent(BaseModel):
    event_id: uuid.UUID
    payment_id: uuid.UUID
