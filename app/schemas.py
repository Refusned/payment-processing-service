import json
import math
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field, HttpUrl

from app.models import Currency, PaymentStatus, WebhookStatus

MAX_METADATA_BYTES = 16 * 1024
MAX_METADATA_DEPTH = 32
MAX_URL_LENGTH = 2048


def _storable_text(value: str) -> str:
    # PostgreSQL не хранит NUL ни в text, ни в jsonb, а одиночный суррогат не кодируется в UTF-8.
    if "\x00" in value:
        raise ValueError("must not contain NUL characters")
    try:
        value.encode()
    except UnicodeEncodeError:
        raise ValueError("must be valid UTF-8 text") from None
    return value


def _check_json(value: Any, depth: int = 0) -> None:
    if depth > MAX_METADATA_DEPTH:
        raise ValueError(f"must not be nested deeper than {MAX_METADATA_DEPTH} levels")
    if isinstance(value, dict):
        for key, item in value.items():
            _storable_text(key)
            _check_json(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            _check_json(item, depth + 1)
    elif isinstance(value, str):
        _storable_text(value)
    elif isinstance(value, float) and not math.isfinite(value):
        # json.loads пропускает NaN, Infinity и 1e400, а jsonb их не принимает.
        raise ValueError("numbers must be finite")


def _empty_if_null(value: Any) -> Any:
    return {} if value is None else value


def _valid_metadata(value: dict[str, Any]) -> dict[str, Any]:
    _check_json(value)
    if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
        raise ValueError(f"must not exceed {MAX_METADATA_BYTES} bytes")
    return value


def _url_fits(value: HttpUrl) -> HttpUrl:
    # Длину проверяем после нормализации: домен в punycode и %-кодирование пути её увеличивают.
    if len(str(value)) > MAX_URL_LENGTH:
        raise ValueError(f"must not exceed {MAX_URL_LENGTH} characters")
    return value


Amount = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, examples=["199.99"])]
Description = Annotated[str, Field(max_length=1024), AfterValidator(_storable_text)]
Metadata = Annotated[dict[str, Any], BeforeValidator(_empty_if_null), AfterValidator(_valid_metadata)]
WebhookUrl = Annotated[HttpUrl, AfterValidator(_url_fits)]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: Description | None = None
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
    webhook_last_error: str | None
    webhook_delivered_at: datetime | None
    created_at: datetime
    processed_at: datetime | None
    updated_at: datetime


class PaymentCreatedEvent(BaseModel):
    event_id: uuid.UUID
    payment_id: uuid.UUID
