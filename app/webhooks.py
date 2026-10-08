import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from app.models import Payment

# Пространство имён для uuid5: id события выводится из платежа и статуса,
# поэтому повторы одного уведомления всегда несут один и тот же X-Event-Id.
EVENT_NAMESPACE = uuid.UUID("6f1c2d3e-8a4b-4c5d-9e7f-0a1b2c3d4e5f")


class WebhookDeliveryError(Exception):
    pass


@dataclass(frozen=True)
class WebhookEvent:
    event_id: uuid.UUID
    event_type: str
    body: dict[str, Any]


def payment_event(payment: Payment) -> WebhookEvent:
    event_type = f"payment.{payment.status.value}"
    event_id = uuid.uuid5(EVENT_NAMESPACE, f"{payment.id}:{event_type}")
    body = {
        "event_id": str(event_id),
        "event_type": event_type,
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "description": payment.description,
        "metadata": payment.payment_metadata,
        "failure_reason": payment.failure_reason,
        "created_at": payment.created_at.isoformat(),
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }
    return WebhookEvent(event_id=event_id, event_type=event_type, body=body)


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, deadline: float) -> None:
        self._client = client
        self._deadline = deadline

    async def send(self, url: str, event: WebhookEvent) -> None:
        headers = {"X-Event-Id": str(event.event_id), "X-Event-Type": event.event_type}
        try:
            async with (
                asyncio.timeout(self._deadline),
                # stream: тело ответа не читаем, получателю достаточно вернуть 2xx.
                self._client.stream("POST", url, json=event.body, headers=headers) as response,
            ):
                if not response.is_success:
                    raise WebhookDeliveryError(f"receiver responded with HTTP {response.status_code}")
        except TimeoutError:
            raise WebhookDeliveryError(f"no response within {self._deadline}s") from None
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
