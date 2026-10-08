import asyncio
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.models import Currency, Payment, PaymentStatus
from app.webhooks import WebhookDeliveryError, WebhookSender, payment_event


def make_payment(status=PaymentStatus.SUCCEEDED, failure_reason=None):
    return Payment(
        id=uuid.UUID("11111111-2222-3333-4444-555555555555"),
        amount=Decimal("199.99"),
        currency=Currency.RUB,
        description="order #42",
        payment_metadata={"order_id": 42},
        status=status,
        failure_reason=failure_reason,
        webhook_url="http://receiver/ok",
        created_at=datetime(2026, 10, 8, 12, 0, tzinfo=UTC),
        processed_at=datetime(2026, 10, 8, 12, 0, 3, tzinfo=UTC),
    )


def test_event_body():
    event = payment_event(make_payment())
    assert event.event_type == "payment.succeeded"
    assert event.body == {
        "event_id": str(event.event_id),
        "event_type": "payment.succeeded",
        "payment_id": "11111111-2222-3333-4444-555555555555",
        "status": "succeeded",
        "amount": "199.99",
        "currency": "RUB",
        "description": "order #42",
        "metadata": {"order_id": 42},
        "failure_reason": None,
        "created_at": "2026-10-08T12:00:00Z",
        "processed_at": "2026-10-08T12:00:03Z",
    }


def test_event_id_is_stable_for_the_same_outcome():
    assert payment_event(make_payment()).event_id == payment_event(make_payment()).event_id


def test_event_id_differs_between_outcomes():
    succeeded = payment_event(make_payment())
    failed = payment_event(make_payment(PaymentStatus.FAILED, "declined"))
    assert failed.event_type == "payment.failed"
    assert failed.event_id != succeeded.event_id


def sender_for(handler, deadline=1.0):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return WebhookSender(client, deadline)


async def test_successful_delivery_sends_event_headers():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(204)

    event = payment_event(make_payment())
    await sender_for(handler).send("http://receiver/ok", event)
    assert seen["x-event-id"] == str(event.event_id)
    assert seen["x-event-type"] == "payment.succeeded"


@pytest.mark.parametrize("status_code", [301, 400, 404, 429, 500, 503])
async def test_non_2xx_is_an_error(status_code):
    sender = sender_for(lambda request: httpx.Response(status_code))
    with pytest.raises(WebhookDeliveryError, match=str(status_code)):
        await sender.send("http://receiver/hook", payment_event(make_payment()))


async def test_connection_error_is_an_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(WebhookDeliveryError, match="ConnectError"):
        await sender_for(handler).send("http://receiver/hook", payment_event(make_payment()))


async def test_timeout_error_has_a_readable_message():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("", request=request)

    with pytest.raises(WebhookDeliveryError, match="ReadTimeout: receiver did not respond in time"):
        await sender_for(handler).send("http://receiver/hook", payment_event(make_payment()))


async def test_deadline_limits_the_whole_request():
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return httpx.Response(200)

    with pytest.raises(WebhookDeliveryError, match="no response"):
        await sender_for(handler, deadline=0.1).send("http://receiver/hook", payment_event(make_payment()))
