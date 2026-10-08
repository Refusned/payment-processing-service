import asyncio
import json
import uuid
from itertools import pairwise

from helpers import (
    dead_lettered,
    eventually,
    get_payment,
    publish_raw,
    received_webhooks,
    wait_until_settled,
)


async def test_successful_payment_is_published_processed_and_notified(api, receiver, db, create_payment):
    payment_id = await create_payment(result="succeeded")

    payment = await wait_until_settled(api, payment_id)
    assert payment["status"] == "succeeded"
    assert payment["failure_reason"] is None
    assert payment["processed_at"] is not None
    assert payment["webhook_status"] == "delivered"
    assert payment["webhook_attempts"] == 1

    outbox = await db.fetchrow("SELECT * FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id))
    assert outbox["event_type"] == "payment.created"
    assert outbox["published_at"] is not None

    [webhook] = await received_webhooks(receiver, payment_id)
    assert webhook["event_type"] == "payment.succeeded"
    assert webhook["event_id"] == webhook["body"]["event_id"]
    assert webhook["body"]["payment_id"] == payment_id
    assert webhook["body"]["status"] == "succeeded"
    assert webhook["body"]["amount"] == "100.00"
    assert webhook["body"]["processed_at"] is not None


async def test_declined_payment_is_notified_without_retries(api, receiver, create_payment):
    payment_id = await create_payment(result="failed")

    payment = await wait_until_settled(api, payment_id)
    assert payment["status"] == "failed"
    assert payment["failure_reason"]
    assert payment["webhook_status"] == "delivered"
    assert payment["webhook_attempts"] == 1

    [webhook] = await received_webhooks(receiver, payment_id)
    assert webhook["event_type"] == "payment.failed"
    assert webhook["body"]["failure_reason"] == payment["failure_reason"]


async def test_failing_webhook_is_retried_three_times_then_dead_lettered(
    api, receiver, rabbit, db, create_payment
):
    payment_id = await create_payment(webhook="fail")

    payment = await wait_until_settled(api, payment_id)
    # Отказ получателя не отменяет списание: платёж прошёл, не дошло только уведомление.
    assert payment["status"] == "succeeded"
    assert payment["webhook_status"] == "failed"
    assert payment["webhook_attempts"] == 3

    calls = await received_webhooks(receiver, payment_id)
    assert len(calls) == 3
    assert len({call["event_id"] for call in calls}) == 1
    gaps = [later["received_at"] - earlier["received_at"] for earlier, later in pairwise(calls)]
    # Задержки 2 и 4 c задаются TTL retry-очередей. Верхняя граница с большим запасом.
    assert 1.8 < gaps[0] < 6
    assert 3.8 < gaps[1] < 8

    event_id = str(await db.fetchval("SELECT id FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id)))
    [message] = await eventually(lambda: dead_lettered(rabbit, "x-event-id", event_id), within=10)
    assert message["properties"]["headers"]["x-attempt"] == "3"

    last_error = await db.fetchval(
        "SELECT webhook_last_error FROM payments WHERE id = $1", uuid.UUID(payment_id)
    )
    assert "HTTP 500" in last_error


async def test_redelivered_event_does_not_repeat_processing(api, receiver, rabbit, db, create_payment):
    payment_id = await create_payment()
    settled = await wait_until_settled(api, payment_id)

    payload = await db.fetchval("SELECT payload FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id))
    event = json.loads(payload)
    # То же событие ещё раз, как после повторной публикации из outbox.
    await publish_raw(rabbit, event, {"x-event-id": event["event_id"]})
    await asyncio.sleep(3)

    assert await get_payment(api, payment_id) == settled
    assert len(await received_webhooks(receiver, payment_id)) == 1


async def test_malformed_message_goes_to_dlq(rabbit):
    marker = str(uuid.uuid4())
    await publish_raw(rabbit, "this is not an event", {"x-test-marker": marker})
    await eventually(lambda: dead_lettered(rabbit, "x-test-marker", marker), within=10)


async def test_event_for_unknown_payment_goes_to_dlq(rabbit):
    event_id = str(uuid.uuid4())
    await publish_raw(
        rabbit, {"event_id": event_id, "payment_id": str(uuid.uuid4())}, {"x-event-id": event_id}
    )
    await eventually(lambda: dead_lettered(rabbit, "x-event-id", event_id), within=10)
