import json
import uuid
from itertools import pairwise

from helpers import (
    compose,
    copies_finished,
    dead_lettered,
    eventually,
    get_payment,
    publish_raw,
    received_webhooks,
    wait_until_settled,
)


async def outbox_event(db, payment_id: str) -> dict:
    payload = await db.fetchval("SELECT payload FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id))
    return json.loads(payload)


async def finished(payment_id: str, copies: int) -> bool:
    return await copies_finished(payment_id) >= copies


def gaps(calls: list[dict]) -> list[float]:
    return [later["received_at"] - earlier["received_at"] for earlier, later in pairwise(calls)]


async def test_successful_payment_is_published_processed_and_notified(api, receiver, db, create_payment):
    payment_id = await create_payment(result="succeeded")

    payment = await wait_until_settled(api, payment_id)
    assert payment["status"] == "succeeded"
    assert payment["failure_reason"] is None
    assert payment["processed_at"] is not None
    assert payment["webhook_status"] == "delivered"
    assert payment["webhook_attempts"] == 1
    assert payment["webhook_delivered_at"] is not None

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
    assert "HTTP 500" in payment["webhook_last_error"]

    calls = await received_webhooks(receiver, payment_id)
    assert len(calls) == 3
    assert len({call["event_id"] for call in calls}) == 1
    # Паузы 2 и 4 c. Сверху с запасом на медленную машину.
    first, second = gaps(calls)
    assert 1.8 < first < 10
    assert 3.8 < second < 15

    event = await outbox_event(db, payment_id)
    [message] = await eventually(lambda: dead_lettered(rabbit, "x-event-id", event["event_id"]), within=10)
    assert message["properties"]["headers"]["x-attempt"] == "3"


async def test_receiver_recovers_before_attempts_run_out(api, receiver, rabbit, db, create_payment):
    payment_id = await create_payment(webhook="flaky/2")

    await eventually(lambda: finished(payment_id, 1), within=30)
    payment = await get_payment(api, payment_id)
    assert payment["webhook_status"] == "delivered"
    assert payment["webhook_attempts"] == 3
    assert payment["webhook_last_error"] is None
    assert len(await received_webhooks(receiver, payment_id)) == 3

    event = await outbox_event(db, payment_id)
    assert await dead_lettered(rabbit, "x-event-id", event["event_id"]) == []


async def test_redelivered_event_does_not_repeat_processing(api, receiver, rabbit, db, create_payment):
    payment_id = await create_payment()
    settled = await wait_until_settled(api, payment_id)

    # То же событие ещё раз, как после повторной публикации из outbox.
    event = await outbox_event(db, payment_id)
    await publish_raw(rabbit, event, {"x-event-id": event["event_id"]})
    await eventually(lambda: finished(payment_id, 2), within=15)

    assert await get_payment(api, payment_id) == settled
    assert len(await received_webhooks(receiver, payment_id)) == 1


async def two_copies_at_once(rabbit, db, create_payment, webhook: str) -> str:
    """Две копии одного события ждут в очереди, пока consumer остановлен, и потом идут параллельно."""
    await compose("stop", "consumer")
    try:
        payment_id = await create_payment(webhook=webhook)

        async def published():
            return await db.fetchval(
                "SELECT published_at FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id)
            )

        await eventually(published, within=10)
        event = await outbox_event(db, payment_id)
        await publish_raw(rabbit, event, {"x-event-id": event["event_id"]})
    finally:
        await compose("start", "consumer")
    return payment_id


async def test_concurrent_duplicates_send_one_webhook(api, receiver, rabbit, db, create_payment):
    payment_id = await two_copies_at_once(rabbit, db, create_payment, webhook="ok")

    await eventually(lambda: finished(payment_id, 2), within=40)
    payment = await get_payment(api, payment_id)
    assert payment["webhook_status"] == "delivered"
    assert payment["webhook_attempts"] == 1
    assert len(await received_webhooks(receiver, payment_id)) == 1


async def test_concurrent_duplicates_share_attempts_and_pauses(api, receiver, rabbit, db, create_payment):
    payment_id = await two_copies_at_once(rabbit, db, create_payment, webhook="fail")

    # Обе копии заканчивают в DLQ: одна после третьей ошибки, вторая увидев, что попытки исчерпаны.
    await eventually(lambda: finished(payment_id, 2), within=60)
    payment = await get_payment(api, payment_id)
    assert payment["webhook_status"] == "failed"
    assert payment["webhook_attempts"] == 3

    calls = await received_webhooks(receiver, payment_id)
    assert len(calls) == 3
    # Вторая копия не сокращает паузы между попытками.
    first, second = gaps(calls)
    assert first > 1.8
    assert second > 3.8

    event = await outbox_event(db, payment_id)
    assert len(await dead_lettered(rabbit, "x-event-id", event["event_id"])) == 2


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
