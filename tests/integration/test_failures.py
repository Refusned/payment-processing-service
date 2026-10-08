"""Отказы инфраструктуры: брокер недоступен, маршрут пропал, consumer убит посреди обработки."""

import uuid
from itertools import pairwise

from helpers import (
    VHOST,
    compose,
    consumer_subscribed,
    copies_finished,
    dead_lettered,
    eventually,
    get_payment,
    received_webhooks,
    start_rabbitmq,
    times_processed,
    wait_until_settled,
)


async def outbox_row(db, payment_id: str):
    return await db.fetchrow("SELECT * FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id))


async def postponed(db, payment_id: str):
    row = await outbox_row(db, payment_id)
    return row if row["last_error"] and row["published_at"] is None else None


async def test_payment_is_accepted_while_broker_is_down(api, receiver, rabbit, db, create_payment):
    await compose("stop", "rabbitmq")
    try:
        payment_id = await create_payment()
        row = await eventually(lambda: postponed(db, payment_id), within=15)
        assert row["attempts"] >= 1
    finally:
        await start_rabbitmq(rabbit)

    # Событие уходит, когда брокер вернулся: отсрочка в outbox растёт до 60 c.
    payment = await wait_until_settled(api, payment_id, within=120)
    assert payment["status"] == "succeeded"
    assert payment["webhook_status"] == "delivered"
    assert (await outbox_row(db, payment_id))["published_at"] is not None
    assert len(await received_webhooks(receiver, payment_id)) == 1


async def test_api_starts_without_broker(api, rabbit, db, create_payment):
    await compose("stop", "rabbitmq")
    try:
        await compose("restart", "api")

        async def healthy():
            try:
                return (await api.get("/health")).status_code == 200
            except Exception:
                return False

        await eventually(healthy, within=30)
        payment_id = await create_payment()
        # Релей ещё не подключился к брокеру, событие ждёт в outbox.
        assert (await outbox_row(db, payment_id))["published_at"] is None
    finally:
        await start_rabbitmq(rabbit)

    payment = await wait_until_settled(api, payment_id, within=120)
    assert payment["webhook_status"] == "delivered"


async def test_unroutable_event_is_not_marked_as_published(api, rabbit, db, create_payment):
    binding = f"/bindings/{VHOST}/e/payments/q/payments.new"
    (await rabbit.delete(f"{binding}/payments.new")).raise_for_status()
    try:
        payment_id = await create_payment()
        row = await eventually(lambda: postponed(db, payment_id), within=10)
        # Брокер принял сообщение, но не нашёл для него очередь: событие остаётся в outbox.
        assert "DeliveryError" in row["last_error"] or "PublishError" in row["last_error"]
    finally:
        (await rabbit.post(binding, json={"routing_key": "payments.new"})).raise_for_status()

    payment = await wait_until_settled(api, payment_id, within=60)
    assert payment["status"] == "succeeded"


async def test_failed_retry_publish_keeps_attempts_and_pauses(api, receiver, rabbit, db, create_payment):
    # Очередь первого повтора недоступна: сообщение возвращается в рабочую очередь сразу,
    # но время следующей попытки записано в БД, и раньше него запрос к получателю не уйдёт.
    binding = f"/bindings/{VHOST}/e/payments/q/payments.retry.2s"
    (await rabbit.delete(f"{binding}/payments.retry.2s")).raise_for_status()
    try:
        payment_id = await create_payment(webhook="fail")

        async def gave_up():
            return await copies_finished(payment_id) >= 1

        await eventually(gave_up, within=40)
    finally:
        (await rabbit.post(binding, json={"routing_key": "payments.retry.2s"})).raise_for_status()

    payment = await get_payment(api, payment_id)
    assert payment["webhook_status"] == "failed"
    assert payment["webhook_attempts"] == 3

    calls = await received_webhooks(receiver, payment_id)
    assert len(calls) == 3
    first, second = (b["received_at"] - a["received_at"] for a, b in pairwise(calls))
    assert first > 1.8
    assert second > 3.8

    event_id = str(await db.fetchval("SELECT id FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id)))
    assert len(await dead_lettered(rabbit, "x-event-id", event_id)) == 1


async def test_consumer_crash_during_processing(api, receiver, rabbit, create_payment):
    payment_id = await create_payment(delay=6)

    # Consumer взял событие и сидит в эмуляторе шлюза (6 c).
    await eventually(lambda: times_processed(payment_id), within=15)
    assert (await get_payment(api, payment_id))["status"] == "pending"

    # SIGKILL: сообщение не подтверждено, брокер вернёт его в очередь.
    await compose("kill", "consumer")
    await compose("start", "consumer")

    payment = await wait_until_settled(api, payment_id, within=60)
    assert payment["status"] == "succeeded"
    assert payment["webhook_status"] == "delivered"
    assert len(await received_webhooks(receiver, payment_id)) == 1
    assert await times_processed(payment_id) >= 2
    await eventually(lambda: consumer_subscribed(rabbit), within=30)
