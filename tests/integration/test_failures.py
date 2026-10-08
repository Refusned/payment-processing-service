"""Отказы инфраструктуры: брокер недоступен, binding пропал, consumer убит посреди обработки."""

import asyncio
import uuid

from helpers import VHOST, compose, eventually, get_payment, received_webhooks, wait_until_settled


async def outbox_row(db, payment_id: str):
    return await db.fetchrow("SELECT * FROM outbox WHERE aggregate_id = $1", uuid.UUID(payment_id))


async def test_payment_is_accepted_while_broker_is_down(api, receiver, db, create_payment):
    compose("stop", "rabbitmq")
    try:
        payment_id = await create_payment()
        await asyncio.sleep(3)
        row = await outbox_row(db, payment_id)
        assert row["published_at"] is None
        assert row["attempts"] >= 1
        assert row["last_error"]
    finally:
        compose("start", "rabbitmq")

    # Событие уходит, когда брокер вернулся: отсрочка в outbox растёт до 60 c.
    payment = await wait_until_settled(api, payment_id, within=120)
    assert payment["status"] == "succeeded"
    assert payment["webhook_status"] == "delivered"
    assert (await outbox_row(db, payment_id))["published_at"] is not None
    assert len(await received_webhooks(receiver, payment_id)) == 1


async def test_unroutable_event_is_not_marked_as_published(api, rabbit, db, create_payment):
    binding = f"/bindings/{VHOST}/e/payments/q/payments.new"
    (await rabbit.delete(f"{binding}/payments.new")).raise_for_status()
    try:
        payment_id = await create_payment()

        async def postponed():
            row = await outbox_row(db, payment_id)
            return row if row["last_error"] else None

        row = await eventually(postponed, within=10)
        # Брокер подтвердил приём, но сообщение никуда не попало: событие не потеряно.
        assert row["published_at"] is None
        assert "NO_ROUTE" in row["last_error"] or "Delivery" in row["last_error"]
    finally:
        (await rabbit.post(binding, json={"routing_key": "payments.new"})).raise_for_status()

    payment = await wait_until_settled(api, payment_id, within=60)
    assert payment["status"] == "succeeded"


async def test_consumer_crash_during_processing(api, receiver, db, create_payment):
    payment_id = await create_payment(delay=6)

    async def picked_up():
        row = await outbox_row(db, payment_id)
        return row["published_at"]

    await eventually(picked_up, within=10)
    await asyncio.sleep(1.5)
    assert (await get_payment(api, payment_id))["status"] == "pending"

    # SIGKILL: сообщение не подтверждено, брокер вернёт его в очередь.
    compose("kill", "consumer")
    compose("start", "consumer")

    payment = await wait_until_settled(api, payment_id, within=60)
    assert payment["status"] == "succeeded"
    assert payment["webhook_status"] == "delivered"
    assert len(await received_webhooks(receiver, payment_id)) == 1
