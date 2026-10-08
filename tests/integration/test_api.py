import asyncio
import uuid

import httpx
import pytest
from helpers import API_URL, payment_body, wait_until_settled


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid.uuid4()}"), ("GET", "/health")],
)
@pytest.mark.parametrize("api_key", [None, "wrong-key"])
async def test_api_key_is_required(method, path, api_key):
    headers = {"Idempotency-Key": str(uuid.uuid4())}
    if api_key:
        headers["X-API-Key"] = api_key
    async with httpx.AsyncClient(base_url=API_URL) as client:
        response = await client.request(method, path, json=payment_body(), headers=headers)
    assert response.status_code == 401


async def test_health(api):
    response = await api.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok", "outbox_relay": "ok"}


@pytest.mark.parametrize("key", [None, "", "k" * 256])
async def test_idempotency_key_is_required(api, key):
    headers = {} if key is None else {"Idempotency-Key": key}
    response = await api.post("/api/v1/payments", json=payment_body(), headers=headers)
    assert response.status_code == 400


async def test_create_returns_202(api):
    response = await api.post(
        "/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": str(uuid.uuid4())}
    )
    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    assert body["status"] == "pending"
    assert "idempotent-replayed" not in response.headers


async def test_invalid_body_is_rejected(api):
    response = await api.post(
        "/api/v1/payments",
        json=payment_body(amount="-5"),
        headers={"Idempotency-Key": str(uuid.uuid4())},
    )
    assert response.status_code == 422


async def test_repeat_with_same_body_returns_original_response(api, db):
    key = str(uuid.uuid4())
    first = await api.post(
        "/api/v1/payments", json=payment_body(amount="100"), headers={"Idempotency-Key": key}
    )
    # Та же сумма, записанная иначе, тот же запрос.
    second = await api.post(
        "/api/v1/payments", json=payment_body(amount="100.00"), headers={"Idempotency-Key": key}
    )

    assert first.status_code == second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["idempotent-replayed"] == "true"

    payment_id = uuid.UUID(first.json()["payment_id"])
    assert await db.fetchval("SELECT count(*) FROM payments WHERE idempotency_key = $1", key) == 1
    assert await db.fetchval("SELECT count(*) FROM outbox WHERE aggregate_id = $1", payment_id) == 1


async def test_repeat_after_processing_still_returns_original_response(api):
    key = str(uuid.uuid4())
    first = await api.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": key})
    await wait_until_settled(api, first.json()["payment_id"])
    second = await api.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": key})
    assert second.json() == first.json()
    assert second.json()["status"] == "pending"


async def test_same_key_with_different_body_is_rejected(api):
    key = str(uuid.uuid4())
    first = await api.post(
        "/api/v1/payments", json=payment_body(amount="100"), headers={"Idempotency-Key": key}
    )
    second = await api.post(
        "/api/v1/payments", json=payment_body(amount="101"), headers={"Idempotency-Key": key}
    )
    assert first.status_code == 202
    assert second.status_code == 422


async def test_concurrent_requests_with_same_key_create_one_payment(api, db):
    key = str(uuid.uuid4())
    responses = await asyncio.gather(
        *(
            api.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": key})
            for _ in range(20)
        )
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert sum(r.headers.get("idempotent-replayed") == "true" for r in responses) == 19

    payment_id = uuid.UUID(responses[0].json()["payment_id"])
    assert await db.fetchval("SELECT count(*) FROM payments WHERE idempotency_key = $1", key) == 1
    assert await db.fetchval("SELECT count(*) FROM outbox WHERE aggregate_id = $1", payment_id) == 1


async def test_get_payment(api):
    key = str(uuid.uuid4())
    created = await api.post("/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": key})
    payment_id = created.json()["payment_id"]

    response = await api.get(f"/api/v1/payments/{payment_id}")
    assert response.status_code == 200
    payment = response.json()
    assert payment["id"] == payment_id
    assert payment["amount"] == "100.00"
    assert payment["currency"] == "RUB"
    assert payment["description"] == "integration test"
    assert payment["metadata"] == payment_body()["metadata"]
    assert payment["idempotency_key"] == key
    assert payment["webhook_url"] == payment_body()["webhook_url"]
    assert payment["created_at"] == created.json()["created_at"]
    assert {"status", "processed_at", "failure_reason", "webhook_status", "updated_at"} <= set(payment)
    assert "request_hash" not in payment


async def test_get_unknown_payment(api):
    assert (await api.get(f"/api/v1/payments/{uuid.uuid4()}")).status_code == 404
    assert (await api.get("/api/v1/payments/not-a-uuid")).status_code == 422
