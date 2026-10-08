"""Адреса тестового стека и помощники для интеграционных тестов."""

import asyncio
import json
import subprocess
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = Path(__file__).with_name("test.env")


def _read_env(path: Path) -> dict[str, str]:
    pairs = (line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
    return {key.strip(): value.strip() for key, value in pairs}


ENV = _read_env(ENV_FILE)
API_URL = f"http://127.0.0.1:{ENV['API_PORT']}"
RECEIVER_URL = f"http://127.0.0.1:{ENV['WEBHOOK_RECEIVER_PORT']}"
RABBIT_API_URL = f"http://127.0.0.1:{ENV['RABBITMQ_UI_PORT']}/api"
DATABASE_DSN = f"postgresql://payments:payments@127.0.0.1:{ENV['POSTGRES_PORT']}/payments"
# Адрес получателя внутри docker-сети, по нему ходит consumer.
INTERNAL_RECEIVER_URL = "http://webhook-receiver:9000"
VHOST = quote("/", safe="")


def compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-p", ENV["COMPOSE_PROJECT_NAME"], "--env-file", str(ENV_FILE), *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )


def payment_body(
    *, webhook: str = "ok", result: str = "succeeded", delay: float = 0.1, amount: str = "100.00"
) -> dict[str, Any]:
    return {
        "amount": amount,
        "currency": "RUB",
        "description": "integration test",
        "metadata": {"emulator": {"result": result, "delay": delay}},
        "webhook_url": f"{INTERNAL_RECEIVER_URL}/{webhook}",
    }


async def eventually[T](check: Callable[[], Awaitable[T | None]], within: float = 30) -> T:
    """Опрашивать check, пока он не вернёт непустое значение, но не дольше within секунд."""
    deadline = asyncio.get_running_loop().time() + within
    while True:
        result = await check()
        if result:
            return result
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"condition not met within {within}s, last value: {result!r}")
        await asyncio.sleep(0.3)


async def get_payment(api: httpx.AsyncClient, payment_id: str) -> dict[str, Any]:
    response = await api.get(f"/api/v1/payments/{payment_id}")
    assert response.status_code == 200, response.text
    return response.json()


async def wait_until_settled(api: httpx.AsyncClient, payment_id: str, within: float = 30) -> dict[str, Any]:
    """Дождаться, пока платёж получит финальный статус и закончится доставка webhook."""

    async def settled() -> dict[str, Any] | None:
        payment = await get_payment(api, payment_id)
        done = payment["status"] != "pending" and payment["webhook_status"] != "pending"
        return payment if done else None

    return await eventually(settled, within=within)


async def received_webhooks(receiver: httpx.AsyncClient, payment_id: str) -> list[dict[str, Any]]:
    response = await receiver.get("/received", params={"payment_id": payment_id})
    response.raise_for_status()
    return response.json()


async def dead_lettered(rabbit: httpx.AsyncClient, header: str, value: str) -> list[dict[str, Any]]:
    """Сообщения из payments.dlq с заданным заголовком. Сообщения остаются в очереди."""
    response = await rabbit.post(
        f"/queues/{VHOST}/payments.dlq/get",
        json={"count": 1000, "ackmode": "ack_requeue_true", "encoding": "auto"},
    )
    response.raise_for_status()
    return [m for m in response.json() if (m["properties"].get("headers") or {}).get(header) == value]


async def publish_raw(
    rabbit: httpx.AsyncClient, payload: str | dict[str, Any], headers: dict[str, str]
) -> None:
    """Опубликовать сообщение в payments.new в обход приложения."""
    properties: dict[str, Any] = {"headers": headers, "delivery_mode": 2}
    if isinstance(payload, dict):
        payload = json.dumps(payload)
        properties["content_type"] = "application/json"
    response = await rabbit.post(
        f"/exchanges/{VHOST}/payments/publish",
        json={
            "properties": properties,
            "routing_key": "payments.new",
            "payload": payload,
            "payload_encoding": "string",
        },
    )
    response.raise_for_status()
    assert response.json()["routed"]
