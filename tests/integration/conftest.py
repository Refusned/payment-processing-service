"""Интеграционные тесты идут против поднятого стека:

    docker compose -p payments-test --env-file tests/integration/test.env up -d --build --wait
    pytest tests/integration

или просто `make test-integration`.
"""

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from helpers import API_URL, DATABASE_DSN, ENV, RABBIT_API_URL, RECEIVER_URL, payment_body


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if Path(str(item.fspath)).is_relative_to(Path(__file__).parent):
            item.add_marker(pytest.mark.integration)


@pytest.fixture
async def api() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=API_URL, headers={"X-API-Key": ENV["API_KEY"]}, timeout=30
    ) as client:
        yield client


@pytest.fixture
async def receiver() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=RECEIVER_URL, timeout=10) as client:
        yield client


@pytest.fixture
async def rabbit() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=RABBIT_API_URL, auth=("guest", "guest"), timeout=10) as client:
        yield client


@pytest.fixture
async def db() -> AsyncIterator[asyncpg.Connection]:
    connection = await asyncpg.connect(DATABASE_DSN)
    yield connection
    await connection.close()


@pytest.fixture
def create_payment(api: httpx.AsyncClient) -> Callable[..., Awaitable[str]]:
    async def create(**kwargs: Any) -> str:
        response = await api.post(
            "/api/v1/payments", json=payment_body(**kwargs), headers={"Idempotency-Key": str(uuid.uuid4())}
        )
        assert response.status_code == 202, response.text
        return response.json()["payment_id"]

    return create
