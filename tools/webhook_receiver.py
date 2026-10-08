"""Получатель webhook для локального запуска и интеграционных тестов.

POST /ok              отвечает 200
POST /fail            всегда отвечает 500
POST /flaky/{n}       первые n запросов по платежу отвечает 500, потом 200
POST /slow            отвечает дольше, чем отправитель готов ждать
GET  /received?payment_id=...   полученные уведомления
"""

import asyncio
import time
from collections import Counter
from typing import Any

from fastapi import FastAPI, Request, Response

app = FastAPI(title="Webhook receiver")
received: list[dict[str, Any]] = []
flaky_calls: Counter[str] = Counter()


async def _record(request: Request) -> dict[str, Any]:
    body = await request.json()
    received.append(
        {
            "path": request.url.path,
            "event_id": request.headers.get("x-event-id"),
            "event_type": request.headers.get("x-event-type"),
            # monotonic: тестам нужны интервалы между запросами, а не время суток.
            "received_at": time.monotonic(),
            "body": body,
        }
    )
    return body


@app.post("/ok")
async def ok(request: Request) -> Response:
    await _record(request)
    return Response(status_code=200)


@app.post("/fail")
async def fail(request: Request) -> Response:
    await _record(request)
    return Response(status_code=500)


@app.post("/flaky/{failures}")
async def flaky(failures: int, request: Request) -> Response:
    body = await _record(request)
    flaky_calls[body["payment_id"]] += 1
    return Response(status_code=500 if flaky_calls[body["payment_id"]] <= failures else 200)


@app.post("/slow")
async def slow(request: Request) -> Response:
    await _record(request)
    await asyncio.sleep(30)
    return Response(status_code=200)


@app.get("/received")
async def list_received(payment_id: str | None = None) -> list[dict[str, Any]]:
    if payment_id is None:
        return received
    return [item for item in received if item["body"].get("payment_id") == payment_id]


@app.delete("/received", status_code=204)
async def clear_received() -> None:
    received.clear()
    flaky_calls.clear()
