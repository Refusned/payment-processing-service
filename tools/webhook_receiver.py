"""Получатель webhook для локального запуска и интеграционных тестов.

POST /ok      отвечает 200
POST /fail    всегда отвечает 500
POST /slow    отвечает дольше, чем отправитель готов ждать
GET  /received?payment_id=...   полученные уведомления
"""

import asyncio
import time
from typing import Any

from fastapi import FastAPI, Request, Response

app = FastAPI(title="Webhook receiver")
received: list[dict[str, Any]] = []


async def _record(request: Request) -> None:
    body = await request.json()
    received.append(
        {
            "path": request.url.path,
            "event_id": request.headers.get("x-event-id"),
            "event_type": request.headers.get("x-event-type"),
            "received_at": time.time(),
            "body": body,
        }
    )


@app.post("/ok")
async def ok(request: Request) -> Response:
    await _record(request)
    return Response(status_code=200)


@app.post("/fail")
async def fail(request: Request) -> Response:
    await _record(request)
    return Response(status_code=500)


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
