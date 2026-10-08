import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.api import SessionDep, require_api_key, router
from app.config import settings
from app.db import engine, session_factory
from app.messaging import build_broker, declare_topology
from app.outbox import OutboxRelay

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    broker = build_broker()
    await broker.connect()
    await declare_topology(broker)

    relay = OutboxRelay(broker, session_factory)
    relay.start()
    app.state.relay = relay
    try:
        yield
    finally:
        await relay.stop()
        await broker.stop()
        await engine.dispose()


app = FastAPI(
    title="Payment processing service",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.docs_enabled else None,
    redoc_url=None,
    openapi_url="/openapi.json" if settings.docs_enabled else None,
)
app.include_router(router)


@app.get("/health", dependencies=[Depends(require_api_key)], tags=["service"])
async def health(request: Request, session: SessionDep) -> JSONResponse:
    try:
        await session.execute(text("SELECT 1"))
        database = "ok"
    except Exception:
        logger.warning("health check: database is unavailable", exc_info=True)
        database = "unavailable"
    relay = "ok" if request.app.state.relay.is_alive() else "stalled"

    healthy = database == "ok" and relay == "ok"
    return JSONResponse(
        {"status": "ok" if healthy else "degraded", "database": database, "outbox_relay": relay},
        status_code=200 if healthy else 503,
    )
