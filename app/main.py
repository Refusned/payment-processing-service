import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.api import SessionDep, require_api_key, router
from app.config import settings
from app.db import engine, session_factory
from app.messaging import build_broker
from app.outbox import OutboxRelay

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    broker = build_broker()
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
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.docs_enabled else None,
    redoc_url=None,
    openapi_url="/openapi.json" if settings.docs_enabled else None,
)
app.include_router(router)


@app.exception_handler(DBAPIError)
@app.exception_handler(OSError)
async def database_unavailable(request: Request, exc: Exception) -> JSONResponse:
    logger.warning("database is unavailable: %s: %s", type(exc).__name__, exc)
    return JSONResponse(
        {"detail": "Database is unavailable, retry later"}, status_code=503, headers={"Retry-After": "5"}
    )


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    # Без поля input: в нём может оказаться текст, который не сериализуется в ответ
    # (например, одиночный суррогат), и вместо 422 клиент получил бы 500.
    errors = [{"loc": e["loc"], "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
    return JSONResponse({"detail": errors}, status_code=422)


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
