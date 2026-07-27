from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse

from app.api import items, stream, system
from app.api.schemas import ErrorResponse
from app.core.config import get_settings
from app.core.errors import DomainError
from app.core.logging import configure_logging
from app.db.pool import close_pool, create_pool
from app.streaming import OutboxDispatcher, StreamHub

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)

    app.state.settings = settings
    app.state.pool = await create_pool(settings)
    app.state.hub = StreamHub(queue_size=settings.stream_queue_size)
    app.state.dispatcher = OutboxDispatcher(settings=settings, hub=app.state.hub)
    await app.state.dispatcher.start()

    logger.info("application started")
    try:
        yield
    finally:
        await app.state.dispatcher.stop()
        await close_pool(app.state.pool)
        logger.info("application stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Transactional Outbox Service",
        version="0.2.0",
        description=(
            "CRUD over PostgreSQL with a transactional outbox. Events are written "
            "in the same transaction as the data and streamed to WebSocket clients "
            "only after commit."
        ),
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
    )

    app.include_router(system.router)
    app.include_router(items.router, prefix="/api/v1")
    app.include_router(stream.router, prefix="/api/v1")
    app.include_router(system.router, prefix="/api/v1", include_in_schema=False)

    @app.exception_handler(DomainError)
    async def domain_error_handler(_: Request, exc: DomainError) -> ORJSONResponse:
        return ORJSONResponse(
            status_code=exc.status_code,
            content=ErrorResponse(code=exc.code, message=exc.message).model_dump(),
        )

    return app


app = create_app()
