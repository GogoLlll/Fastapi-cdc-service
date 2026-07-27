"""Health and diagnostics endpoints."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Query, Request, Response, status

from app.api.deps import OutboxRepo, Pool
from app.api.schemas import HealthResponse, OutboxEventRead

router = APIRouter(tags=["system"])


@router.get("/health", response_model=HealthResponse, summary="Liveness / readiness")
async def health(
    request: Request, pool: Pool, outbox: OutboxRepo, response: Response
) -> HealthResponse:
    try:
        await pool.fetchval("SELECT 1")
    except Exception: 
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(status="degraded", database="unreachable")

    hub = getattr(request.app.state, "hub", None)
    dispatcher = getattr(request.app.state, "dispatcher", None)

    return HealthResponse(
        status="ok",
        database="ok",
        outbox_pending=await outbox.count_unpublished(),
        stream_subscribers=hub.subscriber_count if hub else None,
        dispatcher_running=dispatcher.is_running if dispatcher else None,
    )


@router.get(
    "/outbox",
    response_model=list[OutboxEventRead],
    summary="Inspect the outbox (diagnostics)",
    description=(
        "Exposed to make the consistency guarantees observable from the outside: "
        "tests and reviewers can assert that the outbox matches the item table."
    ),
)
async def read_outbox(
    outbox: OutboxRepo,
    after_id: int = Query(default=0, ge=0),
    aggregate_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[OutboxEventRead]:
    events = await outbox.list_events(
        after_id=after_id, aggregate_id=aggregate_id, limit=limit
    )
    return [OutboxEventRead.model_validate(e) for e in events]
