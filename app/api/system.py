from __future__ import annotations

import os
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

    pending = await outbox.count_unpublished()
    head = await outbox.current_stream_seq()
    cursor = dispatcher.cursor if dispatcher else None

    return HealthResponse(
        status="ok",
        database="ok",
        worker_pid=os.getpid(),
        outbox_pending=pending,
        stream_head=head,
        tailer_cursor=cursor,
        tailer_lag=(head - cursor) if cursor is not None else None,
        stream_subscribers=hub.subscriber_count if hub else None,
        dispatcher_running=dispatcher.is_running if dispatcher else None,
        retention_running=(
            dispatcher.retention.is_running if dispatcher else None
        ),
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
