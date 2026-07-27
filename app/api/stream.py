from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from app.db.repository import OutboxRepository
from app.streaming.hub import StreamHub, Subscriber

logger = logging.getLogger(__name__)

router = APIRouter(tags=["stream"])

WS_CODE_SLOW_CONSUMER = 4001
WS_CODE_SERVER_SHUTDOWN = 4002

@router.websocket("/stream")
async def stream(
    websocket: WebSocket,
    last_event_id: int = Query(
        default=0,
        ge=0,
        description=(
            "Resume position: the highest stream_seq the client has already "
            "processed. 0 (the default) means live-only, no replay."
        ),
    ),
    aggregate_id: uuid.UUID | None = Query(
        default=None, description="Optional server-side filter to one entity"
    ),
    replay: bool = Query(
        default=True,
        description="Set false to skip the backlog and start from live events only",
    ),
) -> None:
    app = websocket.app
    hub: StreamHub = app.state.hub
    outbox = OutboxRepository(app.state.pool)
    settings = app.state.settings

    await websocket.accept()

    subscriber = await hub.subscribe(aggregate_id=aggregate_id, cursor=last_event_id)

    try:
        await websocket.send_json(
            {
                "type": "ready",
                "resume_from": last_event_id,
                "filter_aggregate_id": str(aggregate_id) if aggregate_id else None,
            }
        )

        replayed_to = last_event_id
        if replay and last_event_id > 0:
            replayed_to = await _replay(
                websocket,
                outbox,
                after_seq=last_event_id,
                aggregate_id=aggregate_id,
                page_size=settings.stream_replay_batch_size,
            )

        await _pump(
            websocket,
            subscriber,
            skip_up_to=replayed_to,
            heartbeat=settings.stream_heartbeat_interval,
        )

    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("stream subscriber %s failed", subscriber.id)
    finally:
        await hub.unsubscribe(subscriber)
        if websocket.application_state is WebSocketState.CONNECTED:
            await websocket.close()


async def _replay(
    websocket: WebSocket,
    outbox: OutboxRepository,
    *,
    after_seq: int,
    aggregate_id: uuid.UUID | None,
    page_size: int,
) -> int:
    """Send everything the client missed. Returns the last stream_seq sent."""
    cursor = after_seq
    sent = 0

    while True:
        events = await outbox.replay(
            after_seq=cursor, aggregate_id=aggregate_id, limit=page_size
        )
        if not events:
            break
        for event in events:
            await websocket.send_json(event.to_envelope())
            cursor = event.stream_seq or cursor
            sent += 1
        if len(events) < page_size:
            break

    await websocket.send_json({"type": "replay_complete", "up_to": cursor, "count": sent})
    logger.info("replayed %d events up to stream_seq=%d", sent, cursor)
    return cursor


async def _pump(
    websocket: WebSocket,
    subscriber: Subscriber,
    *,
    skip_up_to: int,
    heartbeat: float,
) -> None:
    """Forward live events until the client goes away or falls behind."""
    while True:
        if subscriber.overflowed:
            logger.warning("closing slow subscriber %s", subscriber.id)
            await websocket.close(code=WS_CODE_SLOW_CONSUMER, reason="slow consumer")
            return

        envelope = await subscriber.next_event(timeout=heartbeat)

        if envelope is None:
            await websocket.send_json({"type": "ping"})
            continue

        if envelope["stream_seq"] <= skip_up_to:
            continue

        await websocket.send_json(envelope)
