from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from app.db.repository import OutboxRepository
from app.streaming.hub import StreamHub, Subscriber, is_shutdown

logger = logging.getLogger(__name__)

router = APIRouter(tags=["stream"])

WS_CODE_SLOW_CONSUMER = 4001
WS_CODE_CURSOR_TOO_OLD = 4003
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

    if replay and last_event_id > 0:
        oldest = await outbox.oldest_replayable_seq()
        if oldest is not None and oldest > last_event_id + 1:
            logger.info(
                "rejecting cursor %d, oldest retained is %d", last_event_id, oldest
            )
            await websocket.send_json(
                {
                    "type": "cursor_too_old",
                    "requested": last_event_id,
                    "oldest_available": oldest,
                    "detail": (
                        "Events after this cursor have been trimmed by retention. "
                        "Resynchronise from GET /api/v1/items, then reconnect "
                        "with the newest stream_seq you receive."
                    ),
                }
            )
            await websocket.close(
                code=WS_CODE_CURSOR_TOO_OLD, reason="cursor too old"
            )
            return

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

        pump = asyncio.create_task(
            _pump(
                websocket,
                subscriber,
                skip_up_to=replayed_to,
                heartbeat=settings.stream_heartbeat_interval,
            )
        )
        watch = asyncio.create_task(_watch_for_disconnect(websocket))

        done, pending = await asyncio.wait(
            {pump, watch}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            task.result()  # re-raise whatever ended the connection

    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("stream subscriber %s failed", subscriber.id)
    finally:
        await hub.unsubscribe(subscriber)
        await _close_quietly(websocket)


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


async def _close_quietly(websocket: WebSocket) -> None:
    if (
        websocket.client_state is WebSocketState.CONNECTED
        and websocket.application_state is WebSocketState.CONNECTED
    ):
        with contextlib.suppress(RuntimeError):
            await websocket.close()


async def _watch_for_disconnect(websocket: WebSocket) -> None:
    """Read from the socket for the sole purpose of noticing it closed.

    The protocol is server-to-client only, so anything the client sends is
    ignored. What matters is the disconnect message, which is the only
    reliable, immediate signal that the peer is gone.
    """
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return


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
            with contextlib.suppress(RuntimeError):
                await websocket.close(
                    code=WS_CODE_SLOW_CONSUMER, reason="slow consumer"
                )
            return

        envelope = await subscriber.next_event(timeout=heartbeat)

        if envelope is None:
            await websocket.send_json({"type": "ping"})
            continue

        if is_shutdown(envelope):
            await websocket.close(
                code=WS_CODE_SERVER_SHUTDOWN, reason="server shutting down"
            )
            return

        if envelope["stream_seq"] <= skip_up_to:
            continue

        await websocket.send_json(envelope)
