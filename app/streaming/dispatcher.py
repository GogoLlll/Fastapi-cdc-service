from __future__ import annotations

import asyncio
import logging
from typing import Any

import asyncpg

from app.core.config import Settings
from app.db.models import OutboxEvent
from app.db.pool import init_connection
from app.streaming.hub import StreamHub

logger = logging.getLogger(__name__)

NOTIFY_CHANNEL = "outbox_new"

DISPATCH_LOCK_ID = 918_273_645

_CLAIM_SQL = """
    SELECT id, aggregate_type, aggregate_id, event_type,
           payload, created_at, published_at, stream_seq
    FROM outbox
    WHERE published_at IS NULL
    ORDER BY id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
"""

_MARK_SQL = """
    UPDATE outbox AS o
    SET published_at = now(),
        stream_seq   = v.seq
    FROM unnest($1::bigint[], $2::bigint[]) AS v(id, seq)
    WHERE o.id = v.id
"""


class OutboxDispatcher:
    def __init__(self, *, settings: Settings, hub: StreamHub) -> None:
        self._settings = settings
        self._hub = hub

        self._task: asyncio.Task[None] | None = None
        self._conn: asyncpg.Connection | None = None
        self._wakeup = asyncio.Event()
        self._stopping = asyncio.Event()

        self.published_total = 0
        self.last_error: str | None = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="outbox-dispatcher")
        logger.info("dispatcher started")

    async def stop(self) -> None:
        self._stopping.set()
        self._wakeup.set()

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        await self._close_connection()
        logger.info("dispatcher stopped (published %d events)", self.published_total)

    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self._ensure_connection()
                await self._drain()
                await self._wait_for_signal()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = repr(exc)
                logger.exception("dispatcher iteration failed, backing off")
                await self._close_connection()
                await asyncio.sleep(self._settings.dispatcher_error_backoff)

    async def _wait_for_signal(self) -> None:
        """Wait for a NOTIFY, or give up after the poll interval."""
        try:
            await asyncio.wait_for(
                self._wakeup.wait(), timeout=self._settings.dispatcher_poll_interval
            )
        except asyncio.TimeoutError:
            pass
        finally:
            self._wakeup.clear()
    async def _ensure_connection(self) -> asyncpg.Connection:
        if self._conn is not None and not self._conn.is_closed():
            return self._conn

        conn = await asyncpg.connect(self._settings.asyncpg_dsn)
        await init_connection(conn)
        await conn.add_listener(NOTIFY_CHANNEL, self._on_notify)
        self._conn = conn
        logger.info("dispatcher connected, listening on %r", NOTIFY_CHANNEL)
        return conn

    def _on_notify(self, *_: Any) -> None:
        self._wakeup.set()

    async def _close_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None or conn.is_closed():
            return
        try:
            await conn.remove_listener(NOTIFY_CHANNEL, self._on_notify)
        except Exception:
            pass
        await conn.close()

    async def _drain(self) -> int:
        """Publish every pending event, in batches. Returns how many went out."""
        total = 0
        while not self._stopping.is_set():
            published = await self._publish_batch()
            total += published
            if published < self._settings.dispatcher_batch_size:
                break
        return total

    async def _publish_batch(self) -> int:
        conn = await self._ensure_connection()
        envelopes: list[dict[str, Any]] = []

        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", DISPATCH_LOCK_ID)

            rows = await conn.fetch(_CLAIM_SQL, self._settings.dispatcher_batch_size)
            if not rows:
                return 0

            count = len(rows)
            start = await conn.fetchval("SELECT nextval('outbox_stream_seq')")
            if count > 1:
                await conn.execute(
                    "SELECT setval('outbox_stream_seq', $1)", start + count - 1
                )

            ids = [r["id"] for r in rows]
            seqs = [start + i for i in range(count)]
            await conn.execute(_MARK_SQL, ids, seqs)

            for row, seq in zip(rows, seqs):
                event = OutboxEvent.from_row(row)
                envelopes.append(
                    OutboxEvent(
                        id=event.id,
                        aggregate_type=event.aggregate_type,
                        aggregate_id=event.aggregate_id,
                        event_type=event.event_type,
                        payload=event.payload,
                        created_at=event.created_at,
                        published_at=None,
                        stream_seq=seq,
                    ).to_envelope()
                )

        self._hub.publish(envelopes)
        self.published_total += len(envelopes)
        logger.debug(
            "published %d events (stream_seq %s..%s)",
            len(envelopes),
            envelopes[0]["stream_seq"],
            envelopes[-1]["stream_seq"],
        )
        return len(envelopes)
