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

PUBLISH_CHANNEL = "outbox_published"

_TAIL_SQL = """
    SELECT id, aggregate_type, aggregate_id, event_type,
           payload, created_at, published_at, stream_seq
    FROM outbox
    WHERE stream_seq > $1
    ORDER BY stream_seq
    LIMIT $2
"""


class OutboxTailer:
    def __init__(self, *, settings: Settings, hub: StreamHub) -> None:
        self._settings = settings
        self._hub = hub

        self._task: asyncio.Task[None] | None = None
        self._conn: asyncpg.Connection | None = None
        self._wakeup = asyncio.Event()
        self._stopping = asyncio.Event()

        self._cursor = 0
        self.delivered_total = 0
        self.last_error: str | None = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def cursor(self) -> int:
        return self._cursor

    async def start(self) -> None:
        self._stopping.clear()
        conn = await self._ensure_connection()
        self._cursor = int(
            await conn.fetchval("SELECT coalesce(max(stream_seq), 0) FROM outbox")
        )
        self._task = asyncio.create_task(self._run(), name="outbox-tailer")
        logger.info("tailer started at stream_seq=%d", self._cursor)

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
        logger.info("tailer stopped (delivered %d events)", self.delivered_total)

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
                logger.exception("tailer iteration failed, backing off")
                await self._close_connection()
                await asyncio.sleep(self._settings.dispatcher_error_backoff)

    async def _wait_for_signal(self) -> None:
        woken_by_notify = True
        try:
            await asyncio.wait_for(
                self._wakeup.wait(), timeout=self._settings.tailer_poll_interval
            )
        except asyncio.TimeoutError:
            woken_by_notify = False
        finally:
            self._wakeup.clear()

        debounce = self._settings.tailer_debounce
        if woken_by_notify and debounce > 0:
            await asyncio.sleep(debounce)

    async def _ensure_connection(self) -> asyncpg.Connection:
        if self._conn is not None and not self._conn.is_closed():
            return self._conn

        conn = await asyncpg.connect(self._settings.asyncpg_dsn)
        await init_connection(conn)
        await conn.add_listener(PUBLISH_CHANNEL, self._on_notify)
        self._conn = conn
        logger.info("tailer connected, listening on %r", PUBLISH_CHANNEL)
        return conn

    def _on_notify(self, *_: Any) -> None:
        self._wakeup.set()

    async def _close_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None or conn.is_closed():
            return
        try:
            await conn.remove_listener(PUBLISH_CHANNEL, self._on_notify)
        except Exception:  # noqa: BLE001
            pass
        await conn.close()

    async def _drain(self) -> int:
        total = 0
        while not self._stopping.is_set():
            delivered = await self._deliver_batch()
            total += delivered
            if delivered < self._settings.tailer_batch_size:
                break
        return total

    async def _deliver_batch(self) -> int:
        conn = await self._ensure_connection()
        rows = await conn.fetch(
            _TAIL_SQL, self._cursor, self._settings.tailer_batch_size
        )
        if not rows:
            return 0

        envelopes: list[dict[str, Any]] = [
            OutboxEvent.from_row(row).to_envelope() for row in rows
        ]

        self._cursor = int(rows[-1]["stream_seq"])
        self._hub.publish(envelopes)
        self.delivered_total += len(envelopes)

        logger.debug(
            "tailed %d events up to stream_seq=%d", len(envelopes), self._cursor
        )
        return len(envelopes)
