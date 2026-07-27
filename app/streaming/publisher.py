from __future__ import annotations

import asyncio
import logging
from typing import Any

import asyncpg

from app.core.config import Settings
from app.db.pool import init_connection

logger = logging.getLogger(__name__)

WRITE_CHANNEL = "outbox_new"

PUBLISH_LOCK_ID = 918_273_645

_CLAIM_SQL = """
    SELECT id
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


class OutboxPublisher:
    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings

        self._task: asyncio.Task[None] | None = None
        self._conn: asyncpg.Connection | None = None
        self._wakeup = asyncio.Event()
        self._stopping = asyncio.Event()

        self.published_total = 0
        self.skipped_rounds = 0
        self.last_error: str | None = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="outbox-publisher")
        logger.info("publisher started")

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
        logger.info("publisher stopped (published %d events)", self.published_total)

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
                logger.exception("publisher iteration failed, backing off")
                await self._close_connection()
                await asyncio.sleep(self._settings.dispatcher_error_backoff)

    async def _wait_for_signal(self) -> None:
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
        await conn.add_listener(WRITE_CHANNEL, self._on_notify)
        self._conn = conn
        logger.info("publisher connected, listening on %r", WRITE_CHANNEL)
        return conn

    def _on_notify(self, *_: Any) -> None:
        self._wakeup.set()

    async def _close_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None or conn.is_closed():
            return
        try:
            await conn.remove_listener(WRITE_CHANNEL, self._on_notify)
        except Exception:
            pass
        await conn.close()

    async def _drain(self) -> int:
        total = 0
        while not self._stopping.is_set():
            published = await self._publish_batch()
            total += published
            if published < self._settings.dispatcher_batch_size:
                break
        return total

    async def _publish_batch(self) -> int:
        conn = await self._ensure_connection()

        async with conn.transaction():
            if not await conn.fetchval(
                "SELECT pg_try_advisory_xact_lock($1)", PUBLISH_LOCK_ID
            ):
                self.skipped_rounds += 1
                return 0

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

        self.published_total += count
        logger.debug("published %d events (stream_seq %d..%d)", count, seqs[0], seqs[-1])
        return count
