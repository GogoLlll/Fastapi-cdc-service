from __future__ import annotations

import asyncio
import logging

import asyncpg

from app.core.config import Settings
from app.db.pool import init_connection

logger = logging.getLogger(__name__)

_DELETE_SQL = """
    DELETE FROM outbox
    WHERE id IN (
        SELECT id
        FROM outbox
        WHERE published_at IS NOT NULL
          AND published_at < now() - ($1 || ' hours')::interval
        ORDER BY id
        LIMIT $2
    )
"""


class OutboxRetention:
    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings

        self._task: asyncio.Task[None] | None = None
        self._conn: asyncpg.Connection | None = None
        self._stopping = asyncio.Event()

        self.deleted_total = 0
        self.last_error: str | None = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if not self._settings.outbox_retention_enabled:
            logger.info("retention disabled")
            return
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="outbox-retention")
        logger.info(
            "retention started (window=%dh, every %.0fs)",
            self._settings.outbox_retention_hours,
            self._settings.outbox_retention_interval,
        )

    async def stop(self) -> None:
        self._stopping.set()

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        await self._close_connection()
        if self.deleted_total:
            logger.info("retention stopped (deleted %d events)", self.deleted_total)

    async def _run(self) -> None:
        await asyncio.sleep(self._settings.outbox_retention_interval)

        while not self._stopping.is_set():
            try:
                deleted = await self._sweep()
                if deleted:
                    logger.info("retention removed %d published events", deleted)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = repr(exc)
                logger.exception("retention sweep failed")
            await asyncio.sleep(self._settings.outbox_retention_interval)

    async def _sweep(self) -> int:
        conn = await self._ensure_connection()
        deleted = 0

        while not self._stopping.is_set():
            result = await conn.execute(
                _DELETE_SQL,
                str(self._settings.outbox_retention_hours),
                self._settings.outbox_retention_batch,
            )
            count = int(result.split()[-1])
            deleted += count
            self.deleted_total += count

            if count < self._settings.outbox_retention_batch:
                break
            await asyncio.sleep(0.1)

        return deleted

    async def _ensure_connection(self) -> asyncpg.Connection:
        if self._conn is not None and not self._conn.is_closed():
            return self._conn
        conn = await asyncpg.connect(self._settings.asyncpg_dsn)
        await init_connection(conn)
        self._conn = conn
        return conn

    async def _close_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None and not conn.is_closed():
            await conn.close()
