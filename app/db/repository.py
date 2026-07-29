from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Any

import asyncpg

from app.core.errors import ItemNotFound, VersionConflict
from app.db.models import Item, OutboxEvent, WriteResult

_ITEM_COLUMNS = "id, name, value, version, created_at, updated_at"
_OUTBOX_COLUMNS = (
    "id, aggregate_type, aggregate_id, event_type, "
    "payload, created_at, published_at, stream_seq"
)

EVENT_CREATED = "item.created"
EVENT_UPDATED = "item.updated"
EVENT_DELETED = "item.deleted"


class ItemRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get(self, item_id: uuid.UUID) -> Item:
        row = await self._pool.fetchrow(
            f"SELECT {_ITEM_COLUMNS} FROM items WHERE id = $1", item_id
        )
        if row is None:
            raise ItemNotFound(item_id)
        return Item.from_row(row)

    async def list(self, limit: int, offset: int) -> tuple[list[Item], int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT {_ITEM_COLUMNS}
                FROM items
                ORDER BY created_at DESC, id
                LIMIT $1 OFFSET $2
                """,
                limit,
                offset,
            )
            total = await conn.fetchval("SELECT count(*) FROM items")
        return [Item.from_row(r) for r in rows], int(total)

    async def create(self, name: str, value: int) -> WriteResult:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"""
                    INSERT INTO items (name, value)
                    VALUES ($1, $2)
                    RETURNING {_ITEM_COLUMNS}
                    """,
                    name,
                    value,
                )
                item = Item.from_row(row)
                event_id = await self._append_event(
                    conn, EVENT_CREATED, item.id, item.to_payload()
                )
        return WriteResult(item=item, event_id=event_id)

    async def update(
        self,
        item_id: uuid.UUID,
        *,
        name: str | None,
        value: int | None,
        expected_version: int | None,
    ) -> WriteResult:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                current = await conn.fetchrow(
                    f"SELECT {_ITEM_COLUMNS} FROM items WHERE id = $1 FOR UPDATE",
                    item_id,
                )
                if current is None:
                    raise ItemNotFound(item_id)

                if (
                    expected_version is not None
                    and current["version"] != expected_version
                ):
                    raise VersionConflict(item_id, expected_version, current["version"])

                row = await conn.fetchrow(
                    f"""
                    UPDATE items
                    SET name       = COALESCE($2, name),
                        value      = COALESCE($3, value),
                        version    = version + 1,
                        updated_at = now()
                    WHERE id = $1
                    RETURNING {_ITEM_COLUMNS}
                    """,
                    item_id,
                    name,
                    value,
                )
                item = Item.from_row(row)
                event_id = await self._append_event(
                    conn, EVENT_UPDATED, item.id, item.to_payload()
                )
        return WriteResult(item=item, event_id=event_id)

    async def delete(
        self, item_id: uuid.UUID, *, expected_version: int | None = None
    ) -> WriteResult:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                current = await conn.fetchrow(
                    f"SELECT {_ITEM_COLUMNS} FROM items WHERE id = $1 FOR UPDATE",
                    item_id,
                )
                if current is None:
                    raise ItemNotFound(item_id)

                if (
                    expected_version is not None
                    and current["version"] != expected_version
                ):
                    raise VersionConflict(item_id, expected_version, current["version"])

                item = Item.from_row(current)
                await conn.execute("DELETE FROM items WHERE id = $1", item_id)

                event_id = await self._append_event(
                    conn, EVENT_DELETED, item.id, item.to_payload()
                )
        return WriteResult(item=item, event_id=event_id)

    @staticmethod
    async def _append_event(
        conn: asyncpg.Connection,
        event_type: str,
        aggregate_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> int:
        return await conn.fetchval(
            """
            INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload)
            VALUES ('item', $1, $2, $3)
            RETURNING id
            """,
            aggregate_id,
            event_type,
            payload,
        )


class OutboxRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def list_events(
        self,
        *,
        after_id: int = 0,
        aggregate_id: uuid.UUID | None = None,
        limit: int = 100,
    ) -> Sequence[OutboxEvent]:
        rows = await self._pool.fetch(
            f"""
            SELECT {_OUTBOX_COLUMNS}
            FROM outbox
            WHERE id > $1
              AND ($2::uuid IS NULL OR aggregate_id = $2)
            ORDER BY id
            LIMIT $3
            """,
            after_id,
            aggregate_id,
            limit,
        )
        return [OutboxEvent.from_row(r) for r in rows]

    async def replay(
        self,
        *,
        after_seq: int,
        aggregate_id: uuid.UUID | None = None,
        limit: int = 500,
    ) -> Sequence[OutboxEvent]:
        rows = await self._pool.fetch(
            f"""
            SELECT {_OUTBOX_COLUMNS}
            FROM outbox
            WHERE stream_seq IS NOT NULL
              AND stream_seq > $1
              AND ($2::uuid IS NULL OR aggregate_id = $2)
            ORDER BY stream_seq
            LIMIT $3
            """,
            after_seq,
            aggregate_id,
            limit,
        )
        return [OutboxEvent.from_row(r) for r in rows]

    async def oldest_replayable_seq(self) -> int | None:
        value = await self._pool.fetchval(
            "SELECT min(stream_seq) FROM outbox WHERE stream_seq IS NOT NULL"
        )
        return int(value) if value is not None else None

    async def current_stream_seq(self) -> int:
        value = await self._pool.fetchval(
            "SELECT coalesce(max(stream_seq), 0) FROM outbox"
        )
        return int(value)

    async def count(self) -> int:
        return int(await self._pool.fetchval("SELECT count(*) FROM outbox"))

    async def count_unpublished(self) -> int:
        return int(
            await self._pool.fetchval(
                "SELECT count(*) FROM outbox WHERE published_at IS NULL"
            )
        )
