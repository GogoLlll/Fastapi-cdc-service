from __future__ import annotations

import asyncio
import uuid

import pytest

from app.db.repository import ItemRepository


class Boom(Exception):
    """Raised deliberately to abort a transaction mid-flight"""


async def count_items(pool) -> int:
    return await pool.fetchval("SELECT count(*) FROM items")


async def count_events(pool) -> int:
    return await pool.fetchval("SELECT count(*) FROM outbox")


class TestRollback:
    async def test_a_failed_transaction_leaves_neither_row_nor_event(self, pool):
        before_items = await count_items(pool)
        before_events = await count_events(pool)
        marker = f"rollback-{uuid.uuid4()}"

        with pytest.raises(Boom):
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        "INSERT INTO items (name, value) VALUES ($1, 1) RETURNING *",
                        marker,
                    )
                    await ItemRepository._append_event(
                        conn, "item.created", row["id"], {"id": str(row["id"])}
                    )
                    raise Boom

        assert await count_items(pool) == before_items
        assert await count_events(pool) == before_events
        assert await pool.fetchval(
            "SELECT count(*) FROM items WHERE name = $1", marker
        ) == 0

    async def test_a_failed_repository_write_leaves_no_event(self, pool):
        repo = ItemRepository(pool)
        before = await count_events(pool)

        with pytest.raises(Exception):
            await repo.update(
                uuid.uuid4(), name="ghost", value=None, expected_version=None
            )

        assert await count_events(pool) == before

    async def test_rollback_does_not_disturb_a_concurrent_write(self, pool):
        """A doomed transaction must not take a healthy one down with it."""
        repo = ItemRepository(pool)
        marker = f"doomed-{uuid.uuid4()}"

        async def doomed() -> None:
            with pytest.raises(Boom):
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        row = await conn.fetchrow(
                            "INSERT INTO items (name, value) VALUES ($1, 1) RETURNING *",
                            marker,
                        )
                        await ItemRepository._append_event(
                            conn, "item.created", row["id"], {"id": str(row["id"])}
                        )
                        await asyncio.sleep(0.05)
                        raise Boom

        async def healthy() -> None:
            await repo.create("survivor", 1)

        await asyncio.gather(doomed(), healthy())

        assert await pool.fetchval(
            "SELECT count(*) FROM items WHERE name = $1", marker
        ) == 0
        assert await pool.fetchval(
            "SELECT count(*) FROM items WHERE name = 'survivor'"
        ) == 1


class TestInvariants:
    async def test_every_item_has_at_least_one_event(self, api, pool):
        for i in range(20):
            await api.post("/api/v1/items", json={"name": f"item-{i}"})

        dangling = await pool.fetchval(
            """
            SELECT count(*) FROM items i
            WHERE NOT EXISTS (SELECT 1 FROM outbox o WHERE o.aggregate_id = i.id)
            """
        )
        assert dangling == 0

    async def test_every_event_names_a_known_type(self, api, pool):
        created = (await api.post("/api/v1/items", json={"name": "a"})).json()
        item_id = created["item"]["id"]
        await api.patch(f"/api/v1/items/{item_id}", json={"value": 1})
        await api.delete(f"/api/v1/items/{item_id}")

        types = await pool.fetch(
            "SELECT event_type FROM outbox WHERE aggregate_id = $1 ORDER BY id",
            uuid.UUID(item_id),
        )
        assert [r["event_type"] for r in types] == [
            "item.created",
            "item.updated",
            "item.deleted",
        ]

    async def test_a_delete_event_outlives_the_row(self, api, pool):
        """No foreign key, deliberately: the event must survive the deletion."""
        created = (await api.post("/api/v1/items", json={"name": "a"})).json()
        item_id = uuid.UUID(created["item"]["id"])

        await api.delete(f"/api/v1/items/{item_id}")

        event = await pool.fetchrow(
            "SELECT payload FROM outbox WHERE aggregate_id = $1 "
            "AND event_type = 'item.deleted'",
            item_id,
        )
        assert event is not None
        assert event["payload"]["name"] == "a"

    async def test_published_and_cursor_stay_in_lockstep(self, api, pool):
        for i in range(10):
            await api.post("/api/v1/items", json={"name": f"item-{i}"})
        await asyncio.sleep(0.6)

        mismatched = await pool.fetchval(
            "SELECT count(*) FROM outbox "
            "WHERE (published_at IS NULL) <> (stream_seq IS NULL)"
        )
        assert mismatched == 0

    async def test_stream_seq_is_unique(self, api, pool):
        for i in range(30):
            await api.post("/api/v1/items", json={"name": f"item-{i}"})
        await asyncio.sleep(0.6)

        duplicates = await pool.fetchval(
            """
            SELECT count(*) FROM (
                SELECT stream_seq FROM outbox
                WHERE stream_seq IS NOT NULL
                GROUP BY stream_seq HAVING count(*) > 1
            ) d
            """
        )
        assert duplicates == 0
