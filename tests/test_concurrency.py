from __future__ import annotations

import asyncio
import uuid

import pytest

pytestmark = pytest.mark.slow


async def test_concurrent_creates_produce_one_event_each(api, pool):
    n = 200

    responses = await asyncio.gather(
        *(api.post("/api/v1/items", json={"name": f"bulk-{i}"}) for i in range(n))
    )

    assert all(r.status_code == 201 for r in responses)
    assert await pool.fetchval("SELECT count(*) FROM items") == n
    assert await pool.fetchval("SELECT count(*) FROM outbox") == n
    assert len({r.json()["event_id"] for r in responses}) == n


async def test_concurrent_updates_of_one_entity_are_serialised(api, pool):
    created = (await api.post("/api/v1/items", json={"name": "hot", "value": 0})).json()
    item_id = created["item"]["id"]
    updates = 50

    await asyncio.gather(
        *(
            api.patch(f"/api/v1/items/{item_id}", json={"value": n})
            for n in range(1, updates + 1)
        )
    )

    rows = await pool.fetch(
        "SELECT id, payload FROM outbox WHERE aggregate_id = $1 ORDER BY id",
        uuid.UUID(item_id),
    )
    versions = [r["payload"]["version"] for r in rows]

    assert len(rows) == updates + 1
    assert versions == list(range(1, updates + 2))


async def test_optimistic_locking_lets_exactly_one_writer_win(api):
    created = (await api.post("/api/v1/items", json={"name": "contested"})).json()
    item_id = created["item"]["id"]

    results = await asyncio.gather(
        *(
            api.patch(f"/api/v1/items/{item_id}", json={"value": n, "version": 1})
            for n in range(5)
        )
    )

    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409, 409, 409, 409]


async def test_concurrent_writes_to_many_entities_keep_per_entity_order(api, pool):
    entities = 10
    updates_each = 10

    created = await asyncio.gather(
        *(api.post("/api/v1/items", json={"name": f"e-{i}"}) for i in range(entities))
    )
    ids = [r.json()["item"]["id"] for r in created]

    await asyncio.gather(
        *(
            api.patch(f"/api/v1/items/{item_id}", json={"value": n})
            for n in range(1, updates_each + 1)
            for item_id in ids
        )
    )

    for item_id in ids:
        rows = await pool.fetch(
            "SELECT payload FROM outbox WHERE aggregate_id = $1 ORDER BY id",
            uuid.UUID(item_id),
        )
        versions = [r["payload"]["version"] for r in rows]
        assert versions == list(range(1, updates_each + 2)), item_id


async def test_deletes_racing_with_updates_never_orphan_an_event(api, pool):
    created = await asyncio.gather(
        *(api.post("/api/v1/items", json={"name": f"race-{i}"}) for i in range(20))
    )
    ids = [r.json()["item"]["id"] for r in created]

    await asyncio.gather(
        *[api.patch(f"/api/v1/items/{i}", json={"value": 1}) for i in ids],
        *[api.delete(f"/api/v1/items/{i}") for i in ids],
    )

    dangling = await pool.fetchval(
        """
        SELECT count(*) FROM items i
        WHERE NOT EXISTS (SELECT 1 FROM outbox o WHERE o.aggregate_id = i.id)
        """
    )
    assert dangling == 0

    for item_id in ids:
        rows = await pool.fetch(
            "SELECT event_type, payload FROM outbox WHERE aggregate_id = $1 ORDER BY id",
            uuid.UUID(item_id),
        )
        versions = [r["payload"]["version"] for r in rows]
        assert versions == sorted(versions), item_id
        assert rows[0]["event_type"] == "item.created"
