"""Stage 1 smoke test: CRUD and outbox atomicity, against a live database.

Not a replacement for the pytest suite (stage 4) -- this is the quick check
that the write path is sound:

    PYTHONPATH=. python scripts/smoke_stage1.py

Requires POSTGRES_* env vars pointing at a migrated database.
WARNING: truncates the `items` and `outbox` tables.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import uuid

import asyncpg
import httpx

from app.core.config import get_settings
from app.db.repository import ItemRepository
from app.main import app

PASSED: list[str] = []
FAILED: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(label)
        print(f"  PASS  {label}", flush=True)
    else:
        FAILED.append(label)
        print(f"  FAIL  {label} {detail}", flush=True)


async def outbox_max_id(client: httpx.AsyncClient) -> int:
    """Page through the outbox and return the highest event id seen."""
    cursor = 0
    while True:
        page = (
            await client.get("/outbox", params={"after_id": cursor, "limit": 1000})
        ).json()
        if not page:
            return cursor
        cursor = page[-1]["id"]


async def crud_happy_path(client: httpx.AsyncClient) -> None:
    print("\n[1] CRUD happy path", flush=True)

    r = await client.post("/api/v1/items", json={"name": "widget", "value": 7})
    check("POST returns 201", r.status_code == 201, r.text)
    body = r.json()
    item_id = body["item"]["id"]
    check("response carries event_id", body["event_id"] > 0)
    check("X-Event-Id header set", r.headers.get("X-Event-Id") == str(body["event_id"]))
    check("initial version is 1", body["item"]["version"] == 1)

    r = await client.get(f"/api/v1/items/{item_id}")
    check("GET returns the item", r.status_code == 200 and r.json()["name"] == "widget")

    r = await client.get("/api/v1/items", params={"limit": 10})
    check("LIST includes the item", any(i["id"] == item_id for i in r.json()["items"]))

    r = await client.patch(f"/api/v1/items/{item_id}", json={"value": 42})
    check("PATCH returns 200", r.status_code == 200, r.text)
    check("version bumped to 2", r.json()["item"]["version"] == 2)
    check("value updated", r.json()["item"]["value"] == 42)
    check("name preserved", r.json()["item"]["name"] == "widget")

    r = await client.delete(f"/api/v1/items/{item_id}")
    check("DELETE returns 204", r.status_code == 204, r.text)
    check("DELETE emits an event", r.headers.get("X-Event-Id") is not None)

    r = await client.get(f"/api/v1/items/{item_id}")
    check("GET after delete returns 404", r.status_code == 404)

    r = await client.get("/outbox", params={"aggregate_id": item_id})
    types = [e["event_type"] for e in r.json()]
    check(
        "outbox holds created/updated/deleted in order",
        types == ["item.created", "item.updated", "item.deleted"],
        str(types),
    )


async def validation_and_conflicts(client: httpx.AsyncClient) -> None:
    print("\n[2] Validation and optimistic locking", flush=True)

    r = await client.get(f"/api/v1/items/{uuid.uuid4()}")
    check("unknown id returns 404", r.status_code == 404)
    check("404 body has error code", r.json().get("code") == "item_not_found")

    r = await client.post("/api/v1/items", json={"name": "", "value": 1})
    check("empty name rejected with 422", r.status_code == 422)

    r = await client.post("/api/v1/items", json={"name": "lock-me"})
    item_id = r.json()["item"]["id"]

    r = await client.patch(f"/api/v1/items/{item_id}", json={"value": 1, "version": 1})
    check("PATCH with correct version succeeds", r.status_code == 200, r.text)

    r = await client.patch(f"/api/v1/items/{item_id}", json={"value": 2, "version": 1})
    check("PATCH with stale version returns 409", r.status_code == 409, r.text)
    check("409 body has error code", r.json().get("code") == "version_conflict")

    r = await client.patch(f"/api/v1/items/{item_id}", json={})
    check("empty PATCH body rejected with 422", r.status_code == 422)

    r = await client.get("/outbox", params={"aggregate_id": item_id})
    check(
        "rejected writes produced no events",
        len(r.json()) == 2,
        f"got {len(r.json())} events",
    )


async def event_ordering_per_entity(client: httpx.AsyncClient) -> None:
    print("\n[3] Per-entity event ordering under concurrency", flush=True)

    r = await client.post("/api/v1/items", json={"name": "ordered", "value": 0})
    item_id = r.json()["item"]["id"]

    updates = 50
    await asyncio.gather(
        *(
            client.patch(f"/api/v1/items/{item_id}", json={"value": n})
            for n in range(1, updates + 1)
        )
    )

    r = await client.get("/outbox", params={"aggregate_id": item_id, "limit": 200})
    events = r.json()
    ids = [e["id"] for e in events]
    versions = [e["payload"]["version"] for e in events]

    check("one event per write", len(events) == updates + 1, f"got {len(events)}")
    check("outbox ids strictly increasing", ids == sorted(ids))
    check(
        "versions increase monotonically with event id",
        versions == list(range(1, updates + 2)),
        f"got {versions[:10]}...",
    )


async def concurrent_writes(client: httpx.AsyncClient) -> None:
    print("\n[4] Concurrent writes: items and events stay in lockstep", flush=True)

    n = 300
    before = await outbox_max_id(client)

    responses = await asyncio.gather(
        *(
            client.post("/api/v1/items", json={"name": f"bulk-{i}", "value": i})
            for i in range(n)
        )
    )
    created = [r for r in responses if r.status_code == 201]
    check("all concurrent creates succeeded", len(created) == n, f"{len(created)}/{n}")

    event_ids = {r.json()["event_id"] for r in created}
    check("every write got a distinct event id", len(event_ids) == n)

    after = await outbox_max_id(client)
    check(
        "outbox grew by exactly the number of writes",
        after - before == n,
        f"delta={after - before}",
    )


async def rollback_consistency(pool: asyncpg.Pool) -> None:
    print("\n[5] Rollback consistency: no orphan events", flush=True)

    repo = ItemRepository(pool)
    before_items = await pool.fetchval("SELECT count(*) FROM items")
    before_events = await pool.fetchval("SELECT count(*) FROM outbox")

    marker = f"rollback-{uuid.uuid4()}"

    class Boom(Exception):
        pass

    # Reproduce exactly what the repository does, then fail before COMMIT.
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "INSERT INTO items (name, value) VALUES ($1, 1) RETURNING *", marker
                )
                await ItemRepository._append_event(
                    conn, "item.created", row["id"], {"id": str(row["id"])}
                )
                raise Boom("simulated failure after both writes, before commit")
    except Boom:
        pass

    after_items = await pool.fetchval("SELECT count(*) FROM items")
    after_events = await pool.fetchval("SELECT count(*) FROM outbox")
    orphan = await pool.fetchval("SELECT count(*) FROM items WHERE name = $1", marker)

    check("rolled-back item is absent", orphan == 0)
    check("item count unchanged", after_items == before_items)
    check("no orphan event was left behind", after_events == before_events)

    # The mirror image: every committed item has at least one event.
    dangling = await pool.fetchval(
        """
        SELECT count(*) FROM items i
        WHERE NOT EXISTS (SELECT 1 FROM outbox o WHERE o.aggregate_id = i.id)
        """
    )
    check("every committed item has an event", dangling == 0, f"dangling={dangling}")

    # A failing repository write must leave nothing behind either.
    try:
        await repo.update(uuid.uuid4(), name="ghost", value=None, expected_version=None)
    except Exception:
        pass
    check(
        "failed update produced no event",
        await pool.fetchval("SELECT count(*) FROM outbox") == after_events,
    )


async def main() -> int:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = get_settings()

    # Start from a clean slate so counts are deterministic.
    conn = await asyncpg.connect(settings.asyncpg_dsn)
    await conn.execute("TRUNCATE items, outbox RESTART IDENTITY")
    await conn.close()

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=30.0
        ) as client:
            await crud_happy_path(client)
            await validation_and_conflicts(client)
            await event_ordering_per_entity(client)
            await concurrent_writes(client)
        await rollback_consistency(app.state.pool)

    print(f"\npassed: {len(PASSED)}   failed: {len(FAILED)}", flush=True)
    return 1 if FAILED else 0


def run() -> None:
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    run()
