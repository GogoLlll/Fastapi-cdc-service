"""Stage 2 smoke test: the event stream, end to end over a real WebSocket.

Runs uvicorn in-process on a loopback port and talks to it as an outside
client would -- no ASGI shortcuts, because the point is to measure real
delivery, including the socket.

    PYTHONPATH=. python scripts/smoke_stage2.py

Requires POSTGRES_* env vars pointing at a migrated database.
WARNING: truncates the `items` and `outbox` tables.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import socket
import statistics
import sys
import uuid

import asyncpg
import httpx
import uvicorn
import websockets

from app.core.config import get_settings
from app.db.repository import ItemRepository
from app.main import app

PASSED: list[str] = []
FAILED: list[str] = []

LATENCY_BUDGET_MS = 500.0


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(label)
        print(f"  PASS  {label}" + (f"  ({detail})" if detail else ""), flush=True)
    else:
        FAILED.append(label)
        print(f"  FAIL  {label} {detail}", flush=True)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# ----------------------------------------------------------------------
# A tiny client that mirrors what a real consumer must do
# ----------------------------------------------------------------------
class StreamClient:
    """Collects events in the background and tracks its own cursor.

    This is the reference implementation of the client contract: remember the
    highest stream_seq processed, discard anything at or below it.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._task: asyncio.Task[None] | None = None
        self.events: list[dict] = []
        self.received: list[dt.datetime] = []
        self.control: list[dict] = []
        self.cursor = 0
        self.duplicates = 0
        self.close_code: int | None = None

    async def __aenter__(self) -> "StreamClient":
        self._ws = await websockets.connect(self._url, open_timeout=10)
        self._task = asyncio.create_task(self._reader())
        await self._await_control("ready")
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _reader(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                message = json.loads(raw)
                if "type" in message:
                    self.control.append(message)
                    continue
                seq = message["stream_seq"]
                if seq <= self.cursor:
                    self.duplicates += 1
                    continue
                self.cursor = seq
                self.events.append(message)
                # Stamped per event, not per batch: the latency we care about
                # is per event, and one shared timestamp would measure the
                # span of the whole run instead.
                self.received.append(now())
        except websockets.ConnectionClosed as exc:
            self.close_code = exc.code

    async def _await_control(self, kind: str, timeout: float = 5.0) -> dict:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            for message in self.control:
                if message.get("type") == kind:
                    return message
            await asyncio.sleep(0.01)
        raise AssertionError(f"control frame {kind!r} never arrived")

    async def wait_for(self, count: int, timeout: float = 10.0) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if len(self.events) >= count:
                return True
            await asyncio.sleep(0.005)
        return False

    async def quiesce(self, idle: float = 0.4, timeout: float = 10.0) -> None:
        """Wait until the stream has been silent for `idle` seconds.

        Needed before snapshotting a cursor: `wait_for(n)` returns as soon as
        n events have landed, but events from earlier sections of this script
        may still be in flight, and a cursor taken mid-flight would make the
        replay assertions off by however many were still arriving.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        seen = -1
        while loop.time() < deadline:
            if len(self.events) == seen:
                return
            seen = len(self.events)
            await asyncio.sleep(idle)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                self._task.cancel()
                await self._task


# ----------------------------------------------------------------------
# Checks
# ----------------------------------------------------------------------
async def live_delivery(http: httpx.AsyncClient, ws_url: str) -> None:
    print("\n[1] Live delivery of committed writes", flush=True)

    async with StreamClient(ws_url) as client:
        r = await http.post("/api/v1/items", json={"name": "live", "value": 1})
        item_id = r.json()["item"]["id"]

        check("event arrived on the stream", await client.wait_for(1, timeout=5))
        if not client.events:
            return

        event = client.events[0]
        check("event type is item.created", event["event_type"] == "item.created")
        check("aggregate_id matches the write", event["aggregate_id"] == item_id)
        check("payload carries the full snapshot", event["data"]["name"] == "live")
        check("stream_seq is present and positive", event["stream_seq"] > 0)
        check("event_id matches the write response", event["event_id"] == r.json()["event_id"])

        await http.patch(f"/api/v1/items/{item_id}", json={"value": 2})
        await http.delete(f"/api/v1/items/{item_id}")
        check("update and delete also arrive", await client.wait_for(3, timeout=5))
        check(
            "stream order matches write order",
            [e["event_type"] for e in client.events[:3]]
            == ["item.created", "item.updated", "item.deleted"],
            str([e["event_type"] for e in client.events[:3]]),
        )


async def rollback_emits_nothing(http: httpx.AsyncClient, ws_url: str, pool) -> None:
    print("\n[2] A rolled-back transaction reaches nobody", flush=True)

    async with StreamClient(ws_url) as client:
        marker = f"rollback-{uuid.uuid4()}"

        class Boom(Exception):
            pass

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        "INSERT INTO items (name, value) VALUES ($1, 1) RETURNING *",
                        marker,
                    )
                    await ItemRepository._append_event(
                        conn, "item.created", row["id"], {"id": str(row["id"])}
                    )
                    raise Boom("fail before commit")
        except Boom:
            pass

        # Give the dispatcher several poll intervals to prove a negative.
        await asyncio.sleep(1.0)
        check("nothing was streamed", len(client.events) == 0, f"got {len(client.events)}")

        # A committed write right after must still flow, proving the stream
        # is alive and the silence above was about the rollback, not a stall.
        await http.post("/api/v1/items", json={"name": "after-rollback"})
        check("stream still works afterwards", await client.wait_for(1, timeout=5))


async def per_entity_ordering(http: httpx.AsyncClient, ws_url: str) -> None:
    print("\n[3] Per-entity ordering survives the stream", flush=True)

    r = await http.post("/api/v1/items", json={"name": "ordered", "value": 0})
    item_id = r.json()["item"]["id"]

    async with StreamClient(f"{ws_url}&aggregate_id={item_id}") as client:
        updates = 40
        await asyncio.gather(
            *(
                http.patch(f"/api/v1/items/{item_id}", json={"value": n})
                for n in range(1, updates + 1)
            )
        )
        arrived = await client.wait_for(updates, timeout=15)
        check("all updates arrived", arrived, f"{len(client.events)}/{updates}")

        versions = [e["data"]["version"] for e in client.events]
        seqs = [e["stream_seq"] for e in client.events]
        check("stream_seq strictly increasing", seqs == sorted(seqs))
        check(
            "versions arrive in order, no gaps",
            versions == list(range(versions[0], versions[0] + len(versions))),
            f"{versions[:8]}...",
        )
        check(
            "server-side filter excluded other entities",
            all(e["aggregate_id"] == item_id for e in client.events),
        )


async def reconnect_and_replay(http: httpx.AsyncClient, ws_url: str) -> None:
    print("\n[4] Reconnect: replay fills the gap, dedup removes the overlap", flush=True)

    async with StreamClient(ws_url) as client:
        await http.post("/api/v1/items", json={"name": "before-drop"})
        await client.wait_for(1, timeout=5)
        await client.quiesce()
        cursor = client.cursor
        check("client has a cursor before dropping", cursor > 0, f"seq={cursor}")

    # Offline window: writes happen while nobody is listening.
    missed = 25
    for i in range(missed):
        await http.post("/api/v1/items", json={"name": f"missed-{i}"})

    # Reconnect from the saved cursor.
    async with StreamClient(f"{ws_url.replace('last_event_id=0', f'last_event_id={cursor}')}") as client:
        complete = await client._await_control("replay_complete", timeout=10)
        # The split between "replayed" and "arrived live" is a race by design:
        # the dispatcher may publish the last of the offline writes while the
        # replay is already running. Asserting an exact replay count would be
        # testing the race, not the guarantee. What must hold is that the two
        # paths together cover every missed event exactly once.
        await client.quiesce()
        replayed = complete["count"]
        total = len(client.events)

        check(
            "replay plus live covers every missed event",
            total == missed,
            f"{total}/{missed} (replay={replayed}, live={total - replayed})",
        )
        check(
            "replay never sends more than was missed",
            replayed <= missed,
            f"replay={replayed} missed={missed}",
        )

        seqs = [e["stream_seq"] for e in client.events]
        check(
            "delivered events are contiguous, no gaps",
            seqs == list(range(cursor + 1, cursor + 1 + missed)),
            f"{seqs[:5]}...",
        )
        check("no duplicates below the cursor", all(s > cursor for s in seqs))

        # Live traffic resumes cleanly on top of the replay.
        await http.post("/api/v1/items", json={"name": "after-reconnect"})
        check("live delivery resumes after replay", await client.wait_for(missed + 1, timeout=5))
        check("no duplicates across the replay/live boundary", client.duplicates == 0)


async def _db_side_latency(pool, after_id: int) -> tuple[float, float, float]:
    """insert -> published_at, straight from the database.

    This is the part the service is actually responsible for. The client-side
    number below also includes socket and event-loop time, which in this
    single-process harness is contention with the load generator itself.
    """
    row = await pool.fetchrow(
        """
        SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95,
               max(ms) AS worst
        FROM (
            SELECT extract(epoch FROM (published_at - created_at)) * 1000 AS ms
            FROM outbox
            WHERE published_at IS NOT NULL AND id > $1
        ) t
        """,
        after_id,
    )
    return float(row["p50"] or 0), float(row["p95"] or 0), float(row["worst"] or 0)


def _percentiles(values: list[float]) -> tuple[float, float, float]:
    ordered = sorted(values)
    return (
        statistics.median(ordered),
        ordered[max(0, int(len(ordered) * 0.95) - 1)],
        ordered[-1],
    )


async def latency(http: httpx.AsyncClient, ws_url: str, pool) -> None:
    print("\n[5] Commit-to-delivery latency, paced load", flush=True)

    waves, per_wave = 20, 10
    samples = waves * per_wave
    high_water = await pool.fetchval("SELECT coalesce(max(id), 0) FROM outbox")

    async with StreamClient(ws_url) as client:
        for w in range(waves):
            await asyncio.gather(
                *(
                    http.post("/api/v1/items", json={"name": f"lat-{w}-{i}"})
                    for i in range(per_wave)
                )
            )
            await asyncio.sleep(0.02)
        arrived = await client.wait_for(samples, timeout=30)
        check("every write reached the stream", arrived, f"{len(client.events)}/{samples}")

    if not client.events:
        return

    # occurred_at is stamped inside the transaction, i.e. strictly *before*
    # commit, and received the moment the frame is parsed. Both directions
    # round against us: this is an upper bound, never an optimistic figure.
    deltas = [
        (received - dt.datetime.fromisoformat(event["occurred_at"])).total_seconds() * 1000
        for event, received in zip(client.events, client.received)
    ]
    p50, p95, worst = _percentiles(deltas)
    d50, d95, dworst = await _db_side_latency(pool, high_water)

    print(
        f"        end-to-end   p50={p50:.0f} ms  p95={p95:.0f} ms  max={worst:.0f} ms",
        flush=True,
    )
    print(
        f"        db-side      p50={d50:.0f} ms  p95={d95:.0f} ms  max={dworst:.0f} ms"
        "   (insert -> published)",
        flush=True,
    )
    check(
        f"end-to-end p95 within the {LATENCY_BUDGET_MS:.0f} ms budget",
        p95 <= LATENCY_BUDGET_MS,
        f"p95={p95:.0f} ms",
    )
    check(
        f"end-to-end max within the {LATENCY_BUDGET_MS:.0f} ms budget",
        worst <= LATENCY_BUDGET_MS,
        f"max={worst:.0f} ms",
    )


async def burst(http: httpx.AsyncClient, ws_url: str, pool) -> None:
    print("\n[6] Burst of concurrent writes", flush=True)

    samples = 300
    high_water = await pool.fetchval("SELECT coalesce(max(id), 0) FROM outbox")

    async with StreamClient(ws_url) as client:
        started = now()
        await asyncio.gather(
            *(http.post("/api/v1/items", json={"name": f"burst-{i}"}) for i in range(samples))
        )
        arrived = await client.wait_for(samples, timeout=60)
        elapsed = (now() - started).total_seconds() * 1000

    check("every write in the burst was delivered", arrived, f"{len(client.events)}/{samples}")
    check("no duplicates under burst", client.duplicates == 0)
    seqs = [e["stream_seq"] for e in client.events]
    check("stream_seq monotonic under burst", seqs == sorted(seqs))

    d50, d95, dworst = await _db_side_latency(pool, high_water)
    print(
        f"        {samples} writes in {elapsed:.0f} ms  |  db-side "
        f"p50={d50:.0f} ms  p95={d95:.0f} ms  max={dworst:.0f} ms",
        flush=True,
    )
    # The end-to-end figure is not asserted here: the load generator, the
    # server and the subscriber share one event loop in this harness, so a
    # 300-write burst measures the harness as much as the service. The real
    # concurrency numbers come from the stage 5 load test, run out of process.
    check(
        f"db-side p95 within the {LATENCY_BUDGET_MS:.0f} ms budget under burst",
        d95 <= LATENCY_BUDGET_MS,
        f"p95={d95:.0f} ms",
    )


async def fanout(http: httpx.AsyncClient, ws_url: str) -> None:
    print("\n[7] Fan-out: every subscriber sees every event", flush=True)

    clients = [StreamClient(ws_url) for _ in range(5)]
    for c in clients:
        await c.__aenter__()
    try:
        writes = 20
        await asyncio.gather(
            *(http.post("/api/v1/items", json={"name": f"fan-{i}"}) for i in range(writes))
        )
        results = await asyncio.gather(*(c.wait_for(writes, timeout=10) for c in clients))
        check("all subscribers received all events", all(results),
              f"{[len(c.events) for c in clients]}")
        seq_sets = [tuple(e["stream_seq"] for e in c.events[:writes]) for c in clients]
        check("all subscribers saw the same order", len(set(seq_sets)) == 1)
    finally:
        for c in clients:
            await c.close()


async def consistency_invariant(pool) -> None:
    print("\n[8] Database invariants after the run", flush=True)

    pending = await pool.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
    check("nothing left unpublished", pending == 0, f"pending={pending}")

    mismatched = await pool.fetchval(
        """
        SELECT count(*) FROM outbox
        WHERE (published_at IS NULL) <> (stream_seq IS NULL)
        """
    )
    check("published_at and stream_seq always agree", mismatched == 0)

    duplicates = await pool.fetchval(
        """
        SELECT count(*) FROM (
            SELECT stream_seq FROM outbox
            WHERE stream_seq IS NOT NULL
            GROUP BY stream_seq HAVING count(*) > 1
        ) d
        """
    )
    check("stream_seq is unique", duplicates == 0)

    total, max_seq, min_seq = await pool.fetchrow(
        "SELECT count(*), max(stream_seq), min(stream_seq) FROM outbox"
    )
    check(
        "stream_seq is a dense range with no holes",
        max_seq - min_seq + 1 == total,
        f"count={total} range={min_seq}..{max_seq}",
    )


# ----------------------------------------------------------------------
async def main() -> int:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    settings = get_settings()
    conn = await asyncpg.connect(settings.asyncpg_dsn)
    await conn.execute("TRUNCATE items, outbox RESTART IDENTITY")
    await conn.execute("SELECT setval('outbox_stream_seq', 1, false)")
    await conn.close()

    port = free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)

    base = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}/api/v1/stream?last_event_id=0"

    try:
        async with httpx.AsyncClient(base_url=base, timeout=30.0, trust_env=False) as http:
            health = (await http.get("/health")).json()
            check("dispatcher is running", health.get("dispatcher_running") is True)

            await live_delivery(http, ws_url)
            await rollback_emits_nothing(http, ws_url, app.state.pool)
            await per_entity_ordering(http, ws_url)
            await reconnect_and_replay(http, ws_url)
            await latency(http, ws_url, app.state.pool)
            await burst(http, ws_url, app.state.pool)
            await fanout(http, ws_url)
            await consistency_invariant(app.state.pool)
    finally:
        server.should_exit = True
        await server_task

    print(f"\npassed: {len(PASSED)}   failed: {len(FAILED)}", flush=True)
    return 1 if FAILED else 0


def run() -> None:
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    run()
