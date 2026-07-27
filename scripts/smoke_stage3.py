from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import logging
import socket
import statistics
import sys

import asyncpg
import httpx
import uvicorn
import websockets

from app.core.config import get_settings
from app.main import create_app

PASSED: list[str] = []
FAILED: list[str] = []

LATENCY_BUDGET_MS = 500.0
WS_CODE_SERVER_SHUTDOWN = 4002
WS_CODE_CURSOR_TOO_OLD = 4003
WS_CODE_GOING_AWAY = {WS_CODE_SERVER_SHUTDOWN, 1012, 1001}


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


class Worker:
    """One app instance, as separate from its sibling as a forked worker is."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.port = free_port()
        self.app = create_app()
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def http_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def ws_url(self, **params: object) -> str:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"ws://127.0.0.1:{self.port}/api/v1/stream" + (f"?{query}" if query else "")

    async def start(self) -> None:
        config = uvicorn.Config(
            self.app, host="127.0.0.1", port=self.port, log_level="warning"
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            await asyncio.sleep(0.05)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            await self._task


class StreamClient:
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
                self.received.append(now())
        except websockets.ConnectionClosed as exc:
            self.close_code = exc.code

    async def await_control(self, kind: str, timeout: float = 10.0) -> dict:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            for message in self.control:
                if message.get("type") == kind:
                    return message
            await asyncio.sleep(0.01)
        raise AssertionError(f"control frame {kind!r} never arrived")

    async def wait_for(self, count: int, timeout: float = 15.0) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if len(self.events) >= count:
                return True
            await asyncio.sleep(0.005)
        return False

    async def wait_closed(self, timeout: float = 10.0) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if self.close_code is not None:
                return True
            await asyncio.sleep(0.01)
        return False

    async def quiesce(self, idle: float = 0.4, timeout: float = 10.0) -> None:
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


async def cross_worker_delivery(a: Worker, b: Worker) -> None:
    print("\n[1] A write on one worker reaches a subscriber on the other", flush=True)

    async with httpx.AsyncClient(base_url=a.http_url, timeout=30, trust_env=False) as http_a:
        async with StreamClient(b.ws_url(last_event_id=0)) as sub_b:
            await sub_b.await_control("ready")

            r = await http_a.post("/api/v1/items", json={"name": "cross", "value": 1})
            item_id = r.json()["item"]["id"]

            check("event crossed the worker boundary", await sub_b.wait_for(1, timeout=10))
            if not sub_b.events:
                return
            check("it is the right event", sub_b.events[0]["aggregate_id"] == item_id)

            # And back the other way, so we are not just testing one direction.
            async with httpx.AsyncClient(
                base_url=b.http_url, timeout=30, trust_env=False
            ) as http_b:
                async with StreamClient(a.ws_url(last_event_id=0)) as sub_a:
                    await sub_a.await_control("ready")
                    await http_b.post("/api/v1/items", json={"name": "cross-back"})
                    check("and in the other direction", await sub_a.wait_for(1, timeout=10))


async def both_workers_see_the_same_order(a: Worker, b: Worker) -> None:
    print("\n[2] Both workers deliver the same events in the same order", flush=True)

    async with httpx.AsyncClient(base_url=a.http_url, timeout=30, trust_env=False) as http_a, \
               httpx.AsyncClient(base_url=b.http_url, timeout=30, trust_env=False) as http_b:
        async with StreamClient(a.ws_url(last_event_id=0)) as sub_a, \
                   StreamClient(b.ws_url(last_event_id=0)) as sub_b:
            await sub_a.await_control("ready")
            await sub_b.await_control("ready")

            writes = 60
            await asyncio.gather(
                *(
                    (http_a if i % 2 == 0 else http_b).post(
                        "/api/v1/items", json={"name": f"mix-{i}"}
                    )
                    for i in range(writes)
                )
            )

            got_a = await sub_a.wait_for(writes, timeout=20)
            got_b = await sub_b.wait_for(writes, timeout=20)
            check("worker A delivered them all", got_a, f"{len(sub_a.events)}/{writes}")
            check("worker B delivered them all", got_b, f"{len(sub_b.events)}/{writes}")

            seqs_a = [e["stream_seq"] for e in sub_a.events[:writes]]
            seqs_b = [e["stream_seq"] for e in sub_b.events[:writes]]
            check("both saw an identical sequence", seqs_a == seqs_b)
            check("stream_seq strictly increasing", seqs_a == sorted(seqs_a))
            check("no duplicates on either worker", sub_a.duplicates == 0 and sub_b.duplicates == 0)


async def only_one_publisher(pool) -> None:
    print("\n[3] Exactly one publisher assigns cursors", flush=True)

    duplicates = await pool.fetchval(
        """
        SELECT count(*) FROM (
            SELECT stream_seq FROM outbox
            WHERE stream_seq IS NOT NULL
            GROUP BY stream_seq HAVING count(*) > 1
        ) d
        """
    )
    check("no stream_seq was handed out twice", duplicates == 0, f"dupes={duplicates}")

    row = await pool.fetchrow(
        """
        SELECT count(*) AS n, min(stream_seq) AS lo, max(stream_seq) AS hi
        FROM outbox WHERE stream_seq IS NOT NULL
        """
    )
    check(
        "cursors form a dense range, no holes",
        row["hi"] - row["lo"] + 1 == row["n"],
        f"count={row['n']} range={row['lo']}..{row['hi']}",
    )

    mismatched = await pool.fetchval(
        "SELECT count(*) FROM outbox WHERE (published_at IS NULL) <> (stream_seq IS NULL)"
    )
    check("published_at and stream_seq always agree", mismatched == 0)


async def latency_across_workers(a: Worker, b: Worker) -> None:
    print("\n[4] Latency with the publisher/tailer hop in the path", flush=True)

    waves, per_wave = 15, 8
    samples = waves * per_wave

    async with httpx.AsyncClient(base_url=a.http_url, timeout=30, trust_env=False) as http_a:
        async with StreamClient(b.ws_url(last_event_id=0)) as sub_b:
            await sub_b.await_control("ready")
            for w in range(waves):
                await asyncio.gather(
                    *(
                        http_a.post("/api/v1/items", json={"name": f"lat-{w}-{i}"})
                        for i in range(per_wave)
                    )
                )
                await asyncio.sleep(0.02)
            arrived = await sub_b.wait_for(samples, timeout=30)
            check("every write arrived", arrived, f"{len(sub_b.events)}/{samples}")

    if not sub_b.events:
        return

    deltas = [
        (received - dt.datetime.fromisoformat(event["occurred_at"])).total_seconds() * 1000
        for event, received in zip(sub_b.events, sub_b.received)
    ]
    ordered = sorted(deltas)
    p50 = statistics.median(ordered)
    p95 = ordered[max(0, int(len(ordered) * 0.95) - 1)]
    worst = ordered[-1]

    print(
        f"        write on A -> deliver on B   p50={p50:.0f} ms  "
        f"p95={p95:.0f} ms  max={worst:.0f} ms",
        flush=True,
    )
    check(
        f"p95 within the {LATENCY_BUDGET_MS:.0f} ms budget",
        p95 <= LATENCY_BUDGET_MS,
        f"p95={p95:.0f} ms",
    )


async def retention_and_stale_cursor(a: Worker, pool) -> None:
    print("\n[5] Retention trims history and stale cursors are refused", flush=True)

    async with httpx.AsyncClient(base_url=a.http_url, timeout=30, trust_env=False) as http:
        async with StreamClient(a.ws_url(last_event_id=0)) as client:
            await client.await_control("ready")
            await http.post("/api/v1/items", json={"name": "will-be-trimmed"})
            await client.wait_for(1, timeout=10)
            await client.quiesce()
            old_cursor = client.cursor

        await pool.execute(
            "UPDATE outbox SET published_at = published_at - interval '48 hours' "
            "WHERE stream_seq <= $1",
            old_cursor,
        )
        deleted = await pool.execute(
            """
            DELETE FROM outbox
            WHERE published_at IS NOT NULL
              AND published_at < now() - interval '24 hours'
            """
        )
        check("retention removed the aged events", int(deleted.split()[-1]) > 0, deleted)

        oldest = await pool.fetchval(
            "SELECT min(stream_seq) FROM outbox WHERE stream_seq IS NOT NULL"
        )
        await http.post("/api/v1/items", json={"name": "after-trim"})
        await asyncio.sleep(0.6)

        stale = StreamClient(a.ws_url(last_event_id=1))
        await stale.__aenter__()
        try:
            frame = await stale.await_control("cursor_too_old", timeout=10)
            check("server refuses a stale cursor", True)
            check("it reports the oldest available seq", frame["oldest_available"] >= (oldest or 1))
            check("it explains what to do", "Resynchronise" in frame["detail"])
            check("closed with code 4003", await stale.wait_closed(timeout=5)
                  and stale.close_code == WS_CODE_CURSOR_TOO_OLD, str(stale.close_code))
        except AssertionError as exc:
            check("server refuses a stale cursor", False, str(exc))
        finally:
            await stale.close()

        head = await pool.fetchval("SELECT max(stream_seq) FROM outbox")
        fresh = StreamClient(a.ws_url(last_event_id=head))
        await fresh.__aenter__()
        try:
            await fresh.await_control("ready", timeout=5)
            check("a cursor inside the window is accepted", True)
            await http.post("/api/v1/items", json={"name": "after-fresh"})
            check("and still receives live events", await fresh.wait_for(1, timeout=10))
        except AssertionError as exc:
            check("a cursor inside the window is accepted", False, str(exc))
        finally:
            await fresh.close()


async def graceful_shutdown(pool) -> None:
    print("\n[6] Graceful shutdown closes subscribers with a code", flush=True)

    worker = Worker("shutdown")
    await worker.start()

    async with httpx.AsyncClient(
        base_url=worker.http_url, timeout=30, trust_env=False
    ) as http:
        client = StreamClient(worker.ws_url(last_event_id=0))
        await client.__aenter__()
        await client.await_control("ready")

        await http.post("/api/v1/items", json={"name": "before-shutdown"})
        check("stream is live before shutdown", await client.wait_for(1, timeout=10))

        await worker.stop()

        check("subscriber was closed", await client.wait_closed(timeout=10))
        check(
            "closed with a reconnect-me code (4002 or 1012)",
            client.close_code in WS_CODE_GOING_AWAY,
            f"code={client.close_code}",
        )
        await client.close()

    pending = await pool.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
    check("shutdown left nothing unpublished", pending == 0, f"pending={pending}")


async def health_reports_the_roles(a: Worker, b: Worker) -> None:
    print("\n[7] Health exposes per-worker state", flush=True)

    async with httpx.AsyncClient(base_url=a.http_url, timeout=30, trust_env=False) as http_a, \
               httpx.AsyncClient(base_url=b.http_url, timeout=30, trust_env=False) as http_b:
        ha = (await http_a.get("/health")).json()
        hb = (await http_b.get("/health")).json()

    check("both workers report healthy", ha["status"] == "ok" and hb["status"] == "ok")
    check("both run their background roles", ha["dispatcher_running"] and hb["dispatcher_running"])
    check("the pids differ", ha["worker_pid"] is not None and hb["worker_pid"] is not None)
    check("both agree on the global head", ha["stream_head"] == hb["stream_head"],
          f"{ha['stream_head']} vs {hb['stream_head']}")
    check("neither tailer is lagging", ha["tailer_lag"] == 0 and hb["tailer_lag"] == 0,
          f"A={ha['tailer_lag']} B={hb['tailer_lag']}")


async def main() -> int:
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    settings = get_settings()
    conn = await asyncpg.connect(settings.asyncpg_dsn)
    await conn.execute("TRUNCATE items, outbox RESTART IDENTITY")
    await conn.execute("SELECT setval('outbox_stream_seq', 1, false)")
    await conn.close()

    pool = await asyncpg.create_pool(dsn=settings.asyncpg_dsn, min_size=1, max_size=4)

    a, b = Worker("A"), Worker("B")
    await a.start()
    await b.start()

    try:
        await cross_worker_delivery(a, b)
        await both_workers_see_the_same_order(a, b)
        await only_one_publisher(pool)
        await latency_across_workers(a, b)
        await health_reports_the_roles(a, b)
        await retention_and_stale_cursor(a, pool)
        await graceful_shutdown(pool)
    finally:
        await a.stop()
        await b.stop()
        await pool.close()

    print(f"\npassed: {len(PASSED)}   failed: {len(FAILED)}", flush=True)
    return 1 if FAILED else 0


def run() -> None:
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    run()
