from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import websockets


def now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def parse_ts(value: str) -> dt.datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return dt.datetime.fromisoformat(value)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * p) - 1))
    return ordered[index]


def summarise(values: list[float]) -> dict[str, float]:
    if not values:
        return {"n": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0}
    return {
        "n": len(values),
        "mean": statistics.fmean(values),
        "p50": percentile(values, 0.50),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values),
    }


@dataclass
class Subscriber:
    url: str
    name: str
    arrivals: dict[int, dt.datetime] = field(default_factory=dict)
    occurred: dict[int, dt.datetime] = field(default_factory=dict)
    seqs: list[int] = field(default_factory=list)
    cursor: int = 0
    duplicates: int = 0
    close_code: int | None = None
    _ws: object | None = None
    _task: asyncio.Task | None = None

    async def start(self) -> None:
        self._ws = await websockets.connect(
            self.url, open_timeout=15, max_queue=None, ping_interval=20
        )
        self._task = asyncio.create_task(self._read())

    async def _read(self) -> None:
        try:
            async for raw in self._ws:  # type: ignore[union-attr]
                message = json.loads(raw)
                if "type" in message:
                    continue
                seq = message["stream_seq"]
                if seq <= self.cursor:
                    self.duplicates += 1
                    continue
                self.cursor = seq
                self.seqs.append(seq)
                self.arrivals[message["event_id"]] = now()
                self.occurred[message["event_id"]] = parse_ts(message["occurred_at"])
        except websockets.ConnectionClosed as exc:
            self.close_code = exc.code
        except Exception as exc:  # noqa: BLE001
            print(f"  [{self.name}] reader failed: {exc!r}", file=sys.stderr)

    async def wait_for(self, count: int, timeout: float) -> bool:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        last_seen = -1
        stalled_since = loop.time()
        while loop.time() < deadline:
            if len(self.arrivals) >= count:
                return True
            if len(self.arrivals) != last_seen:
                last_seen = len(self.arrivals)
                stalled_since = loop.time()
            elif loop.time() - stalled_since > 15:
                return False
            await asyncio.sleep(0.05)
        return False

    @property
    def gaps(self) -> int:
        if len(self.seqs) < 2:
            return 0
        return (self.seqs[-1] - self.seqs[0] + 1) - len(self.seqs)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()  # type: ignore[union-attr]
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


@dataclass
class WriteResult:
    event_id: int
    sent_at: dt.datetime
    responded_at: dt.datetime
    status: int


async def fire_writes(
    base_url: str,
    total: int,
    concurrency: int,
    pool_limit: int,
    rate: float | None = None,
) -> tuple[list[WriteResult], list[str], float, float]:
    results: list[WriteResult] = []
    http_errors: list[str] = []
    transport_errors: list[str] = []
    gate = asyncio.Semaphore(concurrency)

    limits = httpx.Limits(
        max_connections=pool_limit, max_keepalive_connections=pool_limit
    )
    async with httpx.AsyncClient(
        base_url=base_url, timeout=60.0, limits=limits, trust_env=False
    ) as client:

        async def one(i: int) -> None:
            async with gate:
                sent = now()
                try:
                    response = await client.post(
                        "/api/v1/items", json={"name": f"load-{i}", "value": i}
                    )
                except Exception as exc:  # noqa: BLE001
                    transport_errors.append(repr(exc))
                    return
                received = now()
                if response.status_code != 201:
                    http_errors.append(
                        f"HTTP {response.status_code}: {response.text[:120]}"
                    )
                    return
                results.append(
                    WriteResult(
                        event_id=response.json()["event_id"],
                        sent_at=sent,
                        responded_at=received,
                        status=response.status_code,
                    )
                )

        started = time.perf_counter()
        worst_lag = 0.0

        if rate:
            loop = asyncio.get_running_loop()
            interval = 1.0 / rate
            begin = loop.time()
            tasks = []
            for i in range(total):
                due = begin + i * interval
                delay = due - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                else:
                    worst_lag = max(worst_lag, -delay)
                tasks.append(asyncio.create_task(one(i)))
            await asyncio.gather(*tasks)
        else:
            await asyncio.gather(*(one(i) for i in range(total)))

        elapsed = time.perf_counter() - started

    return results, (http_errors, transport_errors), elapsed, worst_lag * 1000


async def service_side_latency(base_url: str, limit: int = 20000) -> dict[str, float]:
    deltas: list[float] = []
    cursor = 0
    async with httpx.AsyncClient(
        base_url=base_url, timeout=30.0, trust_env=False
    ) as client:
        while len(deltas) < limit:
            page = (
                await client.get("/outbox", params={"after_id": cursor, "limit": 1000})
            ).json()
            if not page:
                break
            for row in page:
                if row["published_at"] is None:
                    continue
                created = parse_ts(row["created_at"])
                published = parse_ts(row["published_at"])
                deltas.append((published - created).total_seconds() * 1000)
            cursor = page[-1]["id"]
    return summarise(deltas)


async def run(args: argparse.Namespace) -> int:
    base_url = args.url
    ws_base = base_url.replace("http://", "ws://").replace("https://", "wss://")

    async with httpx.AsyncClient(
        base_url=base_url, timeout=15.0, trust_env=False
    ) as probe:
        try:
            health = (await probe.get("/health")).json()
        except Exception as exc:  # noqa: BLE001
            print(f"Service not reachable at {base_url}: {exc}", file=sys.stderr)
            print(
                "\nCheck, in this order:\n"
                "  docker ps                 does this shell see a Docker daemon?\n"
                "  docker compose ps         is anything running for this project?\n"
                "  docker compose logs app   did it start and then die?\n"
                "\nIf nothing is running:\n"
                "  docker compose up -d --build --wait",
                file=sys.stderr,
            )
            return 2
    if health.get("status") != "ok":
        print(f"Service is not healthy: {health}", file=sys.stderr)
        return 2

    mode = (
        f"open loop at {args.rate:.0f} writes/s"
        if args.rate
        else (f"closed loop, {args.concurrency} in flight")
    )
    print(f"target        {base_url}")
    print(f"writes        {args.writes}")
    print(f"mode          {mode}")
    print(f"subscribers   {args.subscribers}")
    print()

    subscribers = [
        Subscriber(url=f"{ws_base}/api/v1/stream?last_event_id=0", name=f"sub-{i}")
        for i in range(args.subscribers)
    ]
    for sub in subscribers:
        await sub.start()
    await asyncio.sleep(1.0)

    print("firing writes ...")
    writes, (http_errors, transport_errors), elapsed, lag_ms = await fire_writes(
        base_url,
        args.writes,
        args.concurrency,
        args.connection_limit,
        rate=args.rate,
    )

    print(
        f"  {len(writes)}/{args.writes} accepted in {elapsed:.1f}s "
        f"({len(writes) / elapsed:,.0f} writes/s)"
    )

    if args.rate and lag_ms > 50:
        print(
            f"  WARNING: the generator fell up to {lag_ms:.0f} ms behind its own "
            f"schedule -- it cannot sustain {args.rate:.0f}/s, so the latency "
            f"below is understated. Lower --rate or use a bigger client machine."
        )
    if http_errors:
        print(f"  {len(http_errors)} rejected by the service: {http_errors[0]}")
    if transport_errors:
        share = len(transport_errors) / args.writes * 100
        print(
            f"  {len(transport_errors)} connections dropped ({share:.2f}%): "
            f"{transport_errors[0]}"
        )
        print(
            "    Connection-level, not an answer from the service. At this "
            "concurrency it usually means the client, the OS socket backlog "
            "or a port proxy ran out of room."
        )

    print("waiting for the stream to catch up ...")
    reference = subscribers[0]
    complete = await reference.wait_for(len(writes), timeout=args.settle_timeout)
    await asyncio.gather(
        *(s.wait_for(len(writes), timeout=10) for s in subscribers[1:]),
        return_exceptions=True,
    )
    print(f"  {len(reference.arrivals)}/{len(writes)} delivered")

    print("reading service-side latency from /outbox ...")
    service_side = await service_side_latency(base_url)

    report = build_report(
        args,
        health,
        writes,
        http_errors,
        transport_errors,
        elapsed,
        subscribers,
        complete,
        lag_ms,
        service_side,
    )
    print_report(report)
    write_report(report, args.out)

    for sub in subscribers:
        await sub.close()

    return 0 if report["verdict"]["passed"] else 1


def build_report(
    args,
    health,
    writes,
    http_errors,
    transport_errors,
    elapsed,
    subscribers,
    complete,
    lag_ms,
    service_side,
) -> dict:
    reference = subscribers[0]

    write_ms = [(w.responded_at - w.sent_at).total_seconds() * 1000 for w in writes]

    commit_to_delivery: list[float] = []
    request_to_delivery: list[float] = []
    delivered = 0

    for w in writes:
        arrival = reference.arrivals.get(w.event_id)
        if arrival is None:
            continue
        delivered += 1
        occurred = reference.occurred[w.event_id]
        commit_to_delivery.append((arrival - occurred).total_seconds() * 1000)
        request_to_delivery.append((arrival - w.sent_at).total_seconds() * 1000)

    budget = args.budget_ms
    e2e = summarise(commit_to_delivery)
    drop_rate = len(transport_errors) / args.writes if args.writes else 0

    checks = {
        "service rejected nothing": not http_errors,
        "connection drops under 1%": drop_rate < 0.01,
        "every accepted write delivered": delivered == len(writes),
        "no duplicates": all(s.duplicates == 0 for s in subscribers),
        "no gaps in stream_seq": all(s.gaps == 0 for s in subscribers),
        "all subscribers agree": len({tuple(s.seqs) for s in subscribers}) <= 1,
    }
    if args.rate:
        checks[f"commit->delivery p95 <= {budget:.0f} ms"] = e2e["p95"] <= budget
    else:
        checks[f"insert->publish p95 <= {budget:.0f} ms (service side)"] = (
            service_side["p95"] <= budget
        )

    return {
        "generated_at": now().isoformat(timespec="seconds"),
        "target": args.url,
        "settings": {
            "writes": args.writes,
            "mode": "open_loop" if args.rate else "closed_loop",
            "rate": args.rate,
            "concurrency": args.concurrency,
            "subscribers": args.subscribers,
            "budget_ms": budget,
            "generator_schedule_lag_ms": round(lag_ms, 1),
        },
        "service": health,
        "throughput": {
            "accepted": len(writes),
            "http_errors": len(http_errors),
            "transport_errors": len(transport_errors),
            "elapsed_s": round(elapsed, 2),
            "writes_per_s": round(len(writes) / elapsed, 1) if elapsed else 0,
        },
        "latency_ms": {
            "insert_to_publish": service_side,
            "write_response": summarise(write_ms),
            "commit_to_delivery": e2e,
            "request_to_delivery": summarise(request_to_delivery),
        },
        "delivery": {
            "delivered": delivered,
            "expected": len(writes),
            "complete": complete,
            "duplicates": {s.name: s.duplicates for s in subscribers},
            "gaps": {s.name: s.gaps for s in subscribers},
        },
        "verdict": {"checks": checks, "passed": all(checks.values())},
    }


def print_report(report: dict) -> None:
    print()
    print("=" * 66)
    t = report["throughput"]
    print(
        f"  throughput   {t['accepted']} writes in {t['elapsed_s']}s "
        f"= {t['writes_per_s']:,.0f}/s"
    )
    print(
        f"               rejected by service: {t['http_errors']}   "
        f"connections dropped: {t['transport_errors']}"
    )
    print()
    print(f"  {'latency (ms)':<26}{'p50':>8}{'p95':>8}{'p99':>8}{'max':>8}")
    for label, key in (
        ("insert -> publish  [svc]", "insert_to_publish"),
        ("write response", "write_response"),
        ("commit -> delivery", "commit_to_delivery"),
        ("request -> delivery", "request_to_delivery"),
    ):
        s = report["latency_ms"][key]
        print(
            f"  {label:<26}{s['p50']:>8.0f}{s['p95']:>8.0f}"
            f"{s['p99']:>8.0f}{s['max']:>8.0f}"
        )
    if not report["settings"]["rate"]:
        rps = report["throughput"]["writes_per_s"] or 1
        queued = report["settings"]["concurrency"] / rps * 1000
        print()
        print(
            f"  Closed loop: {report['settings']['concurrency']} requests fired at "
            f"once against {rps:,.0f}/s means the last one waits about "
            f"{queued:,.0f} ms in the queue"
        )
        print(
            "  by arithmetic alone. Only the [svc] row is a property of the "
            "service; use --rate to measure latency."
        )
    print()
    for label, ok in report["verdict"]["checks"].items():
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    print("=" * 66)
    print("  RESULT:", "PASSED" if report["verdict"]["passed"] else "FAILED")
    print()


def write_report(report: dict, out: str | None) -> None:
    if not out:
        return
    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"  report written to {path}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load generator for the transactional outbox service.",
        epilog=(
            "Closed loop (default) measures capacity; --rate measures latency "
            "at a sustainable load. See loadtest/RESULTS.md."
        ),
    )

    parser.add_argument("--url", default=os.getenv("LOAD_URL", "http://localhost:8000"))

    parser.add_argument("--writes", type=int, default=5000)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1000,
        help="requests kept in flight at once (the task's target is 1000)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help=(
            "open loop: start this many writes per second regardless of "
            "whether earlier ones finished. Use it to measure latency at a "
            "sustainable load; omit it to measure capacity instead."
        ),
    )
    parser.add_argument("--subscribers", type=int, default=3)
    parser.add_argument(
        "--connection-limit",
        type=int,
        default=1000,
        help="HTTP connections the generator may open",
    )
    parser.add_argument("--budget-ms", type=float, default=500.0)
    parser.add_argument("--settle-timeout", type=float, default=120.0)
    parser.add_argument("--out", default="loadtest/last_run.json")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(asyncio.run(run(parse_args())))
