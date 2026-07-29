from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.helpers import StreamClient, create_item

pytestmark = pytest.mark.slow


async def client_for(server) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=server.http_url, timeout=30.0, trust_env=False)


async def test_a_write_on_one_worker_reaches_a_subscriber_on_the_other(workers):
    a, b = workers

    async with await client_for(a) as http_a:
        async with StreamClient(b.ws_url(last_event_id=0)) as sub_b:
            await sub_b.control_frame("ready")

            created = await create_item(http_a, "cross")

            assert await sub_b.wait_for(1)
            assert sub_b.events[0]["aggregate_id"] == created["item"]["id"]


async def test_delivery_works_in_both_directions(workers):
    a, b = workers

    async with await client_for(b) as http_b:
        async with StreamClient(a.ws_url(last_event_id=0)) as sub_a:
            await sub_a.control_frame("ready")
            await create_item(http_b, "cross-back")
            assert await sub_a.wait_for(1)


async def test_both_workers_observe_the_same_total_order(workers):
    a, b = workers

    async with await client_for(a) as http_a, await client_for(b) as http_b:
        async with (
            StreamClient(a.ws_url(last_event_id=0)) as sub_a,
            StreamClient(b.ws_url(last_event_id=0)) as sub_b,
        ):
            await sub_a.control_frame("ready")
            await sub_b.control_frame("ready")

            writes = 40
            await asyncio.gather(
                *(
                    create_item(http_a if i % 2 == 0 else http_b, f"mix-{i}")
                    for i in range(writes)
                )
            )

            assert await sub_a.wait_for(writes)
            assert await sub_b.wait_for(writes)

            assert sub_a.seqs[:writes] == sub_b.seqs[:writes]
            assert sub_a.seqs == sorted(sub_a.seqs)
            assert sub_a.duplicates == 0 and sub_b.duplicates == 0


async def test_only_one_worker_assigns_cursors(workers, pool):
    a, b = workers

    async with await client_for(a) as http_a, await client_for(b) as http_b:
        await asyncio.gather(
            *(create_item(http_a if i % 2 else http_b, f"race-{i}") for i in range(100))
        )
    await asyncio.sleep(1.0)

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

    row = await pool.fetchrow(
        "SELECT count(*) AS n, min(stream_seq) AS lo, max(stream_seq) AS hi "
        "FROM outbox WHERE stream_seq IS NOT NULL"
    )
    assert row["hi"] - row["lo"] + 1 == row["n"]


async def test_health_is_per_worker_but_agrees_on_the_global_head(workers, pool):
    a, b = workers

    async with await client_for(a) as http_a, await client_for(b) as http_b:
        await create_item(http_a, "head")
        await asyncio.sleep(0.6)

        health_a = (await http_a.get("/health")).json()
        health_b = (await http_b.get("/health")).json()

    assert health_a["status"] == health_b["status"] == "ok"
    assert health_a["dispatcher_running"] and health_b["dispatcher_running"]
    assert health_a["stream_head"] == health_b["stream_head"]
    assert health_a["tailer_lag"] == 0
    assert health_b["tailer_lag"] == 0


async def test_a_subscriber_on_a_stopped_worker_can_resume_on_the_other(workers):
    a, b = workers

    async with await client_for(a) as http_a:
        async with StreamClient(b.ws_url(last_event_id=0)) as sub_b:
            await sub_b.control_frame("ready")
            await create_item(http_a, "before")
            assert await sub_b.wait_for(1)
            await sub_b.quiesce()
            cursor = sub_b.cursor

        for i in range(5):
            await create_item(http_a, f"while-away-{i}")
        await asyncio.sleep(0.6)

        async with StreamClient(a.ws_url(last_event_id=cursor)) as sub_a:
            await sub_a.control_frame("replay_complete")
            await sub_a.quiesce()
            assert sub_a.seqs == list(range(cursor + 1, cursor + 6))
