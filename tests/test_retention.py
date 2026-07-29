from __future__ import annotations

import asyncio

import pytest

from tests.helpers import WS_CURSOR_TOO_OLD, StreamClient, create_item

pytestmark = pytest.mark.slow


async def age_and_trim(pool, up_to_seq: int, *, hours: int = 48) -> int:
    await pool.execute(
        "UPDATE outbox SET published_at = published_at - ($1 || ' hours')::interval "
        "WHERE stream_seq <= $2",
        str(hours),
        up_to_seq,
    )
    result = await pool.execute(
        "DELETE FROM outbox WHERE published_at IS NOT NULL "
        "AND published_at < now() - interval '24 hours'"
    )
    return int(result.split()[-1])


class TestRetention:
    async def test_published_events_older_than_the_window_are_removed(
        self, http, server, pool
    ):
        for i in range(5):
            await create_item(http, f"old-{i}")
        await asyncio.sleep(0.6)

        head = await pool.fetchval("SELECT max(stream_seq) FROM outbox")
        deleted = await age_and_trim(pool, head)

        assert deleted == 5
        assert await pool.fetchval("SELECT count(*) FROM outbox") == 0

    async def test_unpublished_events_are_never_removed(self, pool):
        await pool.execute(
            """
            INSERT INTO outbox (aggregate_id, event_type, payload, created_at)
            VALUES (gen_random_uuid(), 'item.created', '{}'::jsonb,
                    now() - interval '90 days')
            """
        )

        result = await pool.execute(
            "DELETE FROM outbox WHERE published_at IS NOT NULL "
            "AND published_at < now() - interval '24 hours'"
        )

        assert int(result.split()[-1]) == 0
        assert await pool.fetchval("SELECT count(*) FROM outbox") == 1

    async def test_retention_does_not_disturb_recent_events(self, http, server, pool):
        for i in range(3):
            await create_item(http, f"old-{i}")
        await asyncio.sleep(0.6)
        boundary = await pool.fetchval("SELECT max(stream_seq) FROM outbox")

        for i in range(3):
            await create_item(http, f"new-{i}")
        await asyncio.sleep(0.6)

        await age_and_trim(pool, boundary)

        assert await pool.fetchval("SELECT count(*) FROM outbox") == 3
        assert await pool.fetchval("SELECT min(stream_seq) FROM outbox") == boundary + 1


class TestStaleCursor:
    async def test_a_cursor_before_the_window_is_refused(self, http, server, pool):
        for i in range(5):
            await create_item(http, f"trimmed-{i}")
        await asyncio.sleep(0.6)

        boundary = await pool.fetchval("SELECT max(stream_seq) FROM outbox")
        await age_and_trim(pool, boundary)
        await create_item(http, "after-trim")
        await asyncio.sleep(0.6)

        client = StreamClient(server.ws_url(last_event_id=1))
        await client.connect()
        try:
            frame = await client.control_frame("cursor_too_old")
            assert frame["requested"] == 1
            assert frame["oldest_available"] > 1
            assert "Resynchronise" in frame["detail"]
            assert await client.wait_closed() == WS_CURSOR_TOO_OLD
        finally:
            await client.close()

    async def test_a_cursor_inside_the_window_is_served(self, http, server, pool):
        for i in range(5):
            await create_item(http, f"trimmed-{i}")
        await asyncio.sleep(0.6)

        boundary = await pool.fetchval("SELECT max(stream_seq) FROM outbox")
        await age_and_trim(pool, boundary)
        for i in range(3):
            await create_item(http, f"kept-{i}")
        await asyncio.sleep(0.6)

        oldest = await pool.fetchval("SELECT min(stream_seq) FROM outbox")

        async with StreamClient(server.ws_url(last_event_id=oldest - 1)) as client:
            complete = await client.control_frame("replay_complete")
            assert complete["count"] == 3
            assert client.seqs == [oldest, oldest + 1, oldest + 2]

    async def test_a_fresh_client_is_never_refused(self, http, server, pool):
        for i in range(3):
            await create_item(http, f"x-{i}")
        await asyncio.sleep(0.6)
        head = await pool.fetchval("SELECT max(stream_seq) FROM outbox")
        await age_and_trim(pool, head)

        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            await create_item(http, "new")
            assert await client.wait_for(1)
