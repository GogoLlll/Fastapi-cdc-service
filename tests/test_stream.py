from __future__ import annotations

import asyncio
import uuid

import pytest

from tests.helpers import WS_SLOW_CONSUMER, StreamClient, create_item


class TestLiveDelivery:
    async def test_a_committed_write_reaches_a_subscriber(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")

            created = await create_item(http, "live", 1)

            assert await client.wait_for(1)
            event = client.events[0]
            assert event["event_type"] == "item.created"
            assert event["aggregate_id"] == created["item"]["id"]
            assert event["data"]["name"] == "live"
            assert event["stream_seq"] > 0
            assert event["event_id"] == created["event_id"]

    async def test_all_three_event_types_arrive_in_order(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")

            created = await create_item(http, "lifecycle")
            item_id = created["item"]["id"]
            await http.patch(f"/api/v1/items/{item_id}", json={"value": 1})
            await http.delete(f"/api/v1/items/{item_id}")

            assert await client.wait_for(3)
            assert client.event_types[:3] == [
                "item.created",
                "item.updated",
                "item.deleted",
            ]

    async def test_a_rolled_back_write_is_never_streamed(self, http, server, pool):
        from app.db.repository import ItemRepository

        class Boom(Exception):
            pass

        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")

            with pytest.raises(Boom):
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        row = await conn.fetchrow(
                            "INSERT INTO items (name, value) VALUES ('ghost', 1) "
                            "RETURNING *"
                        )
                        await ItemRepository._append_event(
                            conn, "item.created", row["id"], {"id": str(row["id"])}
                        )
                        raise Boom

            await asyncio.sleep(1.0)
            assert client.events == []

            await create_item(http, "after-rollback")
            assert await client.wait_for(1)

    async def test_events_carry_a_full_snapshot(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            await create_item(http, "snapshot", 5)
            assert await client.wait_for(1)

            data = client.events[0]["data"]
            assert set(data) == {
                "id",
                "name",
                "value",
                "version",
                "created_at",
                "updated_at",
            }


class TestOrdering:
    async def test_stream_seq_is_monotonic(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")

            await asyncio.gather(*(create_item(http, f"item-{i}") for i in range(40)))

            assert await client.wait_for(40)

            assert client.seqs == sorted(client.seqs)
            assert len(set(client.seqs)) == len(client.seqs)

    async def test_per_entity_order_survives_the_stream(self, http, server):
        created = await create_item(http, "ordered", 0)
        item_id = created["item"]["id"]

        async with StreamClient(
            server.ws_url(last_event_id=0, aggregate_id=item_id)
        ) as client:
            await client.control_frame("ready")

            updates = 30
            await asyncio.gather(
                *(
                    http.patch(f"/api/v1/items/{item_id}", json={"value": n})
                    for n in range(1, updates + 1)
                )
            )
            assert await client.wait_for(updates)

            versions = [e["data"]["version"] for e in client.events]
            assert versions == list(range(versions[0], versions[0] + len(versions)))

    async def test_the_aggregate_filter_excludes_other_entities(self, http, server):
        target = (await create_item(http, "target"))["item"]["id"]

        async with StreamClient(
            server.ws_url(last_event_id=0, aggregate_id=target)
        ) as client:
            await client.control_frame("ready")

            await create_item(http, "noise-1")
            await http.patch(f"/api/v1/items/{target}", json={"value": 1})
            await create_item(http, "noise-2")

            assert await client.wait_for(1)
            await client.quiesce()
            assert all(e["aggregate_id"] == target for e in client.events)


class TestReconnect:
    async def test_replay_delivers_everything_missed(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            await create_item(http, "before-drop")
            assert await client.wait_for(1)
            await client.quiesce()
            cursor = client.cursor

        missed = 20
        for i in range(missed):
            await create_item(http, f"missed-{i}")

        async with StreamClient(server.ws_url(last_event_id=cursor)) as client:
            complete = await client.control_frame("replay_complete")
            await client.quiesce()

            assert len(client.events) == missed
            assert complete["count"] <= missed
            assert client.seqs == list(range(cursor + 1, cursor + 1 + missed))

    async def test_no_duplicates_across_the_replay_boundary(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            await create_item(http, "first")
            assert await client.wait_for(1)
            await client.quiesce()
            cursor = client.cursor

        for i in range(10):
            await create_item(http, f"gap-{i}")

        async with StreamClient(server.ws_url(last_event_id=cursor)) as client:
            await client.control_frame("replay_complete")
            await create_item(http, "live-again")
            assert await client.wait_for(11)
            await client.quiesce()

            assert client.duplicates == 0
            assert len(set(client.seqs)) == len(client.seqs)

    async def test_a_rewound_cursor_replays_only_what_follows_it(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            for i in range(5):
                await create_item(http, f"seen-{i}")
            assert await client.wait_for(5)
            await client.quiesce()
            assert client.seqs == [1, 2, 3, 4, 5]

        async with StreamClient(server.ws_url(last_event_id=2)) as rewound:
            await rewound.control_frame("replay_complete")
            await rewound.quiesce()
            assert rewound.seqs == [3, 4, 5]

    async def test_duplicates_below_the_cursor_are_discarded_by_the_client(
        self, http, server
    ):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            await create_item(http, "original")
            assert await client.wait_for(1)
            await client.quiesce()

            already_seen = dict(client.events[0])
            server.app.state.hub.publish([already_seen])
            await asyncio.sleep(0.3)

            assert len(client.events) == 1
            assert client.duplicates == 1

    async def test_live_only_client_gets_no_history(self, http, server):
        for i in range(5):
            await create_item(http, f"history-{i}")
        await asyncio.sleep(0.6)

        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            ready = await client.control_frame("ready")
            assert ready["resume_from"] == 0
            await client.quiesce(idle=0.5)
            assert client.events == []

            await create_item(http, "new")
            assert await client.wait_for(1)

    async def test_replay_can_be_disabled(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            await create_item(http, "a")
            assert await client.wait_for(1)
            await client.quiesce()
            cursor = client.cursor

        for i in range(5):
            await create_item(http, f"skipped-{i}")
        await asyncio.sleep(0.6)

        async with StreamClient(
            server.ws_url(last_event_id=cursor, replay="false")
        ) as client:
            await client.control_frame("ready")
            await client.quiesce(idle=0.5)
            assert client.events == []


class TestBackpressure:
    async def test_a_slow_consumer_is_disconnected_not_silently_dropped(
        self, http, server, monkeypatch
    ):
        hub = server.app.state.hub
        subscriber = await hub.subscribe()
        try:
            capacity = subscriber.queue.maxsize
            accepted = sum(
                subscriber.offer({"stream_seq": i, "aggregate_id": str(uuid.uuid4())})
                for i in range(capacity + 5)
            )

            assert accepted == capacity
            assert subscriber.overflowed is True
        finally:
            await hub.unsubscribe(subscriber)

    async def test_overflowed_subscriber_gets_close_code_4001(self, http, server):
        server.app.state.hub._queue_size = 1

        client = StreamClient(server.ws_url(last_event_id=0))
        await client.connect()
        try:
            await client.control_frame("ready")
            await asyncio.gather(*(create_item(http, f"flood-{i}") for i in range(200)))
            code = await client.wait_closed(timeout=15)
            assert code == WS_SLOW_CONSUMER, f"got {code}"
        finally:
            await client.close()
            server.app.state.hub._queue_size = 1000


class TestFanOut:
    async def test_every_subscriber_receives_every_event(self, http, server):
        clients = [StreamClient(server.ws_url(last_event_id=0)) for _ in range(5)]
        for client in clients:
            await client.connect()
            await client.control_frame("ready")
        try:
            writes = 15
            await asyncio.gather(*(create_item(http, f"fan-{i}") for i in range(writes)))

            assert all([await c.wait_for(writes) for c in clients])
            assert len({tuple(c.seqs[:writes]) for c in clients}) == 1
        finally:
            for client in clients:
                await client.close()

    async def test_health_counts_connected_subscribers(self, http, server):
        async with StreamClient(server.ws_url(last_event_id=0)) as client:
            await client.control_frame("ready")
            body = (await http.get("/health")).json()
            assert body["stream_subscribers"] >= 1

        await asyncio.sleep(0.3)
        body = (await http.get("/health")).json()
        assert body["stream_subscribers"] == 0
