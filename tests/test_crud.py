from __future__ import annotations

import uuid

import pytest


async def test_create_returns_item_and_event_id(api):
    response = await api.post("/api/v1/items", json={"name": "widget", "value": 7})

    assert response.status_code == 201
    body = response.json()
    assert body["item"]["name"] == "widget"
    assert body["item"]["value"] == 7
    assert body["item"]["version"] == 1
    assert body["event_id"] > 0
    assert response.headers["X-Event-Id"] == str(body["event_id"])


async def test_get_returns_the_created_item(api):
    created = (await api.post("/api/v1/items", json={"name": "a"})).json()
    item_id = created["item"]["id"]

    response = await api.get(f"/api/v1/items/{item_id}")

    assert response.status_code == 200
    assert response.json() == created["item"]


async def test_get_unknown_id_returns_404(api):
    response = await api.get(f"/api/v1/items/{uuid.uuid4()}")

    assert response.status_code == 404
    assert response.json()["code"] == "item_not_found"


async def test_list_paginates(api):
    for i in range(5):
        await api.post("/api/v1/items", json={"name": f"item-{i}"})

    response = await api.get("/api/v1/items", params={"limit": 2, "offset": 1})

    body = response.json()
    assert body["total"] == 5
    assert len(body["items"]) == 2
    assert body["limit"] == 2
    assert body["offset"] == 1


async def test_update_bumps_version_and_preserves_untouched_fields(api):
    created = (await api.post("/api/v1/items", json={"name": "a", "value": 1})).json()
    item_id = created["item"]["id"]

    response = await api.patch(f"/api/v1/items/{item_id}", json={"value": 42})

    assert response.status_code == 200
    item = response.json()["item"]
    assert item["value"] == 42
    assert item["name"] == "a"
    assert item["version"] == 2


async def test_delete_then_get_returns_404(api):
    created = (await api.post("/api/v1/items", json={"name": "a"})).json()
    item_id = created["item"]["id"]

    deleted = await api.delete(f"/api/v1/items/{item_id}")
    assert deleted.status_code == 204
    assert deleted.headers["X-Event-Id"]

    assert (await api.get(f"/api/v1/items/{item_id}")).status_code == 404


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"name": ""}, id="empty-name"),
        pytest.param({"name": "x" * 256}, id="name-too-long"),
        pytest.param({"value": 1}, id="missing-name"),
    ],
)
async def test_create_rejects_invalid_payloads(api, payload):
    assert (await api.post("/api/v1/items", json=payload)).status_code == 422


async def test_update_with_empty_body_is_rejected(api):
    created = (await api.post("/api/v1/items", json={"name": "a"})).json()

    response = await api.patch(f"/api/v1/items/{created['item']['id']}", json={})

    assert response.status_code == 422


class TestOptimisticLocking:
    async def test_matching_version_succeeds(self, api):
        created = (await api.post("/api/v1/items", json={"name": "a"})).json()
        item_id = created["item"]["id"]

        response = await api.patch(
            f"/api/v1/items/{item_id}", json={"value": 1, "version": 1}
        )

        assert response.status_code == 200
        assert response.json()["item"]["version"] == 2

    async def test_stale_version_is_rejected(self, api):
        created = (await api.post("/api/v1/items", json={"name": "a"})).json()
        item_id = created["item"]["id"]
        await api.patch(f"/api/v1/items/{item_id}", json={"value": 1})

        response = await api.patch(
            f"/api/v1/items/{item_id}", json={"value": 2, "version": 1}
        )

        assert response.status_code == 409
        assert response.json()["code"] == "version_conflict"

    async def test_delete_honours_the_version_query_parameter(self, api):
        created = (await api.post("/api/v1/items", json={"name": "a"})).json()
        item_id = created["item"]["id"]

        stale = await api.delete(f"/api/v1/items/{item_id}", params={"version": 99})
        assert stale.status_code == 409

        current = await api.delete(f"/api/v1/items/{item_id}", params={"version": 1})
        assert current.status_code == 204

    async def test_a_rejected_write_leaves_no_event(self, api, pool):
        created = (await api.post("/api/v1/items", json={"name": "a"})).json()
        item_id = created["item"]["id"]

        await api.patch(f"/api/v1/items/{item_id}", json={"value": 1, "version": 99})

        events = await pool.fetchval(
            "SELECT count(*) FROM outbox WHERE aggregate_id = $1", uuid.UUID(item_id)
        )
        assert events == 1


async def test_health_reports_the_background_roles(api):
    body = (await api.get("/health")).json()

    assert body["status"] == "ok"
    assert body["database"] == "ok"
    assert body["dispatcher_running"] is True
    assert body["worker_pid"] is not None
