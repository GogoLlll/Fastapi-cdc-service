from __future__ import annotations

import random
import uuid

from locust import HttpUser, between, task


class WriterUser(HttpUser):
    wait_time = between(0.05, 0.2)

    def on_start(self) -> None:
        self.owned: list[tuple[str, int]] = []

    @task(5)
    def create(self) -> None:
        with self.client.post(
            "/api/v1/items",
            json={"name": f"locust-{uuid.uuid4().hex[:8]}", "value": 0},
            name="POST /items",
            catch_response=True,
        ) as response:
            if response.status_code != 201:
                response.failure(f"HTTP {response.status_code}")
                return
            body = response.json()
            if len(self.owned) < 20:
                self.owned.append((body["item"]["id"], body["item"]["version"]))

    @task(3)
    def update(self) -> None:
        if not self.owned:
            return
        index = random.randrange(len(self.owned))
        item_id, version = self.owned[index]

        with self.client.patch(
            f"/api/v1/items/{item_id}",
            json={"value": random.randint(0, 1000), "version": version},
            name="PATCH /items/{id}",
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                self.owned[index] = (item_id, response.json()["item"]["version"])
            elif response.status_code == 409:
                response.success()
                self.owned.pop(index)
            else:
                response.failure(f"HTTP {response.status_code}")

    @task(1)
    def read(self) -> None:
        self.client.get("/api/v1/items?limit=20", name="GET /items")
