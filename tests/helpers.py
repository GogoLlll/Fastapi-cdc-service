from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
from typing import Any

import websockets

WS_SLOW_CONSUMER = 4001
WS_SERVER_SHUTDOWN = 4002
WS_CURSOR_TOO_OLD = 4003
WS_GOING_AWAY = {WS_SERVER_SHUTDOWN, 1012, 1001}


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class StreamClient:
    def __init__(self, url: str) -> None:
        self._url = url
        self._ws: Any = None
        self._task: asyncio.Task[None] | None = None

        self.events: list[dict] = []
        self.received: list[dt.datetime] = []
        self.control: list[dict] = []
        self.cursor = 0
        self.duplicates = 0
        self.close_code: int | None = None

    async def __aenter__(self) -> StreamClient:
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def connect(self) -> None:
        self._ws = await websockets.connect(self._url, open_timeout=10)
        self._task = asyncio.create_task(self._reader())

    async def _reader(self) -> None:
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
                self.received.append(utcnow())
        except websockets.ConnectionClosed as exc:
            self.close_code = exc.code

    async def control_frame(self, kind: str, timeout: float = 10.0) -> dict:
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

    async def wait_closed(self, timeout: float = 10.0) -> int | None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if self.close_code is not None:
                return self.close_code
            await asyncio.sleep(0.01)
        return None

    async def quiesce(self, idle: float = 0.4, timeout: float = 10.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        seen = -1
        while loop.time() < deadline:
            if len(self.events) == seen:
                return
            seen = len(self.events)
            await asyncio.sleep(idle)

    @property
    def seqs(self) -> list[int]:
        return [e["stream_seq"] for e in self.events]

    @property
    def event_types(self) -> list[str]:
        return [e["event_type"] for e in self.events]

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task


async def create_item(client, name: str, value: int = 0) -> dict:
    response = await client.post("/api/v1/items", json={"name": name, "value": value})
    assert response.status_code == 201, response.text
    return response.json()
