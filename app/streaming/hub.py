from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)


class SubscriberOverflow(Exception):
    """Raised on the subscriber's own task when its queue overflowed"""


class Subscriber:

    __slots__ = ("id", "queue", "aggregate_id", "cursor", "_overflowed")

    def __init__(
        self,
        *,
        queue_size: int,
        aggregate_id: uuid.UUID | None = None,
        cursor: int = 0,
    ) -> None:
        self.id = uuid.uuid4()
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self.aggregate_id = aggregate_id
        self.cursor = cursor
        self._overflowed = False

    @property
    def overflowed(self) -> bool:
        return self._overflowed

    def wants(self, envelope: dict[str, Any]) -> bool:
        if self.aggregate_id is None:
            return True
        return envelope.get("aggregate_id") == str(self.aggregate_id)

    def offer(self, envelope: dict[str, Any]) -> bool:
        if self._overflowed:
            return False
        try:
            self.queue.put_nowait(envelope)
            return True
        except asyncio.QueueFull:
            self._overflowed = True
            return False

    async def next_event(self, timeout: float) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None


class StreamHub:

    def __init__(self, *, queue_size: int) -> None:
        self._queue_size = queue_size
        self._subscribers: dict[uuid.UUID, Subscriber] = {}
        self._lock = asyncio.Lock()

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def subscribe(
        self, *, aggregate_id: uuid.UUID | None = None, cursor: int = 0
    ) -> Subscriber:
        subscriber = Subscriber(
            queue_size=self._queue_size, aggregate_id=aggregate_id, cursor=cursor
        )
        async with self._lock:
            self._subscribers[subscriber.id] = subscriber
        logger.info(
            "subscriber %s connected (cursor=%d, filter=%s, total=%d)",
            subscriber.id,
            cursor,
            aggregate_id or "none",
            len(self._subscribers),
        )
        return subscriber

    async def unsubscribe(self, subscriber: Subscriber) -> None:
        async with self._lock:
            self._subscribers.pop(subscriber.id, None)
        logger.info(
            "subscriber %s disconnected (total=%d)",
            subscriber.id,
            len(self._subscribers),
        )

    def publish(self, envelopes: list[dict[str, Any]]) -> None:
        if not envelopes:
            return

        for subscriber in list(self._subscribers.values()):
            for envelope in envelopes:
                if not subscriber.wants(envelope):
                    continue
                if not subscriber.offer(envelope):
                    logger.warning(
                        "subscriber %s overflowed at stream_seq=%s, will be dropped",
                        subscriber.id,
                        envelope.get("stream_seq"),
                    )
                    break
                subscriber.cursor = envelope["stream_seq"]
