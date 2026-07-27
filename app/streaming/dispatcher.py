from __future__ import annotations

import logging

from app.core.config import Settings
from app.streaming.hub import StreamHub
from app.streaming.publisher import OutboxPublisher
from app.streaming.retention import OutboxRetention
from app.streaming.tailer import OutboxTailer

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    def __init__(self, *, settings: Settings, hub: StreamHub) -> None:
        self.publisher = OutboxPublisher(settings=settings)
        self.tailer = OutboxTailer(settings=settings, hub=hub)
        self.retention = OutboxRetention(settings=settings)

    @property
    def is_running(self) -> bool:
        return self.publisher.is_running and self.tailer.is_running

    @property
    def published_total(self) -> int:
        return self.publisher.published_total

    @property
    def delivered_total(self) -> int:
        return self.tailer.delivered_total

    @property
    def cursor(self) -> int:
        return self.tailer.cursor

    async def start(self) -> None:
        await self.tailer.start()
        await self.publisher.start()
        await self.retention.start()

    async def stop(self) -> None:
        await self.retention.stop()
        await self.publisher.stop()
        await self.tailer.stop()
