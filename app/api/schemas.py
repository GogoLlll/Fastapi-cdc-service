from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ItemCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    value: int = 0


class ItemUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    value: int | None = None
    version: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Optimistic lock. If supplied and it does not match the stored "
            "version, the write is rejected with 409."
        ),
    )

    @model_validator(mode="after")
    def _at_least_one_field(self) -> "ItemUpdate":
        if self.name is None and self.value is None:
            raise ValueError("at least one of 'name' or 'value' must be provided")
        return self


class ItemRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    value: int
    version: int
    created_at: dt.datetime
    updated_at: dt.datetime


class ItemWriteResponse(BaseModel):
    """A committed item plus the id of the event it generated."""

    item: ItemRead
    event_id: int = Field(
        description=(
            "Outbox id of the event produced by this write. Useful to correlate "
            "a write with the event that later arrives on the stream. Note this "
            "is NOT the stream cursor -- use 'stream_seq' from the event itself."
        )
    )


class ItemListResponse(BaseModel):
    items: list[ItemRead]
    total: int
    limit: int
    offset: int


class OutboxEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    aggregate_type: str
    aggregate_id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    created_at: dt.datetime
    published_at: dt.datetime | None
    stream_seq: int | None


class HealthResponse(BaseModel):
    status: str
    database: str
    worker_pid: int | None = None
    outbox_pending: int | None = None
    stream_head: int | None = None
    tailer_cursor: int | None = None
    tailer_lag: int | None = None
    stream_subscribers: int | None = None
    dispatcher_running: bool | None = None
    retention_running: bool | None = None


class ErrorResponse(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None
