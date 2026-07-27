from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class Item:
    id: uuid.UUID
    name: str
    value: int
    version: int
    created_at: dt.datetime
    updated_at: dt.datetime

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Item":
        return cls(
            id=row["id"],
            name=row["name"],
            value=row["value"],
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def to_payload(self) -> dict[str, Any]:
        """JSON-serialisable snapshot stored in the outbox payload."""
        return {
            "id": str(self.id),
            "name": self.name,
            "value": self.value,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    """A row of the outbox table.

    ``id`` is assigned at INSERT time and therefore does NOT reflect commit
    order. ``stream_seq`` is assigned by the dispatcher after the transaction
    has committed, and is the value clients use as their cursor.
    """

    id: int
    aggregate_type: str
    aggregate_id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    created_at: dt.datetime
    published_at: dt.datetime | None
    stream_seq: int | None = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "OutboxEvent":
        return cls(
            id=row["id"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            event_type=row["event_type"],
            payload=row["payload"],
            created_at=row["created_at"],
            published_at=row["published_at"],
            stream_seq=row["stream_seq"] if "stream_seq" in row else None,
        )

    def to_envelope(self) -> dict[str, Any]:
        """Wire format sent to WebSocket subscribers."""
        return {
            "stream_seq": self.stream_seq,
            "event_id": self.id,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": str(self.aggregate_id),
            "occurred_at": self.created_at.isoformat(),
            "data": self.payload,
        }


@dataclass(frozen=True, slots=True)
class WriteResult:
    """A committed write plus the id of the outbox event it produced.

    The API returns ``event_id`` so a client can correlate its own write with
    the event it will later receive on the stream.
    """

    item: Item
    event_id: int
