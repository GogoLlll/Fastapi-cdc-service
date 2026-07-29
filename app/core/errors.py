from __future__ import annotations

import uuid


class DomainError(Exception):
    status_code: int = 400
    code: str = "domain_error"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ItemNotFound(DomainError):
    status_code = 404
    code = "item_not_found"

    def __init__(self, item_id: uuid.UUID) -> None:
        super().__init__(f"Item {item_id} not found")
        self.item_id = item_id


class VersionConflict(DomainError):
    status_code = 409
    code = "version_conflict"

    def __init__(self, item_id: uuid.UUID, expected: int, actual: int) -> None:
        super().__init__(
            f"Item {item_id} has version {actual}, but version {expected} was expected"
        )
        self.item_id = item_id
        self.expected = expected
        self.actual = actual
