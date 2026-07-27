from __future__ import annotations

from typing import Annotated

import asyncpg
from fastapi import Depends, Request

from app.db.repository import ItemRepository, OutboxRepository


def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


def get_item_repository(
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
) -> ItemRepository:
    return ItemRepository(pool)


def get_outbox_repository(
    pool: Annotated[asyncpg.Pool, Depends(get_pool)],
) -> OutboxRepository:
    return OutboxRepository(pool)


ItemRepo = Annotated[ItemRepository, Depends(get_item_repository)]
OutboxRepo = Annotated[OutboxRepository, Depends(get_outbox_repository)]
Pool = Annotated[asyncpg.Pool, Depends(get_pool)]
