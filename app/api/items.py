from __future__ import annotations

import uuid

from fastapi import APIRouter, Query, Response, status

from app.api.deps import ItemRepo
from app.api.schemas import (
    ErrorResponse,
    ItemCreate,
    ItemListResponse,
    ItemRead,
    ItemUpdate,
    ItemWriteResponse,
)

router = APIRouter(prefix="/items", tags=["items"])

_NOT_FOUND = {404: {"model": ErrorResponse, "description": "Item does not exist"}}
_CONFLICT = {409: {"model": ErrorResponse, "description": "Version conflict"}}


@router.post(
    "",
    response_model=ItemWriteResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an item",
)
async def create_item(
    payload: ItemCreate, repo: ItemRepo, response: Response
) -> ItemWriteResponse:
    result = await repo.create(payload.name, payload.value)
    response.headers["X-Event-Id"] = str(result.event_id)
    return ItemWriteResponse(
        item=ItemRead.model_validate(result.item), event_id=result.event_id
    )


@router.get("", response_model=ItemListResponse, summary="List items")
async def list_items(
    repo: ItemRepo,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> ItemListResponse:
    items, total = await repo.list(limit=limit, offset=offset)
    return ItemListResponse(
        items=[ItemRead.model_validate(i) for i in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/{item_id}",
    response_model=ItemRead,
    responses=_NOT_FOUND,
    summary="Get one item",
)
async def get_item(item_id: uuid.UUID, repo: ItemRepo) -> ItemRead:
    return ItemRead.model_validate(await repo.get(item_id))


@router.patch(
    "/{item_id}",
    response_model=ItemWriteResponse,
    responses={**_NOT_FOUND, **_CONFLICT},
    summary="Update an item",
)
async def update_item(
    item_id: uuid.UUID, payload: ItemUpdate, repo: ItemRepo, response: Response
) -> ItemWriteResponse:
    result = await repo.update(
        item_id,
        name=payload.name,
        value=payload.value,
        expected_version=payload.version,
    )
    response.headers["X-Event-Id"] = str(result.event_id)
    return ItemWriteResponse(
        item=ItemRead.model_validate(result.item), event_id=result.event_id
    )


@router.delete(
    "/{item_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={**_NOT_FOUND, **_CONFLICT},
    summary="Delete an item",
)
async def delete_item(
    item_id: uuid.UUID,
    repo: ItemRepo,
    version: int | None = Query(
        default=None, ge=1, description="Optional optimistic lock"
    ),
) -> Response:
    result = await repo.delete(item_id, expected_version=version)
    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
        headers={"X-Event-Id": str(result.event_id)},
    )
