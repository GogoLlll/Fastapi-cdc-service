from __future__ import annotations

import json
import logging

import asyncpg

from app.core.config import Settings

logger = logging.getLogger(__name__)


async def init_connection(conn: asyncpg.Connection) -> None:
    """Per-connection setup, run once when a connection joins the pool.

    Registering a jsonb codec lets the repository pass and receive plain dicts
    instead of hand-rolling ``json.dumps`` and ``::jsonb`` casts at every call
    site. The dispatcher's standalone LISTEN connection reuses this too.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_pool(settings: Settings) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        dsn=settings.asyncpg_dsn,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        command_timeout=settings.db_command_timeout,
        init=init_connection,
        statement_cache_size=256,
    )
    logger.info(
        "postgres pool ready (min=%d max=%d)",
        settings.db_pool_min_size,
        settings.db_pool_max_size,
    )
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    await pool.close()
    logger.info("postgres pool closed")
