from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator, Iterator

import asyncpg
import httpx
import psycopg2
import pytest
import pytest_asyncio
import uvicorn
from alembic import command
from alembic.config import Config

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])

@pytest.fixture(scope="session")
def test_database() -> Iterator[str]:
    admin_dsn = (
        f"postgresql://{os.getenv('POSTGRES_USER', 'outbox')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'outbox')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/postgres"
    )
    name = os.getenv("POSTGRES_DB", "outbox") + "_test"

    conn = psycopg2.connect(admin_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        cur.execute(f'CREATE DATABASE "{name}"')
    conn.close()

    os.environ["POSTGRES_DB"] = name
    os.environ.setdefault("STREAM_HEARTBEAT_INTERVAL", "1.0")
    from app.core.config import get_settings

    get_settings.cache_clear()

    config = Config(os.path.join(PROJECT_ROOT, "alembic.ini"))
    config.set_main_option("script_location", os.path.join(PROJECT_ROOT, "migrations"))
    command.upgrade(config, "head")

    yield name

    get_settings.cache_clear()
    conn = psycopg2.connect(admin_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    conn.close()


@pytest.fixture(scope="session")
def settings(test_database: str):
    from app.core.config import get_settings

    return get_settings()


@pytest_asyncio.fixture
async def pool(settings) -> AsyncIterator[asyncpg.Pool]:
    from app.db.pool import init_connection

    pool = await asyncpg.create_pool(
        dsn=settings.asyncpg_dsn, min_size=1, max_size=10, init=init_connection
    )
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_database(settings) -> AsyncIterator[None]:
    conn = await asyncpg.connect(settings.asyncpg_dsn)
    await conn.execute("TRUNCATE items, outbox RESTART IDENTITY")
    await conn.execute("SELECT setval('outbox_stream_seq', 1, false)")
    await conn.close()
    yield


@pytest_asyncio.fixture
async def api(settings) -> AsyncIterator[httpx.AsyncClient]:
    from app.main import create_app

    application = create_app()
    async with application.router.lifespan_context(application):
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", timeout=30.0
        ) as client:
            yield client


class LiveServer:
    def __init__(self, application) -> None:
        self.app = application
        self.port = free_port()
        self._server: uvicorn.Server | None = None
        self._task = None

    @property
    def http_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def ws_url(self, **params: object) -> str:
        query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        base = f"ws://127.0.0.1:{self.port}/api/v1/stream"
        return f"{base}?{query}" if query else base

    async def start(self) -> None:
        import asyncio

        config = uvicorn.Config(
            self.app, host="127.0.0.1", port=self.port, log_level="warning"
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())
        while not self._server.started:
            await asyncio.sleep(0.02)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            await self._task


@pytest_asyncio.fixture
async def server(settings) -> AsyncIterator[LiveServer]:
    from app.main import create_app

    live = LiveServer(create_app())
    await live.start()
    try:
        yield live
    finally:
        await live.stop()


@pytest_asyncio.fixture
async def http(server: LiveServer) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=server.http_url, timeout=30.0, trust_env=False
    ) as client:
        yield client


@pytest_asyncio.fixture
async def workers(settings) -> AsyncIterator[list[LiveServer]]:
    from app.main import create_app

    servers = [LiveServer(create_app()) for _ in range(2)]
    for s in servers:
        await s.start()

    try:
        yield servers
    finally:
        for s in servers:
            await s.stop()
