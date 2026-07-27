from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- PostgreSQL ---
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "outbox"
    postgres_password: str = "outbox"
    postgres_db: str = "outbox"

    # --- Connection pool ---
    # Pool size is the real concurrency limiter for writes. Keep it well below
    # Postgres `max_connections`. The dispatcher holds one dedicated connection
    # for LISTEN outside of this pool.
    db_pool_min_size: int = 10
    db_pool_max_size: int = 40
    db_command_timeout: float = 10.0

    # --- Dispatcher ---
    # How many events one dispatcher iteration claims and publishes. Larger
    # batches amortise round trips; smaller batches keep tail latency down.
    dispatcher_batch_size: int = 256
    # Safety-net poll. LISTEN/NOTIFY is the fast path; this interval only
    # bounds the damage if a notification is ever missed (dropped connection,
    # notify queue overflow). It is the worst-case latency, not the typical one.
    dispatcher_poll_interval: float = 0.2
    # Backoff after a dispatcher error before retrying, in seconds.
    dispatcher_error_backoff: float = 1.0

    # --- Streaming ---
    # Per-subscriber send queue. A client that cannot keep up fills its queue
    # and gets disconnected; it then reconnects and replays from its cursor.
    # Nothing is silently dropped.
    stream_queue_size: int = 1000
    # Max events returned in one replay page when a client reconnects.
    stream_replay_batch_size: int = 500
    # Ping interval to keep idle connections and proxies alive, in seconds.
    stream_heartbeat_interval: float = 30.0

    # --- App ---
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "INFO"

    @property
    def asyncpg_dsn(self) -> str:
        """DSN for the async runtime driver (asyncpg)."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def alembic_dsn(self) -> str:
        """DSN for Alembic, which runs synchronously via psycopg2."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
