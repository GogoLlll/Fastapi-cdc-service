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

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "outbox"
    postgres_password: str = "outbox"
    postgres_db: str = "outbox"
    db_pool_min_size: int = 10
    db_pool_max_size: int = 40
    db_command_timeout: float = 10.0

    dispatcher_batch_size: int = 256
    dispatcher_poll_interval: float = 0.2
    dispatcher_debounce: float = 0.005
    dispatcher_error_backoff: float = 1.0

    tailer_batch_size: int = 512
    tailer_poll_interval: float = 0.2
    tailer_debounce: float = 0.005

    outbox_retention_enabled: bool = True
    outbox_retention_hours: int = 24
    outbox_retention_interval: float = 300.0
    outbox_retention_batch: int = 5000
    stream_queue_size: int = 1000
    stream_replay_batch_size: int = 500
    stream_heartbeat_interval: float = 30.0

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_workers: int = 1
    log_level: str = "INFO"

    @property
    def asyncpg_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def alembic_dsn(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
