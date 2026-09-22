"""EP worker configuration — reads from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic import AliasChoices, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


def to_asyncpg_url(database_url: str) -> str:
    """Return a PostgreSQL URL without a SQLAlchemy DBAPI driver suffix."""
    return make_url(database_url).set(drivername="postgresql").render_as_string(hide_password=False)


class EPSettings(BaseSettings):
    """Settings for the execution-plane worker."""

    model_config = SettingsConfigDict(extra="ignore")

    # Database — accepts either APP_DATABASE_URL or DATABASE_URL
    database_url: str = Field(
        validation_alias=AliasChoices("APP_DATABASE_URL", "DATABASE_URL"),
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_asyncpg(self) -> str:
        """asyncpg-compatible URL (strips the +asyncpg SQLAlchemy driver prefix)."""
        return to_asyncpg_url(self.database_url)

    # Script execution — process cleanup timing
    script_cleanup_terminate_timeout: float = 1.0
    script_cleanup_kill_timeout: float = 0.5

    # Script execution — per-env-var size cap (bytes)
    max_env_var_length: int = 32768  # 32 KB

    # Temporal payload — must match the server-side blobSize.error in development-sql.yaml
    temporal_blob_size_error: int = 2_097_152  # 2 MB

    @computed_field  # type: ignore[prop-decorator]
    @property
    def temporal_payload_max_bytes(self) -> int:
        """90% of temporal_blob_size_error — headroom for JSON escaping and protobuf overhead."""
        return int(self.temporal_blob_size_error * 0.9)


@lru_cache
def get_ep_settings() -> EPSettings:
    """Load and cache execution-plane settings from the environment."""
    # BaseSettings loads the required database_url from the environment.
    return EPSettings()  # type: ignore[call-arg]
