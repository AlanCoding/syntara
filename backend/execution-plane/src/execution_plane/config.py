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
    """Settings for the execution-plane worker, including database and pod dispatch."""

    model_config = SettingsConfigDict(extra="ignore")

    # Database — accepts either APP_DATABASE_URL or DATABASE_URL
    database_url: str = Field(
        validation_alias=AliasChoices("APP_DATABASE_URL", "DATABASE_URL"),
    )

    # --- Cold-start Kubernetes node pod dispatch ---
    # Seconds to wait for a freshly created pod to become Running and its gRPC
    # listener to accept a connection before giving up (retryable).
    node_startup_seconds: int = 120
    # Termination grace period for the pod and the gRPC server's drain window.
    node_grace_seconds: int = 30
    # Local kind/minikube API servers present a self-signed cert. Set false for dev.
    node_k8s_verify_ssl: bool = True
    # Optional PEM CA bundle for the cluster API server, when verification is on.
    node_k8s_ca_certificate: str | None = None

    # Backoff before returning a work item to PENDING after a retryable dispatch
    # failure (e.g. cluster capacity/quota). Throttles the serial poll loop so a
    # persistently unavailable target does not hot-loop.
    dispatch_retry_backoff_seconds: float = 5.0

    @computed_field  # type: ignore[prop-decorator]
    @property
    def database_url_asyncpg(self) -> str:
        """asyncpg-compatible URL (strips the +asyncpg SQLAlchemy driver prefix)."""
        return to_asyncpg_url(self.database_url)


@lru_cache
def get_ep_settings() -> EPSettings:
    """Load and cache execution-plane settings from the environment."""
    # BaseSettings loads the required database_url from the environment.
    return EPSettings()  # type: ignore[call-arg]
