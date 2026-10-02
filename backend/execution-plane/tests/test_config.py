"""Tests for execution-plane configuration."""

import pytest
from execution_plane.config import EPSettings, to_asyncpg_url


def test_to_asyncpg_url_normalizes_postgresql_driver_variants() -> None:
    """Listener URLs use the plain PostgreSQL scheme for every input variant."""
    assert (
        to_asyncpg_url("postgresql+asyncpg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )
    assert (
        to_asyncpg_url("postgresql+psycopg://user:password@localhost/syntara")
        == "postgresql://user:password@localhost/syntara"
    )
    assert (
        to_asyncpg_url("postgresql://user:password@localhost/syntara") == "postgresql://user:password@localhost/syntara"
    )


class TestEPSettingsNodeDispatchDefaults:
    """Node pod dispatch settings default to safe, production-oriented values."""

    def test_node_startup_seconds_default(self) -> None:
        assert EPSettings().node_startup_seconds == 120  # type: ignore[call-arg]

    def test_node_grace_seconds_default(self) -> None:
        assert EPSettings().node_grace_seconds == 30  # type: ignore[call-arg]

    def test_node_k8s_verify_ssl_defaults_true(self) -> None:
        assert EPSettings().node_k8s_verify_ssl is True  # type: ignore[call-arg]

    def test_node_k8s_ca_certificate_defaults_none(self) -> None:
        assert EPSettings().node_k8s_ca_certificate is None  # type: ignore[call-arg]

    def test_dispatch_retry_backoff_seconds_default(self) -> None:
        assert EPSettings().dispatch_retry_backoff_seconds == 5.0  # type: ignore[call-arg]


class TestEPSettingsNodeDispatchEnvVars:
    """Node pod dispatch settings are overridable via environment variables."""

    def test_node_startup_seconds_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("NODE_STARTUP_SECONDS", "300")
        assert EPSettings().node_startup_seconds == 300  # type: ignore[call-arg]

    def test_node_k8s_verify_ssl_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("NODE_K8S_VERIFY_SSL", "false")
        assert EPSettings().node_k8s_verify_ssl is False  # type: ignore[call-arg]

    def test_dispatch_retry_backoff_seconds_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DISPATCH_RETRY_BACKOFF_SECONDS", "1.5")
        assert EPSettings().dispatch_retry_backoff_seconds == 1.5  # type: ignore[call-arg]


class TestEPSettingsDatabaseUrl:
    """The database URL accepts either alias and derives an asyncpg variant."""

    def test_accepts_app_database_url_alias(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("APP_DATABASE_URL", "postgresql+asyncpg://user:password@localhost/syntara")
        settings = EPSettings()  # type: ignore[call-arg]
        assert settings.database_url == "postgresql+asyncpg://user:password@localhost/syntara"

    def test_database_url_asyncpg_strips_driver(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:password@localhost/syntara")
        settings = EPSettings()  # type: ignore[call-arg]
        assert settings.database_url_asyncpg == "postgresql://user:password@localhost/syntara"
