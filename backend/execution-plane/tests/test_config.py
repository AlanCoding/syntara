"""Tests for execution-plane configuration."""

import pytest

from execution_plane.config import EPSettings, to_asyncpg_url

_TEST_DB_URL = "postgresql://user:password@localhost/test"


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


@pytest.fixture
def ep_settings() -> EPSettings:
    return EPSettings()


class TestEPSettingsDefaults:
    """Default values match the hardcoded literals they replaced."""

    def test_script_cleanup_terminate_timeout_default(self, ep_settings: EPSettings) -> None:
        assert ep_settings.script_cleanup_terminate_timeout == 1.0

    def test_script_cleanup_kill_timeout_default(self, ep_settings: EPSettings) -> None:
        assert ep_settings.script_cleanup_kill_timeout == 0.5

    def test_max_env_var_length_default(self, ep_settings: EPSettings) -> None:
        assert ep_settings.max_env_var_length == 32768

    def test_temporal_blob_size_error_default(self, ep_settings: EPSettings) -> None:
        assert ep_settings.temporal_blob_size_error == 2_097_152

    def test_temporal_payload_max_bytes_is_ninety_percent_of_blob_size_error(self, ep_settings: EPSettings) -> None:
        assert ep_settings.temporal_payload_max_bytes == int(ep_settings.temporal_blob_size_error * 0.9)


class TestEPSettingsEnvVars:
    """Script execution settings are overridable via environment variables."""

    def test_script_cleanup_terminate_timeout_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SCRIPT_CLEANUP_TERMINATE_TIMEOUT", "5.0")
        assert EPSettings().script_cleanup_terminate_timeout == 5.0

    def test_script_cleanup_kill_timeout_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SCRIPT_CLEANUP_KILL_TIMEOUT", "2.0")
        assert EPSettings().script_cleanup_kill_timeout == 2.0

    def test_max_env_var_length_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAX_ENV_VAR_LENGTH", "65536")
        assert EPSettings().max_env_var_length == 65536

    def test_temporal_blob_size_error_from_env_propagates_to_payload_max(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEMPORAL_BLOB_SIZE_ERROR", "4194304")  # 4 MB
        settings = EPSettings()
        assert settings.temporal_blob_size_error == 4_194_304
        assert settings.temporal_payload_max_bytes == int(4_194_304 * 0.9)
