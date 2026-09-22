"""Shared fixtures for execution-plane unit tests."""

import pytest

from execution_plane.config import get_ep_settings


@pytest.fixture(autouse=True)
def ep_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the minimum required env vars for EPSettings and clear the lru_cache."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:password@localhost/test")
    get_ep_settings.cache_clear()
    yield
    get_ep_settings.cache_clear()
