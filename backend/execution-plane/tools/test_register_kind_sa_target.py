"""Tests for register_kind_sa_target."""

from __future__ import annotations

from typing import TYPE_CHECKING

import register_kind_sa_target
from dev_cli import DEFAULT_DATABASE_URL, EnvironmentProvider

if TYPE_CHECKING:
    from pathlib import Path

    import pytest
    from dev_cli import EnvironmentDetails


def test_register_kind_sa_target_reads_token_and_registers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_file = tmp_path / "sa-token.txt"
    token_file.write_text("sa-token-value\n", encoding="utf-8")
    registered: list[tuple[EnvironmentDetails, str]] = []

    async def _fake_register(details: EnvironmentDetails, database_url: str) -> None:
        registered.append((details, database_url))

    monkeypatch.setattr(register_kind_sa_target, "_register_environment_record", _fake_register)

    assert register_kind_sa_target.main([str(token_file)]) == 0

    details, database_url = registered[0]
    assert details.provider is EnvironmentProvider.KIND
    assert details.name == "execution-plane"
    assert details.endpoint == "https://execution-plane-control-plane:6443"
    assert details.namespace == "execution-plane"
    assert details.api_key == "sa-token-value"
    assert details.labels == {"provider": "kind", "cluster": "execution-plane"}
    assert database_url == DEFAULT_DATABASE_URL


def test_register_kind_sa_target_rejects_an_empty_token_file(tmp_path: Path) -> None:
    token_file = tmp_path / "sa-token.txt"
    token_file.write_text("   \n", encoding="utf-8")

    assert register_kind_sa_target.main([str(token_file)]) == 1
