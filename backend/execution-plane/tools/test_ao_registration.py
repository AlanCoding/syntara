"""Tests for ao_registration — Syntara AO database registration helpers."""

from __future__ import annotations

import uuid
from typing import Self

import ao_registration
import pytest
from ao_registration import register_integration_record

_NAME = "dev-cluster"
_ENDPOINT = "https://api.example.com:6443"
_NAMESPACE = "execution-plane"
_API_KEY = "test-token"
_DATABASE_URL = "postgresql+asyncpg://localhost/syntara_api"
_ACTOR_ID = uuid.UUID(int=0)


class _FakeResult:
    def __init__(self, value: object) -> None:
        self._value = value

    def first(self) -> object:
        return self._value


class _SQLModelSession:
    """Minimal async session stand-in."""

    def __init__(self, exec_results: list[object]) -> None:
        self.exec_results = list(exec_results)
        self.added: list[object] = []
        self.flushed = 0
        self.commits = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def exec(self, _statement: object) -> _FakeResult:
        return _FakeResult(self.exec_results.pop(0) if self.exec_results else None)

    def add(self, item: object) -> None:
        self.added.append(item)

    async def flush(self) -> None:
        self.flushed += 1

    async def commit(self) -> None:
        self.commits += 1


class _FakeEngine:
    async def dispose(self) -> None:
        pass


class _FakeSecretService:
    def __init__(self, created_id: uuid.UUID) -> None:
        self.created_id = created_id
        self.create_calls: list[dict[str, object]] = []
        self.update_calls: list[tuple[uuid.UUID, dict[str, object]]] = []

    async def create_secret(self, fields: dict[str, object]) -> uuid.UUID:
        self.create_calls.append(fields)
        return self.created_id

    async def update_secret(self, secret_id: uuid.UUID, fields: dict[str, object]) -> None:
        self.update_calls.append((secret_id, fields))


def _fake_credential_type() -> object:
    return type("CredentialType", (), {"id": uuid.uuid4(), "name": "HTTP Bearer Token"})()


def _fake_project() -> object:
    return type("Project", (), {"id": uuid.uuid4(), "name": "default", "is_default": True})()


@pytest.mark.asyncio
async def test_register_integration_record_creates_credential_and_integration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bearer_type = _fake_credential_type()
    project = _fake_project()
    session = _SQLModelSession([bearer_type, project, None, None])
    secret_id = uuid.uuid4()
    secret_service = _FakeSecretService(secret_id)

    monkeypatch.setattr(ao_registration, "create_async_engine", lambda *_: _FakeEngine())
    monkeypatch.setattr(ao_registration, "async_sessionmaker", lambda *_, **__: lambda: session)
    monkeypatch.setattr(ao_registration, "create_secret_service", lambda _: secret_service)

    await register_integration_record(
        name=_NAME,
        endpoint=_ENDPOINT,
        namespace=_NAMESPACE,
        api_key=_API_KEY,
        actor_id=_ACTOR_ID,
        database_url=_DATABASE_URL,
    )

    assert secret_service.create_calls == [{"token": _API_KEY}]
    assert session.commits == 1
    added_types = {type(obj).__name__ for obj in session.added}
    assert "Credential" in added_types
    assert "Integration" in added_types


@pytest.mark.asyncio
async def test_register_integration_record_updates_existing_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bearer_type = _fake_credential_type()
    project = _fake_project()
    secret_id = uuid.uuid4()
    existing_credential = type(
        "Credential",
        (),
        {"id": uuid.uuid4(), "secret_id": secret_id, "name": f"{_NAME}-openshift-token"},
    )()
    existing_integration = type(
        "Integration",
        (),
        {
            "name": f"{_NAME}-openshift",
            "configuration": None,
            "management_credential_id": None,
            "updated_by": None,
        },
    )()
    session = _SQLModelSession([bearer_type, project, existing_credential, existing_integration])
    secret_service = _FakeSecretService(uuid.uuid4())

    monkeypatch.setattr(ao_registration, "create_async_engine", lambda *_: _FakeEngine())
    monkeypatch.setattr(ao_registration, "async_sessionmaker", lambda *_, **__: lambda: session)
    monkeypatch.setattr(ao_registration, "create_secret_service", lambda _: secret_service)

    await register_integration_record(
        name=_NAME,
        endpoint=_ENDPOINT,
        namespace=_NAMESPACE,
        api_key="new-token",
        actor_id=_ACTOR_ID,
        database_url=_DATABASE_URL,
    )

    assert secret_service.update_calls == [(secret_id, {"token": "new-token"})]
    assert secret_service.create_calls == []
    assert existing_integration.updated_by == _ACTOR_ID
    assert session.commits == 1


@pytest.mark.asyncio
async def test_register_integration_record_fails_when_bearer_token_type_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _SQLModelSession([None])
    monkeypatch.setattr(ao_registration, "create_async_engine", lambda *_: _FakeEngine())
    monkeypatch.setattr(ao_registration, "async_sessionmaker", lambda *_, **__: lambda: session)
    monkeypatch.setattr(ao_registration, "create_secret_service", lambda _: _FakeSecretService(uuid.uuid4()))

    with pytest.raises(RuntimeError, match="db-seed"):
        await register_integration_record(
            name=_NAME,
            endpoint=_ENDPOINT,
            namespace=_NAMESPACE,
            api_key=_API_KEY,
            actor_id=_ACTOR_ID,
            database_url=_DATABASE_URL,
        )


@pytest.mark.asyncio
async def test_register_integration_record_fails_when_default_project_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bearer_type = _fake_credential_type()
    session = _SQLModelSession([bearer_type, None])
    monkeypatch.setattr(ao_registration, "create_async_engine", lambda *_: _FakeEngine())
    monkeypatch.setattr(ao_registration, "async_sessionmaker", lambda *_, **__: lambda: session)
    monkeypatch.setattr(ao_registration, "create_secret_service", lambda _: _FakeSecretService(uuid.uuid4()))

    with pytest.raises(RuntimeError, match="Default project not found"):
        await register_integration_record(
            name=_NAME,
            endpoint=_ENDPOINT,
            namespace=_NAMESPACE,
            api_key=_API_KEY,
            actor_id=_ACTOR_ID,
            database_url=_DATABASE_URL,
        )
