"""Tests for database ownership and lifecycle in WorkStore."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Self
from unittest.mock import AsyncMock

import pytest
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_store import WorkStore


class _Session:
    def __init__(self, item: WorkItem | None = None) -> None:
        self.item = item
        self.added: Any = None
        self.commits = 0
        self.rollbacks = 0
        self.execute = AsyncMock()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def add(self, item: WorkItem) -> None:
        self.added = item

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def get(self, _model: object, _item_id: uuid.UUID) -> WorkItem | None:
        return self.item


class _SessionFactory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self) -> _Session:
        return self.session


def _store() -> WorkStore:
    return WorkStore("postgresql+asyncpg://localhost/syntara")


@pytest.mark.asyncio
async def test_dispatch_owns_session_and_commits_notification() -> None:
    """Dispatch uses an internally managed session and commits the notification."""
    store = _store()
    session = _Session()
    session.execute.return_value = None
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    item = await store.dispatch("handle", uuid.uuid4(), {"input_config": {}})

    assert item.status == WorkItemStatus.PENDING
    assert session.added is item
    assert session.commits == 1
    session.execute.assert_awaited_once()
    await store.close()


@pytest.mark.asyncio
async def test_set_result_reloads_item_by_id() -> None:
    """Result persistence does not require a session-bound WorkItem."""
    store = _store()
    item = WorkItem(
        id=uuid.uuid4(),
        work_correlation_id=uuid.uuid4(),
        activity_handle="handle",
        created_at=datetime.now(UTC),
    )
    session = _Session(item)
    store._session_factory = _SessionFactory(session)  # type: ignore[assignment]

    await store.set_result(item.id, {"output": "ok"}, WorkItemStatus.COMPLETED)

    assert item.status == WorkItemStatus.COMPLETED
    assert item.result == {"output": "ok"}
    assert session.commits == 1
    await store.close()
