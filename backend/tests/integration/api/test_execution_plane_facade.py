"""Integration tests for the AO Execution Plane facade pagination contract.

Verifies that the AO facade correctly forwards query parameters to EP and
passes through cursor-based pagination fields in the response.  EP HTTP
calls are intercepted by respx; the AO database (postgres) is real.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
import respx
from httpx import AsyncClient, Response

from syntara.execution_plane import client as ep_client_module

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from contextlib import AbstractContextManager

_EP_BASE = "https://ep.test"

_TARGET_1: dict[str, Any] = {
    "id": "7beaf7e4-0000-4000-8000-000000000001",
    "cluster_id": "7beaf7e4-0000-4000-8000-000000000002",
    "name": "workers-1",
    "backend_type": "vanilla_k8s",
    "endpoint": "https://cluster.example",
    "placement": {
        "type": "kubernetes",
        "namespace": "ep-workers",
        "node_selectors": [],
        "tolerations": [],
    },
    "status": "active",
    "enabled": True,
    "is_default": True,
    "labels": {},
    "created_at": "2026-10-01T00:00:00Z",
}

_TARGET_2: dict[str, Any] = {
    **_TARGET_1,
    "id": "7beaf7e4-0000-4000-8000-000000000003",
    "name": "workers-2",
    "created_at": "2026-10-02T00:00:00Z",
}

_WORK_ITEM_1: dict[str, Any] = {
    "id": "aaaaaaaa-0000-4000-8000-000000000001",
    "status": "completed",
    "result": None,
    "created_at": "2026-10-01T00:00:00Z",
    "claimed_at": None,
    "completed_at": None,
}


@pytest.fixture(autouse=True)
def _ep_settings(override_settings: Callable[..., AbstractContextManager[object]]) -> Generator[None, None, None]:
    """Point the EP client at the respx test base URL and supply a dummy JWT."""
    with (
        override_settings(ep_api_url=_EP_BASE),
        patch.object(ep_client_module, "create_ep_service_token", return_value="test-token"),
    ):
        yield


class TestExecutionTargetsPagination:
    """Execution-target list endpoint forwards query params and exposes cursor fields."""

    @pytest.mark.asyncio
    async def test_first_page_returns_resources_and_next_cursor(
        self,
        auth_client: AsyncClient,
    ) -> None:
        with respx.mock(base_url=_EP_BASE, assert_all_called=True) as ep:
            ep.get("/v1/execution-targets").mock(
                return_value=Response(
                    200,
                    json={
                        "resources": [_TARGET_1],
                        "next": "cursor_page2",
                        "prev": None,
                        "total": 2,
                    },
                )
            )
            resp = await auth_client.get(
                "/api/execution_plane/v1/execution_targets",
                params={"limit": 1},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) == 1
        assert body["resources"][0]["name"] == "workers-1"
        assert body["next"] == "cursor_page2"
        assert body["prev"] is None
        assert body["total"] == 2

    @pytest.mark.asyncio
    async def test_subsequent_page_forwards_cursor_to_ep(
        self,
        auth_client: AsyncClient,
    ) -> None:
        with respx.mock(base_url=_EP_BASE, assert_all_called=True) as ep:
            route = ep.get("/v1/execution-targets").mock(
                return_value=Response(
                    200,
                    json={
                        "resources": [_TARGET_2],
                        "next": None,
                        "prev": "cursor_page1",
                        "total": 2,
                    },
                )
            )
            resp = await auth_client.get(
                "/api/execution_plane/v1/execution_targets",
                params={"limit": 1, "cursor": "cursor_page2"},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) == 1
        assert body["resources"][0]["name"] == "workers-2"
        assert body["next"] is None
        assert body["prev"] == "cursor_page1"

        forwarded = route.calls.last.request
        assert "cursor=cursor_page2" in str(forwarded.url)
        assert "limit=1" in str(forwarded.url)

    @pytest.mark.asyncio
    async def test_terminal_page_has_null_next(
        self,
        auth_client: AsyncClient,
    ) -> None:
        with respx.mock(base_url=_EP_BASE) as ep:
            ep.get("/v1/execution-targets").mock(
                return_value=Response(
                    200,
                    json={"resources": [_TARGET_2], "next": None, "prev": "cursor_prev", "total": 2},
                )
            )
            resp = await auth_client.get("/api/execution_plane/v1/execution_targets")

        assert resp.status_code == 200
        assert resp.json()["next"] is None

    @pytest.mark.asyncio
    async def test_legacy_list_response_from_ep_is_accepted(
        self,
        auth_client: AsyncClient,
    ) -> None:
        """EP returning a bare JSON array still works; pagination fields are null."""
        with respx.mock(base_url=_EP_BASE) as ep:
            ep.get("/v1/execution-targets").mock(return_value=Response(200, json=[_TARGET_1]))
            resp = await auth_client.get("/api/execution_plane/v1/execution_targets")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) == 1
        assert body["next"] is None
        assert body["prev"] is None
        assert body["total"] is None

    @pytest.mark.asyncio
    async def test_invalid_limit_too_low_returns_422(
        self,
        auth_client: AsyncClient,
    ) -> None:
        resp = await auth_client.get(
            "/api/execution_plane/v1/execution_targets",
            params={"limit": 0},
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_invalid_limit_too_high_returns_422(
        self,
        auth_client: AsyncClient,
    ) -> None:
        resp = await auth_client.get(
            "/api/execution_plane/v1/execution_targets",
            params={"limit": 101},
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_cursor_too_long_returns_422(
        self,
        auth_client: AsyncClient,
    ) -> None:
        resp = await auth_client.get(
            "/api/execution_plane/v1/execution_targets",
            params={"cursor": "x" * 1025},
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_limit_forwarded_to_ep(
        self,
        auth_client: AsyncClient,
    ) -> None:
        with respx.mock(base_url=_EP_BASE, assert_all_called=True) as ep:
            route = ep.get("/v1/execution-targets").mock(
                return_value=Response(200, json={"resources": [], "next": None, "prev": None, "total": 0})
            )
            await auth_client.get(
                "/api/execution_plane/v1/execution_targets",
                params={"limit": 7},
            )

        assert "limit=7" in str(route.calls.last.request.url)


class TestWorkItemsPagination:
    """Work-item list endpoint forwards query params and exposes cursor fields."""

    @pytest.mark.asyncio
    async def test_first_page_returns_resources_and_next_cursor(
        self,
        auth_client: AsyncClient,
    ) -> None:
        with respx.mock(base_url=_EP_BASE, assert_all_called=True) as ep:
            ep.get("/v1/work-items").mock(
                return_value=Response(
                    200,
                    json={
                        "resources": [_WORK_ITEM_1],
                        "next": "wi_cursor_page2",
                        "prev": None,
                        "total": 3,
                    },
                )
            )
            resp = await auth_client.get(
                "/api/execution_plane/v1/work_items",
                params={"limit": 1},
            )

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) == 1
        assert body["next"] == "wi_cursor_page2"
        assert body["prev"] is None
        assert body["total"] == 3

    @pytest.mark.asyncio
    async def test_cursor_forwarded_to_ep(
        self,
        auth_client: AsyncClient,
    ) -> None:
        with respx.mock(base_url=_EP_BASE, assert_all_called=True) as ep:
            route = ep.get("/v1/work-items").mock(
                return_value=Response(
                    200,
                    json={"resources": [], "next": None, "prev": "wi_cursor_page1", "total": 1},
                )
            )
            await auth_client.get(
                "/api/execution_plane/v1/work_items",
                params={"limit": 1, "cursor": "wi_cursor_page2"},
            )

        forwarded = route.calls.last.request
        assert "cursor=wi_cursor_page2" in str(forwarded.url)

    @pytest.mark.asyncio
    async def test_invalid_limit_returns_422(
        self,
        auth_client: AsyncClient,
    ) -> None:
        resp = await auth_client.get(
            "/api/execution_plane/v1/work_items",
            params={"limit": 0},
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_ep_unavailable_returns_503(
        self,
        auth_client: AsyncClient,
    ) -> None:
        with respx.mock(base_url=_EP_BASE) as ep:
            ep.get("/v1/work-items").mock(return_value=Response(503))
            resp = await auth_client.get("/api/execution_plane/v1/work_items")

        assert resp.status_code == 503
