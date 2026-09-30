"""Unit tests for execute_script_activity — Temporal gate, validation, and EP dispatch envelope.

Subprocess execution moved out of the Execution Plane into the cold-start node
container (invoked over gRPC by the EP worker), so this activity no longer runs
scripts. It only gates on the feature flag, validates the config, and writes a
WorkItem carrying the invocation envelope + image for the EP worker to dispatch.
"""

from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from temporalio.exceptions import ApplicationError

from syntara.core.config.base import get_settings
from syntara.workflows.workflow_engine.activities.ep import ep_dispatch_activity
from syntara.workflows.workflow_engine.activities.ep.ep_dispatch_activity import (
    _build_invocation,
    execute_script_activity,
)

ACTIVITY_INFO_PATH = "syntara.workflows.workflow_engine.activities.ep.ep_dispatch_activity.activity.info"


@pytest.fixture(autouse=True)
def _mock_activity_context() -> Generator[MagicMock, None, None]:
    """Auto-mock activity.info() and activity.heartbeat() so tests run outside a Temporal worker."""
    mock_info = MagicMock()
    mock_info.attempt = 1
    mock_info.workflow_id = "wf-123"
    mock_info.activity_id = "act-456"
    mock_info.task_token = b"token-bytes"
    with patch(ACTIVITY_INFO_PATH, return_value=mock_info) as m, patch("temporalio.activity.heartbeat"):
        yield m


@pytest.fixture
def mock_activity_info(_mock_activity_context: MagicMock) -> MagicMock:
    """Expose the mock so tests can customise attempt number etc."""
    return _mock_activity_context


class TestPydanticConfigValidation:
    """ScriptExecutorParameters.model_validate() is enforced before dispatch."""

    @pytest.mark.asyncio
    async def test_empty_code_raises_config_error(self) -> None:
        input_config = {"language": "bash", "code": ""}
        with pytest.raises(ApplicationError) as exc_info:
            await execute_script_activity(input_config, None)
        assert exc_info.value.type == "ConfigError"

    @pytest.mark.asyncio
    async def test_invalid_language_raises_config_error(self) -> None:
        input_config = {"language": "ruby", "code": "puts 'hello'"}
        with pytest.raises(ApplicationError) as exc_info:
            await execute_script_activity(input_config, None)
        assert exc_info.value.type == "ConfigError"

    @pytest.mark.asyncio
    async def test_missing_code_field_raises_config_error(self) -> None:
        input_config = {"language": "bash"}
        with pytest.raises(ApplicationError) as exc_info:
            await execute_script_activity(input_config, None)
        assert exc_info.value.type == "ConfigError"

    @pytest.mark.asyncio
    async def test_missing_language_field_raises_config_error(self) -> None:
        input_config = {"code": "echo hello"}
        with pytest.raises(ApplicationError) as exc_info:
            await execute_script_activity(input_config, None)
        assert exc_info.value.type == "ConfigError"

    @pytest.mark.asyncio
    async def test_completely_empty_config_raises_config_error(self) -> None:
        with pytest.raises(ApplicationError) as exc_info:
            await execute_script_activity({}, None)
        assert exc_info.value.type == "ConfigError"


class TestScriptNodesGate:
    """The APP_SCRIPT_NODES_ENABLED gate fires before any dispatch work."""

    @pytest.mark.asyncio
    async def test_disabled_raises_application_error(self) -> None:
        settings = get_settings()
        object.__setattr__(settings, "script_nodes_enabled", False)
        try:
            with pytest.raises(ApplicationError) as exc_info:
                await execute_script_activity({"language": "bash", "code": "echo hi"}, None)
            assert exc_info.value.non_retryable is True
            assert exc_info.value.type == "ScriptNodeDisabled"
        finally:
            object.__setattr__(settings, "script_nodes_enabled", True)

    @pytest.mark.asyncio
    async def test_disabled_error_message_is_opaque(self) -> None:
        """Error message must not reference the setting name."""
        settings = get_settings()
        object.__setattr__(settings, "script_nodes_enabled", False)
        try:
            with pytest.raises(ApplicationError) as exc_info:
                await execute_script_activity({"language": "bash", "code": "echo hi"}, None)
            message = str(exc_info.value)
            assert "APP_SCRIPT_NODES_ENABLED" not in message
            assert "script_nodes_enabled" not in message
            assert "setting" not in message.lower()
            assert "Script node execution is not enabled" in message
        finally:
            object.__setattr__(settings, "script_nodes_enabled", True)

    @pytest.mark.asyncio
    async def test_disabled_does_not_dispatch(self) -> None:
        """When disabled, no WorkItem is written."""
        settings = get_settings()
        object.__setattr__(settings, "script_nodes_enabled", False)
        try:
            with (
                patch.object(ep_dispatch_activity, "_dispatch_to_te", new_callable=AsyncMock) as mock_dispatch,
                pytest.raises(ApplicationError),
            ):
                await execute_script_activity({"language": "bash", "code": "echo hi"}, None)
            mock_dispatch.assert_not_called()
        finally:
            object.__setattr__(settings, "script_nodes_enabled", True)


class TestDispatchEnvelope:
    """A valid, enabled script dispatches an invocation envelope and suspends async."""

    @pytest.mark.asyncio
    async def test_enabled_dispatches_and_completes_async(self) -> None:
        with (
            patch.object(ep_dispatch_activity, "_dispatch_to_te", new_callable=AsyncMock) as mock_dispatch,
            patch(
                "syntara.workflows.workflow_engine.activities.ep.ep_dispatch_activity.activity.raise_complete_async",
                side_effect=RuntimeError("async"),
            ),
        ):
            with pytest.raises(RuntimeError, match="async"):
                await execute_script_activity({"language": "bash", "code": "echo hi"}, {"stdout": "stdout"})
            mock_dispatch.assert_awaited_once()


class TestBuildInvocation:
    """The invocation envelope shape matches the shared node-container contract."""

    @staticmethod
    def _settings() -> MagicMock:
        settings = MagicMock()
        settings.workflow_http_request_allowed_hosts = ["example.com"]
        settings.aap_poll_interval_seconds = 2.0
        return settings

    def test_strips_underscore_prefixed_engine_keys_from_inputs(self) -> None:
        input_config = {
            "language": "bash",
            "code": "echo hi",
            "_engine_timeout_seconds": 42,
            "_engine_max_output_bytes": 2048,
        }
        invocation = _build_invocation(input_config, self._settings())
        assert invocation["inputs"] == {"language": "bash", "code": "echo hi"}
        assert invocation["version"] == 1
        assert invocation["operation"] == "execute"
        assert invocation["credentials"] == {"resolved": {}}

    def test_engine_limits_override_defaults(self) -> None:
        input_config = {
            "language": "bash",
            "code": "echo hi",
            "_engine_timeout_seconds": 42,
            "_engine_max_output_bytes": 2048,
        }
        invocation = _build_invocation(input_config, self._settings())
        assert invocation["timeout_seconds"] == 42
        assert invocation["max_output_bytes"] == 2048

    def test_falls_back_to_node_timeout_then_default(self) -> None:
        with_timeout = _build_invocation({"language": "bash", "code": "x", "timeout": 99}, self._settings())
        assert with_timeout["timeout_seconds"] == 99
        no_timeout = _build_invocation({"language": "bash", "code": "x"}, self._settings())
        assert no_timeout["timeout_seconds"] == 300
        assert no_timeout["max_output_bytes"] == 1_048_576

    def test_carries_workflow_context_and_settings(self) -> None:
        invocation = _build_invocation({"language": "bash", "code": "x"}, self._settings())
        assert invocation["workflow_context"] == {"workflow_id": "wf-123", "activity_id": "act-456"}
        assert invocation["settings"]["workflow_http_request_allowed_hosts"] == ["example.com"]
        assert invocation["settings"]["aap_poll_interval_seconds"] == 2.0


class TestDispatchToTe:
    """_dispatch_to_te selects the image and writes the WorkItem payload."""

    @pytest.mark.asyncio
    async def test_missing_image_config_raises_config_error(self) -> None:
        settings = get_settings()
        original = settings.node_container_images
        object.__setattr__(settings, "node_container_images", {})
        try:
            with pytest.raises(ApplicationError) as exc_info:
                await ep_dispatch_activity._dispatch_to_te({"language": "bash", "code": "x"}, None)
            assert exc_info.value.type == "ConfigError"
        finally:
            object.__setattr__(settings, "node_container_images", original)

    @pytest.mark.asyncio
    async def test_writes_payload_with_invocation_image_and_output_config(self) -> None:
        settings = get_settings()
        original = settings.node_container_images
        object.__setattr__(settings, "node_container_images", {"script": "localhost:5001/syntara-node-script:dev"})

        captured: dict[str, Any] = {}

        class _FakeStore:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            async def __aenter__(self) -> "_FakeStore":
                return self

            async def __aexit__(self, *_: object) -> None:
                return None

            async def dispatch(
                self, *, activity_handle: str, work_correlation_id: object, payload: dict[str, Any]
            ) -> MagicMock:
                captured["activity_handle"] = activity_handle
                captured["payload"] = payload
                return MagicMock(id="wi-1")

        try:
            with patch.object(ep_dispatch_activity, "WorkStore", _FakeStore):
                await ep_dispatch_activity._dispatch_to_te({"language": "bash", "code": "x"}, {"stdout": "stdout"})
        finally:
            object.__setattr__(settings, "node_container_images", original)

        payload = captured["payload"]
        assert payload["image"] == "localhost:5001/syntara-node-script:dev"
        assert payload["output_config"] == {"stdout": "stdout"}
        assert payload["invocation"]["inputs"] == {"language": "bash", "code": "x"}
        # Task token is base64-encoded for JSON-safe transport.
        assert captured["activity_handle"] == "dG9rZW4tYnl0ZXM="
