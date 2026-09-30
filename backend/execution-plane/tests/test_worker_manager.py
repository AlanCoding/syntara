"""Unit tests for the cold-start Kubernetes worker manager.

These cover the pure result-mapping logic and the dispatch control flow
(payload validation, target routing, and retryable-vs-terminal transport
failures) without creating real pods — ``run_pod`` is patched.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pytest
from execution_plane.worker_manager.vanilla_k8s import manager as manager_module
from execution_plane.worker_manager.vanilla_k8s.manager import (
    NodeExecutionError,
    RetryableDispatchError,
    VanillaK8sWorkerManager,
    WorkItemPayloadError,
    _map_result,
    _select_output,
)
from execution_plane.worker_manager.vanilla_k8s.transport import TransportError

if TYPE_CHECKING:
    from execution_plane.config import EPSettings
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore


def _result_frame(status_code: int = 0, result_body: dict[str, Any] | None = None, **extra: object) -> dict[str, Any]:
    """Build a node result frame with a valid envelope by default."""
    result: dict[str, Any] = {"StatusCode": status_code, "Result": result_body}
    result.update(extra)
    return {"kind": "result", "result": result, "error": None}


# --------------------------------------------------------------------------- #
# _select_output
# --------------------------------------------------------------------------- #


class TestSelectOutput:
    """Field-selection output mapping."""

    def test_none_config_passes_raw_through(self) -> None:
        raw = {"stdout": "hi", "return_code": 0}
        assert _select_output(raw, None) == raw

    def test_selects_only_configured_keys(self) -> None:
        raw = {"stdout": "hi", "stderr": "", "return_code": 0}
        assert _select_output(raw, {"stdout": "stdout"}) == {"stdout": "hi"}

    def test_missing_keys_are_skipped(self) -> None:
        raw = {"stdout": "hi"}
        assert _select_output(raw, {"stdout": "stdout", "missing": "missing"}) == {"stdout": "hi"}


# --------------------------------------------------------------------------- #
# _map_result
# --------------------------------------------------------------------------- #


class TestMapResult:
    """Translation of the node result frame into an activity result."""

    def test_success_returns_output(self) -> None:
        frame = _result_frame(status_code=0, result_body={"stdout": "hi", "return_code": 0})
        assert _map_result(frame, None) == {"output": {"stdout": "hi", "return_code": 0}}

    def test_success_applies_output_config(self) -> None:
        frame = _result_frame(status_code=0, result_body={"stdout": "hi", "return_code": 0})
        assert _map_result(frame, {"stdout": "stdout"}) == {"output": {"stdout": "hi"}}

    def test_none_result_body_yields_empty_output(self) -> None:
        frame = _result_frame(status_code=0, result_body=None)
        assert _map_result(frame, None) == {"output": {}}

    def test_nonzero_status_raises_node_execution_error(self) -> None:
        frame = {
            "kind": "result",
            "result": {"StatusCode": 2, "Result": {"stdout": "partial"}, "ErrorMessage": "boom"},
            "error": {"type": "ScriptFailure", "retryable": False},
        }
        with pytest.raises(NodeExecutionError) as exc_info:
            _map_result(frame, None)
        assert exc_info.value.error_type == "ScriptFailure"
        assert exc_info.value.output == {"stdout": "partial"}
        assert "boom" in str(exc_info.value)

    def test_nonzero_status_without_error_type_defaults(self) -> None:
        frame = {"kind": "result", "result": {"StatusCode": 1, "Result": {}}, "error": None}
        with pytest.raises(NodeExecutionError) as exc_info:
            _map_result(frame, None)
        assert exc_info.value.error_type == "NodeExecutionError"

    def test_missing_status_code_raises_protocol_error(self) -> None:
        frame: dict[str, Any] = {"kind": "result", "result": {"Result": {}}, "error": None}
        with pytest.raises(NodeExecutionError) as exc_info:
            _map_result(frame, None)
        assert exc_info.value.error_type == "NodeProtocolError"

    def test_bool_status_code_rejected_as_protocol_error(self) -> None:
        # bool is an int subclass; the mapper uses exact type checking to reject it.
        frame = {"kind": "result", "result": {"StatusCode": True, "Result": {}}, "error": None}
        with pytest.raises(NodeExecutionError) as exc_info:
            _map_result(frame, None)
        assert exc_info.value.error_type == "NodeProtocolError"

    def test_out_of_range_status_code_raises_protocol_error(self) -> None:
        frame = _result_frame(status_code=256, result_body={})
        with pytest.raises(NodeExecutionError) as exc_info:
            _map_result(frame, None)
        assert exc_info.value.error_type == "NodeProtocolError"

    def test_non_dict_output_raises_protocol_error(self) -> None:
        frame = {"kind": "result", "result": {"StatusCode": 0, "Result": "not-a-dict"}, "error": None}
        with pytest.raises(NodeExecutionError) as exc_info:
            _map_result(frame, None)
        assert exc_info.value.error_type == "NodeProtocolError"

    def test_non_dict_frame_raises_protocol_error(self) -> None:
        with pytest.raises(NodeExecutionError) as exc_info:
            _map_result("nope", None)  # type: ignore[arg-type]
        assert exc_info.value.error_type == "NodeProtocolError"


# --------------------------------------------------------------------------- #
# VanillaK8sWorkerManager.dispatch
# --------------------------------------------------------------------------- #


def _target(*, enabled: bool = True) -> MagicMock:
    target = MagicMock()
    target.enabled = enabled
    target.endpoint = "https://api.cluster.local:6443"
    target.namespace = "execution"
    target.api_key = "secret-token"
    return target


def _settings() -> MagicMock:
    settings = MagicMock()
    settings.node_startup_seconds = 120
    settings.node_grace_seconds = 30
    settings.node_k8s_ca_certificate = None
    settings.node_k8s_verify_ssl = True
    return settings


def _work_item(*, target_id: uuid.UUID | None, payload: dict[str, Any] | None) -> MagicMock:
    item = MagicMock()
    item.id = uuid.uuid4()
    item.execution_target_id = target_id
    item.payload = payload
    return item


class _TargetStore:
    def __init__(self, target: MagicMock | None) -> None:
        self._target = target
        self.calls: list[dict[str, Any]] = []

    async def get(self, target_id: uuid.UUID, *, include_secret: bool = False) -> MagicMock | None:
        self.calls.append({"target_id": target_id, "include_secret": include_secret})
        return self._target


def _manager(store: _TargetStore, settings: MagicMock) -> VanillaK8sWorkerManager:
    """Build a manager, casting the fake store/settings to the real dependency types."""
    return VanillaK8sWorkerManager(cast("ExecutionTargetStore", store), cast("EPSettings", settings))


def _valid_payload() -> dict[str, Any]:
    return {
        "invocation": {"version": 1, "operation": "execute", "inputs": {"code": "echo hi"}},
        "image": "localhost:5001/syntara-node-script:dev",
        "output_config": {"stdout": "stdout"},
    }


class TestDispatch:
    """Dispatch control flow: routing, payload validation, transport outcomes."""

    @pytest.mark.asyncio
    async def test_no_target_id_is_retryable(self) -> None:
        mgr = _manager(_TargetStore(_target()), _settings())
        item = _work_item(target_id=None, payload=_valid_payload())
        with pytest.raises(RetryableDispatchError):
            await mgr.dispatch(item)

    @pytest.mark.asyncio
    async def test_missing_target_is_retryable(self) -> None:
        mgr = _manager(_TargetStore(None), _settings())
        item = _work_item(target_id=uuid.uuid4(), payload=_valid_payload())
        with pytest.raises(RetryableDispatchError):
            await mgr.dispatch(item)

    @pytest.mark.asyncio
    async def test_disabled_target_is_retryable(self) -> None:
        mgr = _manager(_TargetStore(_target(enabled=False)), _settings())
        item = _work_item(target_id=uuid.uuid4(), payload=_valid_payload())
        with pytest.raises(RetryableDispatchError):
            await mgr.dispatch(item)

    @pytest.mark.asyncio
    async def test_loads_target_with_secret(self) -> None:
        store = _TargetStore(_target())
        mgr = _manager(store, _settings())
        item = _work_item(target_id=uuid.uuid4(), payload={"invocation": {}, "image": ""})
        # Image empty → payload error, but target must have been fetched with the secret first.
        with pytest.raises(WorkItemPayloadError):
            await mgr.dispatch(item)
        assert store.calls[0]["include_secret"] is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"image": "img"},
            {"invocation": {}},
            {"invocation": "not-a-dict", "image": "img"},
            {"invocation": {}, "image": ""},
            {"invocation": {}, "image": 123},
        ],
    )
    async def test_malformed_payload_raises_payload_error(self, payload: dict[str, Any]) -> None:
        mgr = _manager(_TargetStore(_target()), _settings())
        item = _work_item(target_id=uuid.uuid4(), payload=payload)
        with pytest.raises(WorkItemPayloadError):
            await mgr.dispatch(item)

    @pytest.mark.asyncio
    async def test_success_returns_mapped_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_run_pod(**_: object) -> dict[str, Any]:
            return _result_frame(status_code=0, result_body={"stdout": "hi", "return_code": 0})

        monkeypatch.setattr(manager_module, "run_pod", fake_run_pod)
        mgr = _manager(_TargetStore(_target()), _settings())
        item = _work_item(target_id=uuid.uuid4(), payload=_valid_payload())
        assert await mgr.dispatch(item) == {"output": {"stdout": "hi"}}

    @pytest.mark.asyncio
    async def test_retryable_transport_error_becomes_retryable_dispatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_run_pod(**_: object) -> dict[str, Any]:
            msg = "cluster at capacity"
            raise TransportError(msg, retryable=True)

        monkeypatch.setattr(manager_module, "run_pod", fake_run_pod)
        mgr = _manager(_TargetStore(_target()), _settings())
        item = _work_item(target_id=uuid.uuid4(), payload=_valid_payload())
        with pytest.raises(RetryableDispatchError):
            await mgr.dispatch(item)

    @pytest.mark.asyncio
    async def test_terminal_transport_error_becomes_node_execution_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_run_pod(**_: object) -> dict[str, Any]:
            msg = "image pull backoff"
            raise TransportError(msg, retryable=False)

        monkeypatch.setattr(manager_module, "run_pod", fake_run_pod)
        mgr = _manager(_TargetStore(_target()), _settings())
        item = _work_item(target_id=uuid.uuid4(), payload=_valid_payload())
        with pytest.raises(NodeExecutionError) as exc_info:
            await mgr.dispatch(item)
        assert exc_info.value.error_type == "NodeTransportError"

    @pytest.mark.asyncio
    async def test_k8s_target_mapping_passed_to_run_pod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def fake_run_pod(**kwargs: object) -> dict[str, Any]:
            captured.update(kwargs)
            return _result_frame(status_code=0, result_body={})

        monkeypatch.setattr(manager_module, "run_pod", fake_run_pod)
        settings = _settings()
        settings.node_k8s_verify_ssl = False
        mgr = _manager(_TargetStore(_target()), settings)
        item = _work_item(target_id=uuid.uuid4(), payload=_valid_payload())
        await mgr.dispatch(item)

        assert captured["target"] == {
            "base_url": "https://api.cluster.local:6443",
            "namespace": "execution",
            "token": "secret-token",
            "ca_certificate": None,
            "verify_ssl": False,
        }
        assert captured["image"] == "localhost:5001/syntara-node-script:dev"
        assert captured["identity"] == str(item.id)
