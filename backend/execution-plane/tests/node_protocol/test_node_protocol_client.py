"""Unit tests for the vendored node-protocol gRPC client (``invoke``).

``invoke`` is synchronous (the worker manager runs it in a thread), so these
drive it with a fake ``NodeServiceStub`` and real protobuf events built by the
codec. No cluster, channel, or port-forward is involved.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any, cast

import grpc
import pytest
from execution_plane.node_protocol.client import NodeRpcError, invoke
from execution_plane.node_protocol.codec import encode_event

_IDENTITY = "wi-abc"

# ``invoke`` type-checks its first argument as a ``grpc.Channel``; the fake stub is
# installed over the wire so the channel object is never touched at runtime.
_CHANNEL = cast("grpc.Channel", object())


def _invocation(**overrides: object) -> dict[str, Any]:
    base: dict[str, Any] = {
        "version": 1,
        "operation": "execute",
        "inputs": {"code": "echo hi"},
        "timeout_seconds": 1,
    }
    base.update(overrides)
    return base


def _progress_event(event: str = "log") -> object:
    return encode_event({"kind": "progress", "event": event, "data": {"line": "x"}}, _IDENTITY)


def _result_event(status_code: int = 0) -> object:
    frame = {
        "kind": "result",
        "result": {"Result": {"stdout": "hi"}, "StatusCode": status_code, "StatusMessage": "", "ErrorMessage": ""},
        "error": None,
    }
    return encode_event(frame, _IDENTITY)


class _FakeRpcError(grpc.RpcError):
    """A grpc.RpcError with a code, for exercising the error paths."""

    def __init__(self, code: grpc.StatusCode = grpc.StatusCode.UNAVAILABLE) -> None:
        super().__init__()
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


class _FakeCall:
    """An iterable Execute call; may yield events or raise on iteration."""

    def __init__(self, events: list[object] | None = None, *, raise_on_iter: BaseException | None = None) -> None:
        self._events = events or []
        self._raise = raise_on_iter
        self.cancelled = False

    def __iter__(self) -> Any:  # noqa: ANN401 - iterator of protobuf events
        if self._raise is not None:
            raise self._raise
        yield from self._events

    def cancel(self) -> None:
        self.cancelled = True


class _FakeStub:
    """Configurable NodeServiceStub double."""

    def __init__(
        self,
        *,
        health: object = None,
        health_error: BaseException | None = None,
        call: _FakeCall | None = None,
    ) -> None:
        self._health = health if health is not None else SimpleNamespace(ready=True, protocol_version=1)
        self._health_error = health_error
        self._call = call or _FakeCall([_result_event()])

    def Health(self, _request: object, timeout: float | None = None) -> object:  # noqa: N802 - gRPC method name
        if self._health_error is not None:
            raise self._health_error
        return self._health

    def Execute(self, _request: object, timeout: float | None = None) -> _FakeCall:  # noqa: N802 - gRPC method name
        return self._call

    def Cancel(self, _request: object, timeout: float | None = None) -> object:  # noqa: N802 - gRPC method name
        return SimpleNamespace(accepted=True)


def _install_stub(monkeypatch: pytest.MonkeyPatch, stub: _FakeStub) -> None:
    monkeypatch.setattr("execution_plane.node_protocol.client.rpc.NodeServiceStub", lambda _channel: stub)


class TestInvoke:
    """The gRPC invocation contract: health gate, single result, error mapping."""

    def test_success_returns_result_and_reports_progress(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub(call=_FakeCall([_progress_event(), _result_event()])))
        progress: list[dict[str, Any]] = []
        result = invoke(
            _CHANNEL, _invocation(), identity=_IDENTITY, progress=progress.append, cancelled=threading.Event()
        )
        assert result["kind"] == "result"
        assert result["result"]["Result"] == {"stdout": "hi"}
        assert [p["kind"] for p in progress] == ["progress"]

    def test_events_after_result_are_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub(call=_FakeCall([_result_event(), _progress_event()])))
        with pytest.raises(NodeRpcError, match="after its terminal result"):
            invoke(_CHANNEL, _invocation(), identity=_IDENTITY, progress=lambda _f: None, cancelled=threading.Event())

    def test_stream_without_result_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub(call=_FakeCall([_progress_event()])))
        with pytest.raises(NodeRpcError, match="without a result"):
            invoke(_CHANNEL, _invocation(), identity=_IDENTITY, progress=lambda _f: None, cancelled=threading.Event())

    def test_incompatible_protocol_version_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub(health=SimpleNamespace(ready=True, protocol_version=2)))
        with pytest.raises(NodeRpcError, match="unavailable or incompatible"):
            invoke(_CHANNEL, _invocation(), identity=_IDENTITY, progress=lambda _f: None, cancelled=threading.Event())

    def test_health_never_ready_is_retryable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub(health_error=_FakeRpcError()))
        with pytest.raises(NodeRpcError) as exc_info:
            invoke(
                _CHANNEL,
                _invocation(),
                identity=_IDENTITY,
                progress=lambda _f: None,
                cancelled=threading.Event(),
                startup=0.0,
            )
        assert exc_info.value.retryable is True

    def test_rpc_error_during_stream_hides_remote_text(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub(call=_FakeCall(raise_on_iter=_FakeRpcError(grpc.StatusCode.INTERNAL))))
        with pytest.raises(NodeRpcError) as exc_info:
            invoke(_CHANNEL, _invocation(), identity=_IDENTITY, progress=lambda _f: None, cancelled=threading.Event())
        # Only the code name is surfaced, never remote diagnostic text.
        assert "INTERNAL" in str(exc_info.value)

    def test_oversize_invocation_is_rejected_before_submission(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub())
        oversize = _invocation(inputs={"blob": "x" * 2_200_000})
        with pytest.raises(NodeRpcError, match="exceeds transport limit"):
            invoke(_CHANNEL, oversize, identity=_IDENTITY, progress=lambda _f: None, cancelled=threading.Event())

    def test_cancel_before_submission_is_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_stub(monkeypatch, _FakeStub())
        cancelled = threading.Event()
        cancelled.set()
        with pytest.raises(NodeRpcError, match="cancelled before submission"):
            invoke(_CHANNEL, _invocation(), identity=_IDENTITY, progress=lambda _f: None, cancelled=cancelled)
