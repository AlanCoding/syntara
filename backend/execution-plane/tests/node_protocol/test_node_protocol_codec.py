"""Unit tests for the vendored node-protocol codec.

The codec is pure (protobuf <-> internal JSON envelope), so these are
round-trip and validation tests with no cluster or gRPC channel involved.
"""

from __future__ import annotations

from typing import Any

import pytest
from execution_plane.node_protocol import node_pb2 as pb
from execution_plane.node_protocol.codec import (
    MAX_INVOCATION_ID_LENGTH,
    decode_event,
    decode_object,
    decode_request,
    encode_event,
    encode_json,
    encode_request,
)

_IDENTITY = "wi-123"


def _invocation(**overrides: object) -> dict[str, Any]:
    base: dict[str, Any] = {
        "version": 1,
        "operation": "execute",
        "inputs": {"code": "echo hi", "count": 3},
        "credentials": {"resolved": {}},
        "workflow_context": {"workflow_id": "wf-1", "activity_id": "act-1"},
        "settings": {"aap_poll_interval_seconds": 2.0},
        "timeout_seconds": 42,
        "max_output_bytes": 2048,
    }
    base.update(overrides)
    return base


class TestEncodeJson:
    """encode_json preserves JSON scalar fidelity."""

    def test_preserves_integers_and_null(self) -> None:
        assert encode_json({"n": 3, "empty": None}) == b'{"n":3,"empty":null}'

    def test_rejects_nan(self) -> None:
        with pytest.raises(ValueError, match="Out of range"):
            encode_json(float("nan"))


class TestDecodeObject:
    """decode_object requires a JSON object."""

    def test_empty_bytes_yields_empty_dict(self) -> None:
        assert decode_object(b"") == {}

    def test_object_is_returned(self) -> None:
        assert decode_object(b'{"a":1}') == {"a": 1}

    def test_non_object_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Expected a JSON object"):
            decode_object(b"[1, 2]")


class TestRequestRoundTrip:
    """encode_request / decode_request preserve the invocation envelope."""

    def test_execute_round_trip(self) -> None:
        decoded = decode_request(encode_request(_invocation(), _IDENTITY))
        assert decoded["operation"] == "execute"
        assert decoded["inputs"] == {"code": "echo hi", "count": 3}
        assert decoded["credentials"] == {"resolved": {}}
        assert decoded["workflow_context"] == {"workflow_id": "wf-1", "activity_id": "act-1"}
        assert decoded["settings"] == {"aap_poll_interval_seconds": 2.0}
        assert decoded["timeout_seconds"] == 42
        assert decoded["max_output_bytes"] == 2048

    def test_cancel_operation_round_trips(self) -> None:
        request = encode_request(_invocation(operation="cancel"), _IDENTITY)
        assert request.operation == pb.CANCEL_EXTERNAL
        assert decode_request(request)["operation"] == "cancel"

    def test_unsupported_operation_rejected_on_encode(self) -> None:
        with pytest.raises(ValueError, match="Unsupported node operation"):
            encode_request(_invocation(operation="destroy"), _IDENTITY)

    def test_defaults_applied_for_missing_fields(self) -> None:
        request = encode_request({"operation": "execute"}, _IDENTITY)
        decoded = decode_request(request)
        assert decoded["inputs"] == {}
        assert decoded["timeout_seconds"] == 300
        assert decoded["max_output_bytes"] == 1_048_576

    def test_wrong_protocol_version_rejected_on_decode(self) -> None:
        request = encode_request(_invocation(version=2), _IDENTITY)
        with pytest.raises(ValueError, match="Invalid invocation envelope"):
            decode_request(request)

    def test_empty_invocation_id_rejected_on_decode(self) -> None:
        with pytest.raises(ValueError, match="Invalid invocation envelope"):
            decode_request(encode_request(_invocation(), ""))

    def test_overlong_invocation_id_rejected_on_decode(self) -> None:
        request = encode_request(_invocation(), "x" * (MAX_INVOCATION_ID_LENGTH + 1))
        with pytest.raises(ValueError, match="Invalid invocation envelope"):
            decode_request(request)


class TestEventRoundTrip:
    """encode_event / decode_event preserve progress and terminal-result frames."""

    def test_progress_round_trip(self) -> None:
        frame = {"kind": "progress", "event": "log", "data": {"line": "working"}}
        decoded = decode_event(encode_event(frame, _IDENTITY), _IDENTITY)
        assert decoded == {"version": 1, "kind": "progress", "event": "log", "data": {"line": "working"}}

    def test_successful_result_round_trip(self) -> None:
        frame = {
            "kind": "result",
            "result": {"Result": {"stdout": "hi"}, "StatusCode": 0, "StatusMessage": "", "ErrorMessage": ""},
            "error": None,
        }
        decoded = decode_event(encode_event(frame, _IDENTITY), _IDENTITY)
        assert decoded["kind"] == "result"
        assert decoded["result"]["Result"] == {"stdout": "hi"}
        assert decoded["result"]["StatusCode"] == 0
        assert decoded["error"] is None

    def test_failed_result_carries_error_classification(self) -> None:
        frame = {
            "kind": "result",
            "result": {"Result": None, "StatusCode": 2, "StatusMessage": "", "ErrorMessage": "boom"},
            "error": {"type": "ScriptFailure", "retryable": True},
        }
        decoded = decode_event(encode_event(frame, _IDENTITY), _IDENTITY)
        assert decoded["result"]["Result"] is None
        assert decoded["result"]["StatusCode"] == 2
        assert decoded["result"]["ErrorMessage"] == "boom"
        assert decoded["error"] == {"type": "ScriptFailure", "retryable": True}

    def test_wrong_invocation_id_rejected(self) -> None:
        event = encode_event({"kind": "progress", "event": "log", "data": {}}, _IDENTITY)
        with pytest.raises(ValueError, match="wrong invocation ID"):
            decode_event(event, "other-id")
