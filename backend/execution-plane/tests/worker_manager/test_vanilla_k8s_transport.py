"""Unit tests for the vanilla-k8s transport.

``pod_body`` is a pure function (security-critical pod spec). ``run_pod`` owns
the single-pod lifecycle; it is exercised with the Kubernetes client, the
port-forward, the gRPC channel, and ``invoke`` all faked, so no cluster is
required. These verify the retry/cleanup contract, not real networking.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Self

import pytest
from execution_plane.worker_manager.vanilla_k8s import transport as transport_module
from execution_plane.worker_manager.vanilla_k8s.transport import TransportError, pod_body, run_pod
from kubernetes.client.exceptions import ApiException

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

_IDENTITY = "wi-xyz"


def _invocation(**overrides: object) -> dict[str, Any]:
    base: dict[str, Any] = {
        "version": 1,
        "operation": "execute",
        "inputs": {"code": "echo secret-hello"},
        "timeout_seconds": 30,
    }
    base.update(overrides)
    return base


def _target() -> dict[str, Any]:
    return {
        "base_url": "https://api.cluster.local:6443",
        "namespace": "execution",
        "token": "super-secret-token",
        "ca_certificate": None,
        "verify_ssl": True,
    }


class TestPodBody:
    """The pod spec is locked down and never carries secrets."""

    def test_hardened_security_context(self) -> None:
        body = pod_body("p", "img:1", _invocation(), startup=120, grace=30)
        spec = body["spec"]
        assert spec["restartPolicy"] == "Never"
        assert spec["automountServiceAccountToken"] is False
        assert spec["securityContext"]["runAsNonRoot"] is True
        assert spec["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
        container = spec["containers"][0]
        assert container["securityContext"]["allowPrivilegeEscalation"] is False
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]

    def test_active_deadline_sums_startup_timeout_and_grace(self) -> None:
        body = pod_body("p", "img:1", _invocation(timeout_seconds=30), startup=120, grace=30)
        assert body["spec"]["activeDeadlineSeconds"] == 180

    def test_image_is_set_on_the_container(self) -> None:
        body = pod_body("p", "registry/node:dev", _invocation(), startup=1, grace=1)
        assert body["spec"]["containers"][0]["image"] == "registry/node:dev"

    def test_no_invocation_data_or_secret_in_spec(self) -> None:
        body = pod_body("p", "img:1", _invocation(), startup=1, grace=1)
        serialized = json.dumps(body)
        assert "secret-hello" not in serialized
        assert "super-secret-token" not in serialized

    def test_tls_secret_absent_by_default(self) -> None:
        body = pod_body("p", "img:1", _invocation(), startup=1, grace=1)
        assert all(v["name"] != "agent-tls" for v in body["spec"]["volumes"])

    def test_tls_secret_mounts_read_only_when_provided(self) -> None:
        body = pod_body("p", "img:1", _invocation(), startup=1, grace=1, tls_secret="agent-cert")  # noqa: S106
        tls_volume = next(v for v in body["spec"]["volumes"] if v["name"] == "agent-tls")
        assert tls_volume["secret"]["secretName"] == "agent-cert"
        mount = next(m for m in body["spec"]["containers"][0]["volumeMounts"] if m["name"] == "agent-tls")
        assert mount["readOnly"] is True


class _FakeApiClient:
    def __init__(self, _config: object) -> None:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _FakeApi:
    """Kubernetes CoreV1Api double with configurable phase and error injection."""

    def __init__(self) -> None:
        self.phase = "Running"
        self.create_error: BaseException | None = None
        self.deleted: list[str] = []
        self.created = False

    def create_namespaced_pod(self, _namespace: str, _body: dict[str, Any], _request_timeout: int = 0) -> None:
        if self.create_error is not None:
            raise self.create_error
        self.created = True

    def read_namespaced_pod(self, _name: str, _namespace: str, _request_timeout: int = 0) -> object:
        return SimpleNamespace(status=SimpleNamespace(phase=self.phase))

    def connect_get_namespaced_pod_portforward(self, *_args: object, **_kwargs: object) -> None:
        return None

    def delete_namespaced_pod(
        self, name: str, _namespace: str, grace_period_seconds: int = 0, _request_timeout: int = 0
    ) -> None:
        self.deleted.append(name)


class _FakeConnection:
    def socket(self, _port: int) -> object:
        return object()

    def close(self) -> None:
        pass


class _FakeChannel:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def _install_happy_transport(
    monkeypatch: pytest.MonkeyPatch,
    api: _FakeApi,
    invoke_result: dict[str, Any],
    *,
    configuration: Callable[[], object] | None = None,
    api_client: type = _FakeApiClient,
) -> None:
    fake_client = SimpleNamespace(
        Configuration=configuration
        or (lambda: SimpleNamespace(host=None, api_key={}, api_key_prefix={}, verify_ssl=True, ssl_ca_cert=None)),
        ApiClient=api_client,
        CoreV1Api=lambda _api_client: api,
    )
    monkeypatch.setattr(transport_module, "client", fake_client)
    monkeypatch.setattr(transport_module, "portforward", lambda *_a, **_k: _FakeConnection())

    @contextmanager
    def _fake_forward_socket(_remote: object) -> Iterator[str]:
        yield "127.0.0.1:12345"

    monkeypatch.setattr(transport_module, "forward_socket", _fake_forward_socket)
    monkeypatch.setattr(
        "execution_plane.worker_manager.vanilla_k8s.transport.grpc.insecure_channel",
        lambda *_a, **_k: _FakeChannel(),
    )

    def _fake_invoke(_channel: object, _invocation: dict[str, Any], **_kwargs: object) -> dict[str, Any]:
        return invoke_result

    monkeypatch.setattr(transport_module, "invoke", _fake_invoke)


def _run(api: _FakeApi, **overrides: object) -> dict[str, Any]:
    import threading

    kwargs: dict[str, Any] = {
        "target": _target(),
        "image": "img:1",
        "invocation": _invocation(),
        "identity": _IDENTITY,
        "startup": 1,
        "grace": 1,
        "cancelled": threading.Event(),
        "progress": lambda _f: None,
    }
    kwargs.update(overrides)
    return run_pod(**kwargs)


class TestRunPod:
    """Pod lifecycle: happy path, readiness/cleanup, and failure classification."""

    def test_success_returns_invoke_result_and_reaps_pod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api = _FakeApi()
        _install_happy_transport(monkeypatch, api, {"result": {"StatusCode": 0}})
        result = _run(api)
        assert result == {"result": {"StatusCode": 0}}
        assert api.created is True
        assert len(api.deleted) == 1

    def test_oversize_invocation_is_rejected_before_any_pod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api = _FakeApi()
        _install_happy_transport(monkeypatch, api, {"result": {}})
        with pytest.raises(TransportError, match="exceeds transport limit"):
            _run(api, invocation=_invocation(inputs={"blob": "x" * 2_200_000}))
        assert api.created is False

    def test_pod_never_ready_is_retryable_and_reaps_pod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api = _FakeApi()
        api.phase = "Pending"
        _install_happy_transport(monkeypatch, api, {"result": {}})
        with pytest.raises(TransportError) as exc_info:
            _run(api, startup=0)
        assert exc_info.value.retryable is True
        assert len(api.deleted) == 1

    def test_pod_failed_phase_is_retryable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api = _FakeApi()
        api.phase = "Failed"
        _install_happy_transport(monkeypatch, api, {"result": {}})
        with pytest.raises(TransportError) as exc_info:
            _run(api)
        assert exc_info.value.retryable is True

    def test_cancelled_before_ready_is_terminal_and_reaps_pod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import threading

        api = _FakeApi()
        api.phase = "Pending"
        _install_happy_transport(monkeypatch, api, {"result": {}})
        cancelled = threading.Event()
        cancelled.set()
        with pytest.raises(TransportError) as exc_info:
            _run(api, cancelled=cancelled)
        assert exc_info.value.retryable is False
        assert len(api.deleted) == 1

    def test_transient_api_exception_on_create_is_retryable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api = _FakeApi()
        api.create_error = ApiException(status=503, reason="Service Unavailable")
        _install_happy_transport(monkeypatch, api, {"result": {}})
        with pytest.raises(TransportError) as exc_info:
            _run(api)
        assert exc_info.value.retryable is True
        # Pod creation failed, so there is nothing to reap.
        assert api.deleted == []

    def test_permanent_api_exception_on_create_is_terminal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api = _FakeApi()
        api.create_error = ApiException(status=500, reason="Internal Server Error")
        _install_happy_transport(monkeypatch, api, {"result": {}})
        with pytest.raises(TransportError) as exc_info:
            _run(api)
        assert exc_info.value.retryable is False

    def test_api_exception_message_hides_remote_detail(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api = _FakeApi()
        api.create_error = ApiException(status=500, reason="secret-cluster-detail")
        _install_happy_transport(monkeypatch, api, {"result": {}})
        with pytest.raises(TransportError) as exc_info:
            _run(api)
        assert "secret-cluster-detail" not in str(exc_info.value)

    def test_api_exception_message_surfaces_safe_status_code(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Regression: a 403 (e.g. a rejected/anonymous auth header) must be
        # distinguishable in the work-item result — the status code is safe to
        # surface even though the remote reason/body must stay hidden.
        api = _FakeApi()
        api.create_error = ApiException(status=403, reason="secret-cluster-detail")
        _install_happy_transport(monkeypatch, api, {"result": {}})
        with pytest.raises(TransportError) as exc_info:
            _run(api)
        assert "403" in str(exc_info.value)
        assert "secret-cluster-detail" not in str(exc_info.value)


class TestAuthConfiguration:
    """The bearer token must reach the API server as ``Authorization: Bearer <token>``.

    Regression for the kubernetes Python client v36 auth-scheme rename: its
    backward-compat shim reads the token from the legacy ``authorization`` key but
    resolves the prefix only under ``BearerToken``, so a prefix set under
    ``authorization`` alone is silently dropped and the API server treats the
    request as anonymous (kubernetes-client/python#2595). The happy-path lifecycle
    tests fake ``Configuration`` away, so this test drives the *real* one.
    """

    def test_bearer_prefix_is_applied_to_the_real_configuration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from kubernetes import client as real_client

        captured: dict[str, Any] = {}

        class _CapturingApiClient(_FakeApiClient):
            def __init__(self, config: object) -> None:
                captured["config"] = config
                super().__init__(config)

        api = _FakeApi()
        _install_happy_transport(
            monkeypatch,
            api,
            {"result": {}},
            configuration=real_client.Configuration,
            api_client=_CapturingApiClient,
        )
        _run(api)

        config = captured["config"]
        # `_target()` sets token="super-secret-token"; the emitted header must carry
        # the Bearer scheme, not the raw token.
        assert config.auth_settings()["BearerToken"]["value"] == "Bearer super-secret-token"
