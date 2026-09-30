"""Cold-start Kubernetes WorkerManager: one fresh pod per work item, over gRPC.

The manager is agnostic to the node type. The Temporal activity (AO side) selects
the container image and builds the full invocation envelope; this manager only
creates the pod, streams the single result over gRPC, and maps it back to a
Temporal-compatible activity result. No dependency on the main syntara package.
"""

from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING, Any

import structlog

from execution_plane.worker_manager.vanilla_k8s.transport import TransportError, run_pod

if TYPE_CHECKING:
    from execution_plane.config import EPSettings
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane.models.work_item import WorkItem

logger = structlog.stdlib.get_logger(__name__)

MAX_STATUS_CODE = 255


class RetryableDispatchError(Exception):
    """Transient transport/capacity failure — requeue the work item, do not fail it."""


class NodeExecutionError(Exception):
    """The node ran and returned a non-zero result — terminal for the activity."""

    def __init__(self, message: str, *, error_type: str, output: dict[str, Any] | None = None) -> None:
        """Carry the safe error classification and any partial output."""
        super().__init__(message)
        self.error_type = error_type
        self.output = output


class WorkItemPayloadError(Exception):
    """The work item payload is missing required dispatch fields — terminal."""


class VanillaK8sWorkerManager:
    """Dispatch a work item to a fresh Kubernetes pod and return its terminal result."""

    def __init__(self, target_store: ExecutionTargetStore, settings: EPSettings) -> None:
        """Bind the manager to the target store and worker settings."""
        self._target_store = target_store
        self._settings = settings

    async def dispatch(self, work_item: WorkItem) -> dict[str, Any]:
        """Create a pod for work_item, stream its result, and map it to an activity result.

        Returns ``{"output": ...}`` on success. Raises:
        - ``RetryableDispatchError`` when the pod could not run (requeue with backoff),
        - ``NodeExecutionError`` when the node ran but failed (fail the activity),
        - ``WorkItemPayloadError`` when the payload is malformed (fail the activity).
        """
        if work_item.execution_target_id is None:
            message = "Work item has no execution target assigned"
            raise RetryableDispatchError(message)
        # include_secret=True: the manager needs the API token to reach the cluster.
        target = await self._target_store.get(work_item.execution_target_id, include_secret=True)
        if target is None or not target.enabled:
            message = "Assigned execution target is unavailable"
            raise RetryableDispatchError(message)

        payload = work_item.payload or {}
        invocation = payload.get("invocation")
        image = payload.get("image")
        if not isinstance(invocation, dict) or not isinstance(image, str) or not image:
            message = "Work item payload is missing 'invocation' or 'image'"
            raise WorkItemPayloadError(message)
        output_config: dict[str, str] | None = payload.get("output_config")

        identity = str(work_item.id)
        # EP-driven cancellation is future work; the event is never set for now.
        cancelled = threading.Event()

        def on_progress(frame: dict[str, Any]) -> None:
            logger.debug("node progress", work_item_id=identity, node_event=frame.get("event"))

        try:
            frame = await asyncio.to_thread(
                run_pod,
                target=self._k8s_target(target),
                image=image,
                invocation=invocation,
                identity=identity,
                startup=self._settings.node_startup_seconds,
                grace=self._settings.node_grace_seconds,
                cancelled=cancelled,
                progress=on_progress,
            )
        except TransportError as exc:
            if exc.retryable:
                raise RetryableDispatchError(str(exc)) from None
            # A non-retryable transport failure is terminal for this activity.
            raise NodeExecutionError(str(exc), error_type="NodeTransportError") from None

        return _map_result(frame, output_config)

    def _k8s_target(self, target: ExecutionTarget) -> dict[str, Any]:
        """Map an ExecutionTarget onto the connection dict run_pod expects.

        This is the single place that reads target topology fields. Michael is
        moving ``namespace`` (and node selectors/tolerations) into a backend-specific
        metadata block with a K8s/RHEL discriminator; when that lands, only this
        method changes.
        """
        return {
            "base_url": target.endpoint,
            "namespace": target.namespace,
            "token": target.api_key,
            "ca_certificate": self._settings.node_k8s_ca_certificate or None,
            "verify_ssl": self._settings.node_k8s_verify_ssl,
        }


def _map_result(frame: dict[str, Any], output_config: dict[str, str] | None) -> dict[str, Any]:
    """Translate the SDK node result frame into a Temporal activity result dict.

    Output mapping is *simple field selection* only. Template-expression output
    mapping (e.g. ``"${result.stdout}"``) requires NamespaceResolver from the
    syntara package, which the execution plane must not import — deferred to
    AAP-93073, matching the existing script-node limitation.
    """
    result = frame.get("result") if isinstance(frame, dict) else None
    if not isinstance(result, dict) or type(result.get("StatusCode")) is not int:
        message = "Node returned an invalid result"
        raise NodeExecutionError(message, error_type="NodeProtocolError")
    status_code = result["StatusCode"]
    if not 0 <= status_code <= MAX_STATUS_CODE:
        message = "Node returned an out-of-range status code"
        raise NodeExecutionError(message, error_type="NodeProtocolError")

    raw = result.get("Result")
    if raw is not None and not isinstance(raw, dict):
        message = "Node returned invalid output"
        raise NodeExecutionError(message, error_type="NodeProtocolError")
    output = _select_output(raw or {}, output_config)

    if status_code != 0:
        failure = frame.get("error") or {}
        message = result.get("ErrorMessage") or result.get("StatusMessage") or "Node execution failed"
        raise NodeExecutionError(message, error_type=failure.get("type") or "NodeExecutionError", output=output)
    return {"output": output}


def _select_output(raw: dict[str, Any], output_config: dict[str, str] | None) -> dict[str, Any]:
    """Select output fields by name, mirroring the current script-node behaviour."""
    if output_config is None:
        return raw
    return {key: raw[key] for key in output_config if key in raw}
