"""Script activity — dispatches script execution to the Execution Plane worker.

This module defines the Temporal activity that handles Script nodes. It validates
the request, writes a WorkItem to the execution_plane schema, and suspends via
Temporal async completion. The Execution Plane worker picks up the WorkItem,
runs the script, and resumes the activity with the result.
"""

import base64
import uuid
from typing import Any

from execution_plane.work_store import WorkStore
from sqlalchemy.pool import NullPool
from temporalio import activity
from temporalio.exceptions import ApplicationError

from syntara.core.config.base import get_settings
from syntara.workflows.workflow_engine.activities.common import HEARTBEAT_STOP_MONITOR
from syntara.workflows.workflow_engine.models.workflow_definition import (
    ActivityName,
    ScriptExecutorParameters,
)

# Workflow node type dispatched by this activity. The Execution Plane worker is
# node-type agnostic; the image reference below is what makes the pod a "script" node.
_NODE_TYPE = "script"

# Fallback caps when the node config omits engine-injected limits.
_DEFAULT_TIMEOUT_SECONDS = 300
_DEFAULT_MAX_OUTPUT_BYTES = 1_048_576


def _build_invocation(input_config: dict[str, Any], settings: Any) -> dict[str, Any]:  # noqa: ANN401
    """Assemble the full node invocation envelope for the cold-start pod.

    Per the EP integration design, the AO activity (not the EP worker) owns
    envelope construction and image selection; the EP worker manager stays
    node-type agnostic and only creates the pod and maps its result. The shape
    here matches the synchronous node-container path so a single SDK node image
    serves both the sync (Temporal-activity) and async (EP) execution routes.

    Script nodes carry no resolved credentials, so ``credentials.resolved`` is
    empty; the ``settings`` block still ships the generic node settings the SDK
    expects.
    """
    return {
        "version": 1,
        "operation": "execute",
        # Underscore-prefixed keys are engine routing/limits, not node inputs.
        "inputs": {key: value for key, value in input_config.items() if not key.startswith("_")},
        "credentials": {"resolved": {}},
        "workflow_context": {
            "workflow_id": activity.info().workflow_id,
            "activity_id": activity.info().activity_id,
        },
        "settings": {
            "workflow_http_request_allowed_hosts": settings.workflow_http_request_allowed_hosts,
            "aap_poll_interval_seconds": settings.aap_poll_interval_seconds,
        },
        "timeout_seconds": int(
            input_config.get("_engine_timeout_seconds", input_config.get("timeout", _DEFAULT_TIMEOUT_SECONDS))
        ),
        "max_output_bytes": int(input_config.get("_engine_max_output_bytes", _DEFAULT_MAX_OUTPUT_BYTES)),
    }


async def _dispatch_to_te(
    input_config: dict[str, Any],
    output_config: dict[str, str] | None,
) -> None:
    """Write a WorkItem to the execution_plane schema for TE worker pickup.

    BOUNDARY CROSSING — see docs/execution-plane/integration.md.
    This function writes directly to the execution_plane DB schema instead of
    calling an HTTP API. When the EP worker becomes a standalone service, this
    becomes POST /api/execution_plane/v1/submit with the same payload.

    MVP SHORTCUT (secrets at rest): the invocation envelope — including node
    inputs — is persisted in ``work_items.payload`` in plaintext. Script nodes
    carry no credentials today, but node types that do (aap_*, agentic) must not
    reuse this path until payloads are encrypted or credentials are resolved by
    the EP worker at dispatch time. See followup in
    docs/execution-plane/integration.md.
    """
    settings = get_settings()

    image = settings.node_container_images.get(_NODE_TYPE)
    if not image:
        msg = f"No Execution Plane container image configured for node type '{_NODE_TYPE}'"
        raise ApplicationError(msg, type="ConfigError", non_retryable=True)

    task_token_bytes: bytes = activity.info().task_token
    task_token_b64 = base64.b64encode(task_token_bytes).decode("ascii")
    correlation_id_str = activity.info().workflow_id

    try:
        work_correlation_id = uuid.UUID(correlation_id_str) if correlation_id_str else uuid.uuid4()
    except ValueError:
        work_correlation_id = uuid.uuid4()

    payload = {
        "invocation": _build_invocation(input_config, settings),
        "image": image,
        "output_config": output_config,
    }

    async with WorkStore(settings.database_url.render_as_string(hide_password=False), poolclass=NullPool) as store:
        work_item = await store.dispatch(
            activity_handle=task_token_b64,
            work_correlation_id=work_correlation_id,
            payload=payload,
        )
    activity.logger.info("Dispatched work item to TE work_item_id=%s", work_item.id)


@activity.defn(name=ActivityName.SCRIPT)
async def execute_script_activity(
    input_config: dict[str, Any],
    output_config: dict[str, str] | None,
) -> dict[str, Any]:
    """Schedule a script for execution on the Execution Plane worker.

    SECURITY: Script nodes execute arbitrary user-supplied code (bash/Python)
    in the Execution Plane worker container. Unlike the main Temporal worker,
    the EP worker is a separate process/container boundary — but operators must
    still treat the EP worker host as a trust boundary. Any user with
    workflow:create + execution:run permissions can run arbitrary code in that
    environment. Enabling Script Nodes is not recommended for production
    deployments unless the EP worker is appropriately isolated and sandboxed.

    Validates the config eagerly (before writing the work item) so bad configs
    are rejected at submission time with a clear error rather than silently
    failing inside the EP worker. Actual execution happens asynchronously: this
    activity writes a WorkItem row and suspends via Temporal async completion;
    the EP worker executes the script and resumes the activity with the result.
    """
    activity.heartbeat({HEARTBEAT_STOP_MONITOR: True})

    if not get_settings().script_nodes_enabled:
        msg = "Script node execution is not enabled."
        raise ApplicationError(msg, type="ScriptNodeDisabled", non_retryable=True)

    # Validate config before writing the work item to catch bad input early.
    try:
        ScriptExecutorParameters.model_validate(input_config)
    except Exception:  # noqa: BLE001
        msg = "Script activity configuration validation failed"
        raise ApplicationError(msg, type="ConfigError", non_retryable=True) from None

    await _dispatch_to_te(input_config, output_config)
    activity.raise_complete_async()
