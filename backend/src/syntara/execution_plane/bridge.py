"""Deliver EP completion callbacks to the AO-owned async Temporal activity.

EP owns a durable completion outbox and retries delivery until AO returns a
2xx (see the EP service's ``event_delivery`` loop), so AO keeps no inbox or
dispatch state of its own. A completion is addressed purely by the identifiers
already stored on the activity: the work item UUID is ``ActivityExecution.id``,
from which AO resolves ``(temporal_workflow_id, temporal_activity_id)`` and
completes the async activity — the same path used for approval and agentic
callbacks in ``ExecutionService.handle_activity_callback``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog
from sqlmodel import col, select
from temporalio.exceptions import ApplicationError

from syntara.core.database.session import AsyncSessionLocal
from syntara.workflows.models.activity_execution import ActivityExecution
from syntara.workflows.models.execution import Execution

if TYPE_CHECKING:
    from uuid import UUID

    from syntara.workflows.workflow_engine.services.temporal_execution_service import (
        TemporalExecutionService,
    )

logger = structlog.stdlib.get_logger(__name__)


class CompletionBindingNotFoundError(LookupError):
    """Raised when an EP callback names a work item AO has no activity for."""


async def _resolve_temporal_identity(work_item_id: UUID) -> tuple[str, str]:
    """Return ``(temporal_workflow_id, temporal_activity_id)`` for a work item."""
    async with AsyncSessionLocal() as session:
        row = (
            await session.exec(
                select(Execution.temporal_workflow_id, ActivityExecution.temporal_activity_id)
                .join(Execution, col(ActivityExecution.execution_id) == col(Execution.id))
                .where(col(ActivityExecution.id) == work_item_id)
            )
        ).first()
    if row is None:
        raise CompletionBindingNotFoundError(work_item_id)
    return row[0], row[1]


def _failure_error(status: str, result: dict[str, Any]) -> ApplicationError:
    """Translate a non-completed EP status into a non-retryable activity failure."""
    if status == "cancelled":
        return ApplicationError(
            "Execution Plane work was cancelled before execution",
            result,
            type="ExecutionPlaneWorkCancelled",
            non_retryable=True,
        )
    if status == "reconciliation_required":
        return ApplicationError(
            "Execution Plane could not determine the work outcome",
            result,
            type="ExecutionPlaneReconciliationRequired",
            non_retryable=True,
        )
    error_message = str(result.get("error", "Execution Plane work failed"))
    error_type = str(result.get("error_type", "ScriptExecutionError"))
    return ApplicationError(error_message, result, type=error_type, non_retryable=True)


async def deliver_ep_completion(event: dict[str, Any], temporal_service: TemporalExecutionService) -> None:
    """Complete or fail the async activity addressed by an EP completion event.

    Idempotent: a re-delivered callback targets an already-resolved activity,
    which Temporal reports as not-found and the service treats as a no-op.
    """
    work_item_id: UUID = event["work_id"]
    temporal_workflow_id, temporal_activity_id = await _resolve_temporal_identity(work_item_id)

    status = event["status"]
    if status == "completed":
        await temporal_service.complete_async_activity(
            temporal_workflow_id=temporal_workflow_id,
            activity_id=temporal_activity_id,
            result=event["result"],
        )
    else:
        await temporal_service.fail_async_activity(
            temporal_workflow_id=temporal_workflow_id,
            activity_id=temporal_activity_id,
            error=_failure_error(status, event["result"]),
        )
    logger.info(
        "Delivered Execution Plane completion to Temporal",
        work_item_id=str(work_item_id),
        status=status,
    )
