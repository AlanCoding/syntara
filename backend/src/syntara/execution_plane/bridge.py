"""AO-owned persistence and Temporal completion bridge for EP events."""

from __future__ import annotations

import asyncio
import base64
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any, NoReturn
from uuid import UUID

import structlog
from sqlalchemy.exc import IntegrityError
from sqlmodel import col, select
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError

from syntara.core.config.base import get_encryption_key
from syntara.core.database.session import AsyncSessionLocal
from syntara.core.lib.encryption import SecretEncryptor, key_from_string
from syntara.execution_plane.client import (
    ExecutionPlaneError,
    ExecutionPlaneHttpClient,
)
from syntara.execution_plane.models import ExecutionPlaneCompletionInbox
from syntara.workflows.models.activity_execution import ActivityExecution
from syntara.workflows.models.execution import Execution

if TYPE_CHECKING:
    from temporalio.client import Client

logger = structlog.stdlib.get_logger(__name__)

AO_EP_CLIENT_ID = "syntara-orchestration"
CALLBACK_POLL_SECONDS = 2
CALLBACK_LEASE_SECONDS = 60
MISSING_EVENT_RECONCILE_SECONDS = 15
UNCERTAIN_RECONCILIATION_DELAY_SECONDS = 86_400
CANCELLATION_RETRY_MAX_SECONDS = 300


class CompletionConflictReason(StrEnum):
    """Stable explanations for contradictory dispatch and result records."""

    CALLBACK_SCOPE = "callback scope does not match AO's recorded dispatch"
    CALLBACK_WORK_ID = "callback work ID does not match AO's accepted dispatch"
    EVENT_ID_REUSED = "event ID was reused with different completion data"
    REQUEST_REVISION_REUSED = "request revision was reused with contradictory result data"
    CALLBACK_CONFLICT = "conflicting callback event already exists"


class CompletionEventConflictError(ValueError):
    """Raised when a producer submits contradictory data for an event or request."""

    def __init__(self, reason: CompletionConflictReason) -> None:
        """Expose a stable safe conflict reason to the callback route."""
        super().__init__(reason.value)


class CompletionBindingNotFoundError(LookupError):
    """Raised when an EP callback has no matching AO dispatch binding."""


class CompletionBindingNotReadyError(RuntimeError):
    """Raised while the dispatch activity has not finished its async handoff."""

    def __init__(self) -> None:
        """Describe the callback delivery race before the async handoff settles."""
        super().__init__("AO submission acknowledgement or async handoff is not ready")


class CompletionAlreadyReturnedSynchronouslyError(RuntimeError):
    """Raised when the activity returned the terminal result before callback delivery."""


def _raise_completion_conflict(reason: CompletionConflictReason) -> NoReturn:
    """Raise a dispatch conflict from the shared exception boundary."""
    raise CompletionEventConflictError(reason)


def _encryptor() -> SecretEncryptor:
    key = key_from_string(get_encryption_key().get_secret_value())
    return SecretEncryptor(key)


async def request_ep_cancellation_for_workflow(temporal_workflow_id: str) -> int:
    """Persist cancellation intent for accepted and still-submitting EP requests."""
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        execution_subq = select(Execution.id).where(Execution.temporal_workflow_id == temporal_workflow_id).subquery()
        result = await session.exec(
            select(ActivityExecution)
            .where(ActivityExecution.execution_id.in_(execution_subq))
            .where(col(ActivityExecution.ep_status).in_(["submitting", "handoff_pending"]))
            .where(col(ActivityExecution.ep_cancel_delivered_at).is_(None))
            .with_for_update(skip_locked=True)
        )
        rows = list(result.all())
        for ae in rows:
            if ae.ep_cancel_requested_at is None:
                ae.ep_cancel_requested_at = now
                ae.ep_cancel_next_attempt_at = now
                ae.ep_cancel_lease_expires_at = None
        await session.commit()
    return len(rows)


async def persist_dispatch_binding(
    *,
    work_item_id: UUID,
    project_id: UUID,
    execution_id: UUID,
    temporal_activity_id: str,
    activity_attempt: int,
    task_token: bytes,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Persist AO's first encrypted request and reuse it on every activity retry."""
    enc_key = str(work_item_id)
    encryptor = _encryptor()
    token_ciphertext = encryptor.encrypt_field(
        base64.b64encode(task_token).decode("ascii"), enc_key, "temporal_task_token"
    )
    payload_ciphertext = encryptor.encrypt_field(payload, enc_key, "execution_request")
    async with AsyncSessionLocal() as session:
        try:
            ae = (
                await session.exec(
                    select(ActivityExecution).where(ActivityExecution.id == work_item_id).with_for_update()
                )
            ).first()
            if ae is None:
                msg = f"ActivityExecution not found for work_item_id={work_item_id}"
                raise CompletionBindingNotFoundError(msg)

            if ae.ep_status is None:
                # First dispatch attempt — freeze the payload
                frozen_payload = payload
                ae.ep_task_token_ciphertext = token_ciphertext
                ae.ep_payload_ciphertext = payload_ciphertext
                ae.ep_activity_attempt = activity_attempt
                ae.ep_status = "submitting"
            else:
                # Retry — reuse frozen payload, update token + attempt
                ae.ep_task_token_ciphertext = token_ciphertext
                ae.ep_activity_attempt = activity_attempt
                ae.ep_status = "submitting"
                frozen_payload = encryptor.decrypt_field(
                    ae.ep_payload_ciphertext,
                    enc_key,
                    "execution_request",
                )
                if not isinstance(frozen_payload, dict):
                    msg = "Stored payload ciphertext did not decrypt to a dict"
                    raise RuntimeError(msg)
                # Reset any unprocessed inbox events so they retry with the new token
                inbox_result = await session.exec(
                    select(ExecutionPlaneCompletionInbox).where(
                        ExecutionPlaneCompletionInbox.work_item_id == work_item_id
                    )
                )
                now = datetime.now(UTC)
                for event in inbox_result.all():
                    event.processed_at = None
                    event.lease_expires_at = None
                    event.next_attempt_at = now

            await session.commit()
            return frozen_payload
        except Exception:
            await session.rollback()
            raise


async def mark_dispatch_accepted(work_item_id: UUID, *, terminal: bool) -> None:
    """Record EP acceptance without keeping AO's database transaction open over HTTP."""
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        ae = (
            await session.exec(select(ActivityExecution).where(ActivityExecution.id == work_item_id).with_for_update())
        ).first()
        if ae is None:
            raise CompletionBindingNotFoundError(work_item_id)
        ae.ep_status = "completed_synchronously" if terminal else "handoff_pending"
        ae.updated_at = now
        inbox_result = await session.exec(
            select(ExecutionPlaneCompletionInbox).where(ExecutionPlaneCompletionInbox.work_item_id == work_item_id)
        )
        for event in inbox_result.all():
            if terminal:
                event.processed_at = now
                event.lease_expires_at = None
            else:
                event.next_attempt_at = now + timedelta(seconds=2)
                event.lease_expires_at = None
        await session.commit()


def _same_event(row: ExecutionPlaneCompletionInbox, event: dict[str, Any]) -> bool:
    return bool(
        row.client_id == event["client_id"]
        and row.project_id == event["project_id"]
        and row.work_item_id == event["work_id"]
        and row.state_revision == event["state_revision"]
        and row.status == event["status"]
        and row.result == event["result"]
        and row.completed_at == event["completed_at"]
    )


async def persist_completion_event(event: dict[str, Any]) -> None:
    """Validate the dispatch binding and persist a deduplicated callback before ACK."""
    work_item_id: UUID = event["work_id"]
    async with AsyncSessionLocal() as session:
        ae = (await session.exec(select(ActivityExecution).where(ActivityExecution.id == work_item_id))).first()
        if ae is None:
            raise CompletionBindingNotFoundError(work_item_id)
        if ae.project_id != event["project_id"]:
            raise CompletionEventConflictError(CompletionConflictReason.CALLBACK_SCOPE)

        existing = await session.get(ExecutionPlaneCompletionInbox, event["event_id"])
        if existing is not None:
            if not _same_event(existing, event):
                raise CompletionEventConflictError(CompletionConflictReason.EVENT_ID_REUSED)
            return

        prior_revision = (
            await session.exec(
                select(ExecutionPlaneCompletionInbox)
                .where(ExecutionPlaneCompletionInbox.client_id == event["client_id"])
                .where(ExecutionPlaneCompletionInbox.project_id == event["project_id"])
                .where(ExecutionPlaneCompletionInbox.work_item_id == work_item_id)
                .where(ExecutionPlaneCompletionInbox.state_revision == event["state_revision"])
            )
        ).first()
        if prior_revision is not None:
            if not _same_event(prior_revision, event):
                raise CompletionEventConflictError(CompletionConflictReason.REQUEST_REVISION_REUSED)
            return

        session.add(
            ExecutionPlaneCompletionInbox(
                event_id=event["event_id"],
                client_id=event["client_id"],
                project_id=event["project_id"],
                work_item_id=work_item_id,
                state_revision=event["state_revision"],
                status=event["status"],
                result=event["result"],
                completed_at=event["completed_at"],
                received_at=datetime.now(UTC),
                next_attempt_at=(
                    datetime.now(UTC) + timedelta(seconds=2)
                    if ae.ep_status in {"submitting", "handoff_pending"}
                    else datetime.now(UTC)
                ),
                processed_at=datetime.now(UTC) if ae.ep_status == "completed_synchronously" else None,
            )
        )
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            duplicate = await session.get(ExecutionPlaneCompletionInbox, event["event_id"])
            if duplicate is None or not _same_event(duplicate, event):
                raise CompletionEventConflictError(CompletionConflictReason.CALLBACK_CONFLICT) from None


async def _claim_due_events() -> list[ExecutionPlaneCompletionInbox]:
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        result = await session.exec(
            select(ExecutionPlaneCompletionInbox)
            .where(col(ExecutionPlaneCompletionInbox.processed_at).is_(None))
            .where(col(ExecutionPlaneCompletionInbox.next_attempt_at) <= now)
            .where(
                (col(ExecutionPlaneCompletionInbox.lease_expires_at).is_(None))
                | (col(ExecutionPlaneCompletionInbox.lease_expires_at) <= now)
            )
            .order_by(col(ExecutionPlaneCompletionInbox.received_at))
            .limit(50)
            .with_for_update(skip_locked=True)
        )
        rows = list(result.all())
        for row in rows:
            row.lease_expires_at = now + timedelta(seconds=CALLBACK_LEASE_SECONDS)
            row.attempts += 1
        await session.commit()
        return rows


async def _claim_bindings_for_status_check() -> list[tuple[UUID, UUID]]:
    """Throttle status reconciliation for accepted requests with no callback event."""
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        result = await session.exec(
            select(ActivityExecution)
            .where(col(ActivityExecution.ep_status) == "handoff_pending")
            .where(
                (col(ActivityExecution.ep_last_status_check_at).is_(None))
                | (
                    col(ActivityExecution.ep_last_status_check_at)
                    < now - timedelta(seconds=MISSING_EVENT_RECONCILE_SECONDS)
                )
            )
            .order_by(col(ActivityExecution.updated_at))
            .limit(25)
            .with_for_update(skip_locked=True)
        )
        rows = list(result.all())
        for ae in rows:
            ae.ep_last_status_check_at = now
        await session.commit()
        return [(ae.id, ae.project_id) for ae in rows]


async def _reconcile_missing_events() -> None:
    """Recover terminal EP results when the durable callback delivery is unavailable."""
    bindings = await _claim_bindings_for_status_check()
    if not bindings:
        return
    async with ExecutionPlaneHttpClient() as ep_client:
        for work_item_id, project_id in bindings:
            try:
                state = await ep_client.get_work_item(project_id=project_id, work_item_id=work_item_id)
            except ExecutionPlaneError as exc:
                logger.warning(
                    "EP result reconciliation could not read accepted work",
                    work_item_id=str(work_item_id),
                    error_type=type(exc).__name__,
                )
                continue
            if state.get("status") not in {"completed", "failed", "cancelled", "reconciliation_required"}:
                continue
            event_id = state.get("completion_event_id")
            state_revision = state.get("state_revision")
            completed_at = state.get("completed_at")
            if not event_id or state_revision is None or not completed_at:
                continue
            completed = datetime.fromisoformat(str(completed_at))
            event = {
                "event_id": UUID(str(event_id)),
                "client_id": AO_EP_CLIENT_ID,
                "project_id": project_id,
                "work_id": work_item_id,
                "state_revision": int(state_revision),
                "status": str(state["status"]),
                "result": state.get("result") or {},
                "completed_at": completed,
            }
            await persist_completion_event(event)
            logger.info("Recovered missing EP completion callback from status API", work_item_id=str(work_item_id))


async def _claim_due_cancellations() -> list[tuple[UUID, UUID, int]]:
    """Lease pending AO-to-EP cancellation outbox rows for one delivery attempt."""
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        result = await session.exec(
            select(ActivityExecution)
            .where(col(ActivityExecution.ep_cancel_requested_at).is_not(None))
            .where(col(ActivityExecution.ep_cancel_delivered_at).is_(None))
            .where(col(ActivityExecution.ep_cancel_next_attempt_at) <= now)
            .where(
                (col(ActivityExecution.ep_cancel_lease_expires_at).is_(None))
                | (col(ActivityExecution.ep_cancel_lease_expires_at) <= now)
            )
            .order_by(col(ActivityExecution.ep_cancel_requested_at))
            .limit(50)
            .with_for_update(skip_locked=True)
        )
        rows = list(result.all())
        for ae in rows:
            ae.ep_cancel_lease_expires_at = now + timedelta(seconds=CALLBACK_LEASE_SECONDS)
            ae.ep_cancel_attempts += 1
        await session.commit()
        return [(ae.id, ae.project_id, ae.ep_cancel_attempts) for ae in rows]


async def _record_cancellation_attempt(
    work_item_id: UUID,
    *,
    delivered: bool,
    error: str | None = None,
    attempts: int,
) -> None:
    """Acknowledge EP cancellation intent or schedule a bounded-backoff retry."""
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        ae = await session.get(ActivityExecution, work_item_id, with_for_update=True)
        if ae is None:
            return
        ae.ep_cancel_lease_expires_at = None
        ae.ep_cancel_last_error = None if delivered else (error or "cancellation delivery failed")[:1000]
        if delivered:
            ae.ep_cancel_delivered_at = now
        else:
            delay = min(2 ** min(attempts, 8), CANCELLATION_RETRY_MAX_SECONDS)
            ae.ep_cancel_next_attempt_at = now + timedelta(seconds=delay)
        await session.commit()


async def _deliver_requested_cancellations() -> None:
    """Deliver persisted cancellation intent, retrying until EP acknowledges it."""
    cancellations = await _claim_due_cancellations()
    if not cancellations:
        return
    async with ExecutionPlaneHttpClient() as ep_client:
        for work_item_id, project_id, attempts in cancellations:
            try:
                state = await ep_client.cancel_work_item(
                    project_id=project_id,
                    work_item_id=work_item_id,
                )
                status_value = str(state.get("status", ""))[:80]
            except ExecutionPlaneError as exc:
                await _record_cancellation_attempt(
                    work_item_id,
                    delivered=False,
                    error=f"{type(exc).__name__}: EP cancellation delivery failed",
                    attempts=attempts,
                )
                logger.warning(
                    "Execution Plane cancellation delivery will retry",
                    work_item_id=str(work_item_id),
                    error_type=type(exc).__name__,
                )
            else:
                acknowledged = status_value in {
                    "cancel_requested",
                    "cancelled",
                    "completed",
                    "failed",
                    "reconciliation_required",
                }
                await _record_cancellation_attempt(
                    work_item_id,
                    delivered=acknowledged,
                    error=None if acknowledged else "EP returned an unsupported cancellation state",
                    attempts=attempts,
                )
                if acknowledged:
                    logger.info(
                        "Execution Plane acknowledged cancellation intent",
                        work_item_id=str(work_item_id),
                        status=status_value,
                    )
                else:
                    logger.warning(
                        "Execution Plane cancellation delivery will retry",
                        work_item_id=str(work_item_id),
                        status=status_value,
                    )


async def _mark_binding_delivering(work_item_id: UUID) -> tuple[bytes, int]:
    async with AsyncSessionLocal() as session:
        ae = (
            await session.exec(select(ActivityExecution).where(ActivityExecution.id == work_item_id).with_for_update())
        ).first()
        if ae is None:
            raise CompletionBindingNotFoundError(work_item_id)
        if ae.ep_status == "completed_synchronously":
            raise CompletionAlreadyReturnedSynchronouslyError(work_item_id)
        if ae.ep_status == "submitting":
            raise CompletionBindingNotReadyError
        if ae.ep_status == "handoff_pending" and datetime.now(UTC) < ae.updated_at + timedelta(seconds=2):
            raise CompletionBindingNotReadyError
        if ae.ep_status in {"delivering", "reconciliation_required"}:
            msg = "previous Temporal completion outcome is uncertain; manual reconciliation is required"
            raise RuntimeError(msg)
        enc_key = str(work_item_id)
        encrypted = ae.ep_task_token_ciphertext
        activity_attempt = ae.ep_activity_attempt
        ae.ep_status = "delivering"
        ae.updated_at = datetime.now(UTC)
        await session.commit()
    token_b64 = _encryptor().decrypt_field(encrypted, enc_key, "temporal_task_token")
    return base64.b64decode(token_b64), activity_attempt


async def _mark_event_processed(event_id: UUID, work_item_id: UUID, expected_attempt: int) -> None:
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        event = await session.get(ExecutionPlaneCompletionInbox, event_id, with_for_update=True)
        ae = (
            await session.exec(select(ActivityExecution).where(ActivityExecution.id == work_item_id).with_for_update())
        ).first()
        if event is not None:
            event.lease_expires_at = None
            if ae is not None and ae.ep_activity_attempt == expected_attempt:
                event.processed_at = now
                event.last_error = None
            else:
                event.next_attempt_at = now + timedelta(seconds=1)
                event.last_error = "activity binding changed during completion; retrying with current token"
        if ae is not None and ae.ep_activity_attempt == expected_attempt:
            ae.ep_status = "completed"
            ae.updated_at = now
        await session.commit()


async def _mark_event_for_reconciliation(
    event_id: UUID,
    work_item_id: UUID,
    reason: str,
    expected_attempt: int | None = None,
) -> None:
    now = datetime.now(UTC)
    async with AsyncSessionLocal() as session:
        event = await session.get(ExecutionPlaneCompletionInbox, event_id, with_for_update=True)
        ae = (
            await session.exec(select(ActivityExecution).where(ActivityExecution.id == work_item_id).with_for_update())
        ).first()
        if event is not None:
            event.lease_expires_at = None
            if ae is None or expected_attempt is None or ae.ep_activity_attempt == expected_attempt:
                event.next_attempt_at = now + timedelta(seconds=UNCERTAIN_RECONCILIATION_DELAY_SECONDS)
                event.last_error = reason[:1000]
            else:
                event.next_attempt_at = now + timedelta(seconds=1)
                event.last_error = "activity binding changed during completion; retrying with current token"
        if ae is not None and (expected_attempt is None or ae.ep_activity_attempt == expected_attempt):
            ae.ep_status = "reconciliation_required"
            ae.updated_at = now
        await session.commit()


async def _deliver_one(client: Client, event: ExecutionPlaneCompletionInbox) -> None:  # noqa: C901
    work_item_id = event.work_item_id
    try:
        task_token, activity_attempt = await _mark_binding_delivering(work_item_id)
    except CompletionAlreadyReturnedSynchronouslyError:
        return
    except CompletionBindingNotReadyError as exc:
        now = datetime.now(UTC)
        async with AsyncSessionLocal() as session:
            inbox_event = await session.get(ExecutionPlaneCompletionInbox, event.event_id, with_for_update=True)
            if inbox_event is not None:
                inbox_event.lease_expires_at = None
                inbox_event.next_attempt_at = now + timedelta(seconds=2)
                inbox_event.last_error = str(exc)
                await session.commit()
        return
    except CompletionBindingNotFoundError:
        await _mark_event_for_reconciliation(event.event_id, work_item_id, "dispatch binding is missing")
        return
    except RuntimeError as exc:
        await _mark_event_for_reconciliation(event.event_id, work_item_id, str(exc))
        return

    handle = client.get_async_activity_handle(task_token=task_token)
    try:
        if event.status == "completed":
            await handle.complete(event.result)
        else:
            result = event.result
            error_message = str(result.get("error", "Script execution failed"))
            error_type = str(result.get("error_type", "ScriptExecutionError"))
            if event.status == "cancelled":
                error_message = "Execution Plane work was cancelled before execution"
                error_type = "ExecutionPlaneWorkCancelled"
            await handle.fail(ApplicationError(error_message, result, type=error_type, non_retryable=True))
    except RPCError as exc:
        await _mark_event_for_reconciliation(
            event.event_id,
            work_item_id,
            f"Temporal completion outcome is uncertain ({exc.status.name})",
            activity_attempt,
        )
        logger.exception(
            "EP completion requires Temporal reconciliation",
            event_id=str(event.event_id),
            work_item_id=str(work_item_id),
            rpc_status=exc.status.name,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        await _mark_event_for_reconciliation(
            event.event_id,
            work_item_id,
            f"Temporal completion outcome is uncertain ({type(event).__name__})",
            activity_attempt,
        )
        logger.exception("EP completion requires Temporal reconciliation", event_id=str(event.event_id))
    else:
        await _mark_event_processed(event.event_id, work_item_id, activity_attempt)
        logger.info(
            "EP completion delivered to Temporal",
            event_id=str(event.event_id),
            work_item_id=str(work_item_id),
        )


async def run_completion_bridge(client: Client) -> None:
    """Continuously claim persisted EP events and complete AO-owned activities."""
    while True:
        try:
            await _deliver_requested_cancellations()
            events = await _claim_due_events()
            if not events:
                await _reconcile_missing_events()
                await asyncio.sleep(CALLBACK_POLL_SECONDS)
                continue
            for event in events:
                await _deliver_one(client, event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Execution Plane completion bridge iteration failed")
            await asyncio.sleep(CALLBACK_POLL_SECONDS)
