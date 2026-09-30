"""Execution Plane TE worker — polls work_items and dispatches to Temporal on completion."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
import structlog

from execution_plane.bootstrap import bootstrap_local_cluster
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config import EPSettings, get_ep_settings, to_asyncpg_url
from execution_plane.drain_monitor import DrainMonitor
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import build_placement_resolver
from execution_plane.models.execution_target import BackendType
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.temporal_client import send_temporal_callback
from execution_plane.work_store import WorkStore
from execution_plane.worker_manager.base import WorkerManager
from execution_plane.worker_manager.vanilla_k8s.manager import (
    NodeExecutionError,
    RetryableDispatchError,
    VanillaK8sWorkerManager,
    WorkItemPayloadError,
)

logger = structlog.stdlib.get_logger(__name__)

POLL_INTERVAL_SECONDS = 5
NOTIFY_CHANNEL = "execution_plane_work_items"


CompletionCallback = Callable[[WorkItem], Awaitable[bool]]
WorkerManagers = dict[BackendType, WorkerManager]


async def _dispatch(item: WorkItem, target_store: ExecutionTargetStore, managers: WorkerManagers) -> dict[str, Any]:
    """Route a claimed item to the worker manager for its target's backend type."""
    if item.execution_target_id is None:
        message = "Work item has no execution target assigned"
        raise RetryableDispatchError(message)
    # Routing only; the manager re-loads the target with its secret to reach the cluster.
    target = await target_store.get(item.execution_target_id)
    if target is None or not target.enabled:
        message = "Assigned execution target is unavailable"
        raise RetryableDispatchError(message)
    manager = managers.get(target.backend_type)
    if manager is None:
        message = f"No worker manager for backend type {target.backend_type}"
        raise NodeExecutionError(message, error_type="UnsupportedBackendType")
    return await manager.dispatch(item)


async def _process_item(
    item: WorkItem,
    store: WorkStore,
    target_store: ExecutionTargetStore,
    managers: WorkerManagers,
    settings: EPSettings,
    completion_callback: CompletionCallback,
) -> None:
    """Dispatch to a cold-start pod, persist the result, then signal Temporal.

    Retryable failures requeue the item (no Temporal signal); terminal failures
    persist an error result and fail the suspended activity.
    """
    wi_id = str(item.id)

    try:
        result = await _dispatch(item, target_store, managers)
        item = await store.set_result(item.id, result, WorkItemStatus.COMPLETED)
        logger.info("Work item completed", work_item_id=wi_id)
    except RetryableDispatchError as e:
        logger.warning("Retryable dispatch failure, requeuing", work_item_id=wi_id, error=str(e))
        # Backoff throttles the serial poll loop against a persistently unavailable target.
        await asyncio.sleep(settings.dispatch_retry_backoff_seconds)
        await store.requeue(item.id)
        return  # Do not signal Temporal; the item stays PENDING for a later claim.
    except NodeExecutionError as e:
        item = await store.set_result(
            item.id,
            {"error": str(e), "error_type": e.error_type, "output": e.output},
            WorkItemStatus.FAILED,
        )
        logger.warning("Node execution failed", work_item_id=wi_id, error=str(e))
    except WorkItemPayloadError as e:
        item = await store.set_result(
            item.id,
            {"error": str(e), "error_type": "WorkItemPayloadError"},
            WorkItemStatus.FAILED,
        )
        logger.warning("Work item payload invalid", work_item_id=wi_id, error=str(e))
    except Exception as e:  # any other failure is terminal for the activity
        item = await store.set_result(
            item.id,
            {"error": str(e), "error_type": type(e).__name__},
            WorkItemStatus.FAILED,
        )
        logger.exception("Unexpected error processing work item", work_item_id=wi_id)

    if await completion_callback(item):
        await store.mark_signal_delivered(item.id)


async def _recover_undelivered(store: WorkStore, completion_callback: CompletionCallback) -> None:
    """Retry callbacks for items that completed but were never confirmed delivered.

    Runs once at startup. Bounded query: only terminal items with NULL signaled_at.
    Each store operation owns its own short-lived session.
    """
    items = await store.find_undelivered()
    if not items:
        return
    logger.info("Recovering undelivered Temporal callbacks", count=len(items))
    for item in items:
        if await completion_callback(item):
            await store.mark_signal_delivered(item.id)


async def _listen_loop(database_url: str, wakeup_event: asyncio.Event) -> None:
    """Hold a LISTEN connection and set the wakeup_event on every NOTIFY.

    Known gap: a zombie TCP connection (NAT expiry, silent load-balancer drop,
    VM migration) will not trigger the termination listener, so the worker
    silently falls back to POLL_INTERVAL_SECONDS cadence until the OS-level
    TCP keepalive eventually kills the connection. Fix: periodic self-NOTIFY or
    a LISTEN/UNLISTEN probe to detect stale connections. See AAP-92715.
    """
    while True:
        try:
            disconnected = asyncio.Event()
            conn: asyncpg.Connection = await asyncpg.connect(database_url)
            try:
                conn.add_termination_listener(lambda _, ev=disconnected: ev.set())
                await conn.add_listener(NOTIFY_CHANNEL, lambda *_: wakeup_event.set())
                # Recheck work queued before LISTEN became active (also on reconnect).
                wakeup_event.set()
                logger.info("Listening for notifications", channel=NOTIFY_CHANNEL)
                await disconnected.wait()
            finally:
                with contextlib.suppress(Exception):
                    await conn.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Notification listener failed, reconnecting in 5s")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _poll_loop(
    store: WorkStore,
    target_store: ExecutionTargetStore,
    managers: WorkerManagers,
    settings: EPSettings,
    wakeup_event: asyncio.Event,
    completion_callback: CompletionCallback,
) -> None:
    """Claim one work item at a time; sleep between polls when queue is empty."""
    logger.info("Execution Plane worker started, polling for work items")
    wakeup_event.set()  # process any items already present at startup
    while True:
        item = None
        try:
            item = await store.claim_one()
            if item:
                logger.info("Claimed work item", work_item_id=str(item.id))
                await _process_item(item, store, target_store, managers, settings, completion_callback)
        except Exception:
            logger.exception("Error in polling loop, will retry")

        if not item:
            wakeup_event.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wakeup_event.wait(), timeout=POLL_INTERVAL_SECONDS)


def _build_worker_managers(target_store: ExecutionTargetStore, settings: EPSettings) -> WorkerManagers:
    """Instantiate one worker manager per supported backend type."""
    return {BackendType.VANILLA_K8S: VanillaK8sWorkerManager(target_store, settings)}


async def run_worker(
    database_url: str,
    completion_callback: CompletionCallback = send_temporal_callback,
) -> None:
    """Run processing until cancelled, using the supplied database and callback.

    Cancellation closes both the notification listener and the polling task,
    then disposes the WorkStore.
    """
    settings = get_ep_settings()
    await bootstrap_local_cluster(database_url)
    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        drain_monitor = DrainMonitor(target_store, cluster_store, work_store)
        placement_resolver = build_placement_resolver(cluster_store, target_store)
        logger.debug(
            "ExecutionTarget reconciler constructed",
            resolver=type(placement_resolver).__name__,
        )
        managers = _build_worker_managers(target_store, settings)
        await drain_monitor.start()
        try:
            await _recover_undelivered(work_store, completion_callback)
            wakeup_event = asyncio.Event()
            async with asyncio.TaskGroup() as tg:
                tg.create_task(
                    _listen_loop(to_asyncpg_url(database_url), wakeup_event),
                    name="ep-listener",
                )
                tg.create_task(
                    _poll_loop(work_store, target_store, managers, settings, wakeup_event, completion_callback),
                    name="ep-poll",
                )
        finally:
            await drain_monitor.stop()


async def _run() -> None:
    settings = get_ep_settings()
    await run_worker(settings.database_url)


def main() -> None:
    """Entry point for the execution-plane-worker CLI command."""
    logging.basicConfig(level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )
    asyncio.run(_run())


if __name__ == "__main__":
    main()
