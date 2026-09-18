# Execution Plane: Worker Manager

The Worker Manager acquires a worker from a selected Target, submits work, monitors
execution, and persists the result. It is the component that crosses from EP's internal
state into the actual compute infrastructure.

See [logical_components.md](logical_components.md) for how it fits into the logical decomposition of the Execution Plane service. For K8s-specific implementation see
[kubernetes-backend.md](kubernetes-backend.md).

---

## Interface

Defined as a Protocol in `execution_plane/worker_manager/base.py`:

```python
class WorkerManager(Protocol):
    async def dispatch(self, work_item: WorkItem) -> dict[str, Any]:
        """Dispatch work_item to an available worker and return the terminal result."""
```

Each backend type (`vanilla_k8s`, `openshell`, …) provides a concrete implementation.
The `PlacementResolver` (see `pool-reconciler-notes.md`) selects the right implementation
based on the chosen Target's `backend_type`.

---

## Capacity management

The ExecutionTarget Reconciler returns an ordered list of eligible Targets by label matching. The
Worker Manager is responsible for the next layer: ensuring it doesn't over-submit to a
Target and handling K8s-level rejections.

### Proactive

Before submitting, check that the Target has remaining capacity by comparing
`current_jobs` against `pool_size` in Postgres. This is the DB-level atomic selection
described in `pool-reconciler-notes.md`. It is fast, local, and handles concurrent EP
workers correctly without locking across K8s calls.

### Reactive

The Worker Manager submits work to K8s and may receive a rejection (pod unschedulable,
resource pressure, etc.). On bounce-back:

1. Write the rejection timestamp to the Pool Registry for that Target — fire-and-forget,
   no lock held.
2. Subsequent scheduling passes treat a Target that bounced within the backoff window as
   ineligible, even if `current_jobs` says it has headroom.

K8s interaction is slow; no locks are held across it. The proactive DB check runs first
and fast; the reactive update feeds back asynchronously after the K8s call returns.

---

## Placement failure backoff

When the Worker Manager fails to place a work item on any Target, it records a
`retry_after` timestamp on the work item in the Work Store. The Work Scheduler skips
items whose `retry_after` is in the future.

This prevents thrashing. Without an explicit hold-off in the DB, the same unplaceable
item can be retried immediately by any of the concurrent EP workers on any wakeup —
pg_notify, another worker's poll cycle, or the 5-second interval — with no guarantee
of ordering or spacing.
