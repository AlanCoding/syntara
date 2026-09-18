# Execution Plane: Work Store

`WorkStore` is the internal Python interface for interacting with `WorkItem` records in its durable
store (Postgres in the current branch).
See [logical_components.md](logical_components.md) for how it fits into the logical decomposition of the Execution Plane service.

**Source:** `execution_plane/work_store.py`, `execution_plane/models/work_item.py`

---

## Context

The Consumer (Syntara or AWX) submits work to the [Work Executor](work-executor.md),
which persists a `WorkItem` to the `WorkStore` in `PENDING` status. The `WorkStore`
has durable Postgres storage — a `WorkItem` written there survives process restarts and
worker crashes.

From that point, multiple other components interact with the `WorkStore` through its
public methods:

| Component | Operation |
|---|---|
| Work Executor | Creates the initial `WorkItem` record |
| Work Scheduler | `claim_one()` — atomically moves one item from `PENDING` to `CLAIMED` |
| Worker Manager | `set_result()` — writes result and transitions to `COMPLETED` or `FAILED` |
| Worker Manager | Records placement failure with `retry_after` to prevent thrashing |
| Completion Notifier | Reads terminal items with undelivered callbacks |
| Startup recovery | `find_undelivered()` — finds terminal items with `NULL signaled_at` |
| Worker Manager | `mark_signal_delivered()` — sets `signaled_at` after callback is confirmed |

---

## Work item lifecycle

```
PENDING → CLAIMED → COMPLETED
                 → FAILED
```

| Status | Meaning |
|---|---|
| `PENDING` | Submitted, waiting to be claimed by a worker |
| `CLAIMED` | Locked by one worker; execution in progress |
| `COMPLETED` | Finished successfully; result stored |
| `FAILED` | Execution error; error detail stored |

---

## Key operations

| Method | Description |
|---|---|
| `claim_one()` | Atomically claims one `PENDING` item (`SELECT FOR UPDATE SKIP LOCKED`) |
| `set_result(item, result, status)` | Writes result dict and transitions status |
| `mark_signal_delivered(item)` | Sets `signaled_at` after Temporal callback confirmed |
| `find_undelivered()` | Finds terminal items with `NULL signaled_at` for startup recovery |
| `check_ready()` | Health check — verifies DB connectivity and can read work items |

---

## Schema

`work_items` table (abbreviated):

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | Caller-generated; passed in at submission |
| `status` | VARCHAR | `WorkItemStatus` enum |
| `payload` | JSONB | Input config, output config, and routing metadata |
| `result` | JSONB | Written on completion or failure |
| `created_at` | TIMESTAMPTZ | |
| `claimed_at` | TIMESTAMPTZ | Set on claim |
| `completed_at` | TIMESTAMPTZ | Set on terminal transition |
| `signaled_at` | TIMESTAMPTZ | Set when Temporal callback is confirmed delivered |

---

## pg_notify

The channel `execution_plane_work_items` receives a NOTIFY on each new work item
submission. The EP worker listens on this channel to wake immediately rather than waiting
for the poll interval. The listen loop is in `worker.py:_listen_loop`.

---

## Open areas

- `retry_after` field for placement failure backoff (see `worker-manager.md`)
- `execution_target_id` FK for tracking which Target handled a given item
- Cancel / revoke path
