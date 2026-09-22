# ExecutionTarget Reconciler: Implementation Notes

Notes on the DB-level implementation of Target selection that follows from the Pool
Reconciler's output. See `pool-reconciler.md` for the reconciler design (AAP-92721).

---

## Target capacity selection

The ExecutionTarget Reconciler returns a ranked, ordered list of eligible Target IDs. The Work
Scheduler then needs to atomically claim the first Target in that list that has available
capacity — skipping over Targets that are full or currently being claimed by another EP
worker.

Two approaches are viable. The current design uses Option A.

### Option A: DB-level atomic claim (current)

Multiple EP worker processes race to claim capacity directly in Postgres using
`SELECT FOR UPDATE SKIP LOCKED`. The DB enforces isolation; no coordination between
processes is needed. Releasing capacity (job completion) is an unconditional decrement —
no lock required, since adding to an integer counter is safe under concurrency.

`ExecutionTarget` carries two integer fields:

- `pool_size` — configured maximum concurrent jobs for this Target
- `current_jobs` — live count of jobs currently running

Given the ordered list from the reconciler, the scheduler picks the first available Target
in a single atomic Postgres query:

```sql
SELECT et.*
FROM execution_targets et
WHERE et.id = ANY($1::uuid[])                    -- reconciler's ordered list of Target IDs
  AND et.current_jobs < et.pool_size             -- has remaining capacity
ORDER BY array_position($1::uuid[], et.id)       -- preserve reconciler's priority order
LIMIT 1
FOR UPDATE SKIP LOCKED
```

`array_position($1, et.id)` orders rows by their position in the input array, so the
reconciler's priority ordering is respected exactly.

`SKIP LOCKED` means: if another EP worker transaction is currently holding a lock on a
row (i.e. mid-claim on that Target), skip it and try the next one in priority order. No
blocking, no deadlock.

#### Transaction pattern

The full claim is one transaction:

1. Run the query above → row locked
2. `UPDATE execution_targets SET current_jobs = current_jobs + 1 WHERE id = $selected`
3. `UPDATE work_items SET execution_target_id = $selected WHERE id = $work_item`
4. Commit → lock released

On `WorkItem` completion or failure, decrement:

```sql
UPDATE execution_targets SET current_jobs = current_jobs - 1 WHERE id = $selected
```

If the EP worker crashes before commit, the transaction rolls back atomically —
`current_jobs` is never incremented and the lock is released. No orphan cleanup needed.

#### Cold-start Targets

For cold-start Targets, `pool_size` is NULL (unlimited). The `current_jobs < pool_size`
condition must handle NULL explicitly:

```sql
AND (et.pool_size IS NULL OR et.current_jobs < et.pool_size)
```

`current_jobs` is still tracked for observability even when unconstrained.

#### Edge case: capacity filter races the lock

A Target can pass the capacity filter (`current_jobs < pool_size`) and then lose the lock
race to another worker who also passed it. That is correct behaviour — the worker that
wins the lock is the one that will increment the count. The loser moves on to the next
Target in priority order. No corrective action needed.

### Option B: Singleton capacity manager

A separate in-process service holds all Target capacity state in memory. Any component
that needs to subtract capacity must ask this service. A thread within the process takes
a strong lock — potentially scoped to a single Target — performs the operation, and
releases the lock. Because only one process is ever allowed to run, lock semantics are
simple and flexible: no distributed locking, no DB contention for capacity ops.

Releasing capacity is lock-free: incrementing a counter is always safe and needs no
coordination, regardless of concurrency. This asymmetry (lock to subtract, no lock to
add) is a key property that makes the singleton viable without a global lock.

**Downsides:** the deployment constraint is strict — exactly one instance must be running
at all times. Two instances would split state and corrupt counts; zero means capacity
checks are unavailable. This requires careful orchestration (leader election or a process
supervisor that prevents duplicate instances). Cross-process reliability concerns also
apply: a message to the service can be dropped at any point in the call path, and the
caller must handle that correctly.

