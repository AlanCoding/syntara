# Execution Plane: Work Executor

The Work Executor is the entry point for work submission into the EP. The Consumer
(Syntara or AWX) calls it to create a `WorkItem` and persist it to the `WorkStore`.

See [logical_components.md](logical_components.md) for how it fits into the logical decomposition of the Execution Plane service.

---

## Responsibility

Accepts a work submission from the Consumer — including a caller-generated UUID and the
work payload — validates it, and writes a `WorkItem` record to the `WorkStore` in
`PENDING` status. Returns to the Consumer immediately; execution is asynchronous.

The UUID is caller-owned. The Consumer generates it before submitting so it can track
the work item without waiting for a response.

---

## Open areas

- REST or RPC surface definition
- Validation rules on the work payload
- Isolation policy attachment at submission time
