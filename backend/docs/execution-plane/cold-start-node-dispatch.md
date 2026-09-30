# Execution Plane: Cold-Start Node Dispatch (MVP)

Status: **implemented** for the Vanilla Kubernetes backend under
[AAP-93615](https://redhat.atlassian.net/browse/AAP-93615).

This document records what the cold-start node dispatch MVP actually ships, and —
more importantly — the shortcuts it takes on purpose. Each shortcut below is
written to be turned directly into a followup Jira: it states the shortcut, why
it is acceptable for the MVP, and the shape of the work that removes it.

For the general worker-manager design (capacity, WorkWatcher, placement backoff)
see [worker-manager.md](worker-manager.md). For the service boundary shortcuts
(direct DB writes instead of an HTTP API) see [integration.md](integration.md).

---

## What shipped

The MVP runs one fresh Kubernetes pod per `WorkItem` and talks to it over gRPC:

- **`VanillaK8sWorkerManager`** (`worker_manager/vanilla_k8s/manager.py`) — loads
  the `ExecutionTarget` (with its secret), reads the invocation envelope + image
  from `work_item.payload`, creates a pod, streams the single result over gRPC,
  reaps the pod, and maps the result to a Temporal-compatible activity result.
  The manager is **node-type agnostic**; it never inspects the node kind.
- **Vendored node protocol + transport** (`node_protocol/`, `worker_manager/
  vanilla_k8s/transport.py`, `forward.py`) — a copy of the SDK node gRPC client,
  codec, and generated protobuf, plus the K8s pod lifecycle / port-forward
  transport. Vendored (copied), not taken as a package dependency — see shortcut
  6.
- **AO builds the envelope** — the Temporal activity
  (`ep_dispatch_activity.py`) selects the container image per node type and
  builds the full invocation envelope, then writes it into `work_items.payload`.
  The EP worker stays "dumb". This matches the shape used by the synchronous
  node-container path so one SDK image serves both routes.
- **Three-outcome dispatch** in `worker.py`:
  - success → `set_result(COMPLETED)` → Temporal `handle.complete()`
  - node ran but failed (`NodeExecutionError`) → `set_result(FAILED)` →
    Temporal `handle.fail()`
  - transport/capacity failure (`RetryableDispatchError`) → `WorkStore.requeue()`
    back to `PENDING` after a fixed backoff, no Temporal signal.

---

## MVP shortcuts and followups

### 1. Secrets at rest in `work_items.payload` (plaintext)

**Shortcut.** The invocation envelope — including node `inputs` — is persisted
in `work_items.payload` (JSONB) in plaintext. Script nodes carry no credentials,
so nothing sensitive is stored today.

**Why acceptable for MVP.** The only wired node type is `script`, whose
`credentials.resolved` is empty. The DB is already the trust boundary for the
Temporal task token stored alongside it.

**Followup shape.** Before any credential-bearing node type (`aap_*`, `agentic`)
uses this path, either (a) encrypt `work_items.payload` at rest, or (b) have the
EP worker resolve credential *references* at dispatch time so secrets never land
in the row. Option (b) is cleaner but requires EP to reach a secret service,
which reintroduces a Syntara coupling — decide alongside the service-boundary
work in [integration.md](integration.md). Until then, the AO activity must not
route credential-bearing node types through `_dispatch_to_te`.

### 2. Output mapping is field-selection only

**Shortcut.** `_map_result` supports simple field selection (`output_config`
keys picked out of the node's `Result` dict). Template-expression output mapping
(e.g. `"${result.stdout}"`) is not supported.

**Why acceptable for MVP.** This matches the existing script-node limitation;
template mapping needs `NamespaceResolver` from the syntara package, which the EP
must not import.

**Followup.** Tracked by
[AAP-93073](https://redhat.atlassian.net/browse/AAP-93073) — resolve output
mapping without importing syntara (e.g. a small shared expression evaluator, or
mapping applied AO-side after the result returns).

### 3. No cancellation

**Shortcut.** The gRPC contract has a `Cancel` RPC and `run_pod` accepts a
`cancelled` threading event, but the EP worker never sets it. A cancelled
`WorkItem` does not stop a running pod.

**Why acceptable for MVP.** Cold-start pods are short-lived and reaped on
completion; a leaked pod is bounded by the node timeout.

**Followup shape.** Wire `WorkItem` cancellation → set the `cancelled` event /
invoke `NodeService.Cancel`, and reap the pod on cancel. Needs a cancellation
signal path from AO/Temporal into the EP worker (a status column poll or a
second pg_notify channel).

### 4. Fixed backoff, no per-item hold-off

**Shortcut.** `WorkStore.requeue()` returns the item to `PENDING` and clears its
target; the poll loop sleeps `dispatch_retry_backoff_seconds` before requeuing.
There is no per-item attempt counter and no not-before timestamp, so a second
concurrent worker could immediately re-claim a just-requeued item.

**Why acceptable for MVP.** The worker claims serially and there is a single EP
worker in local/dev; the fixed sleep throttles a persistently unavailable
target well enough to avoid a hot loop.

**Followup shape.** This is the "Placement failure backoff" design already
sketched in [worker-manager.md](worker-manager.md#placement-failure-backoff):
add a `last_placement_failed_at` column (Alembic migration), make requeue a
single atomic UPDATE, and have `claim_one()` skip items inside the penalty
window. Also add an attempt counter to cap retries.

### 5. Cluster TLS/topology is global, not per-target

**Shortcut.** `node_k8s_verify_ssl` and `node_k8s_ca_certificate` are global
`EPSettings`, applied to every target. Only `namespace` is read per-target.
`_k8s_target()` in the manager is the single place that maps an `ExecutionTarget`
onto the connection dict.

**Why acceptable for MVP.** Local kind/minikube uses one self-signed API server;
a single global `verify_ssl=false` covers dev.

**Followup shape.** Move `namespace`, node selectors/tolerations, CA bundle, and
verify flag into a backend-specific metadata block on the `ExecutionTarget` with
a K8s/RHEL discriminator (Michael's in-flight refactor). When that lands, only
`_k8s_target()` changes.

### 6. Node protocol is vendored, not a dependency

**Shortcut.** `syntara_node_protocol` (gRPC client, codec, generated protobuf)
is copied into `execution_plane/node_protocol/` rather than depended on as a
package. Imports were rewritten `syntara_node_protocol` → `execution_plane.
node_protocol`; the generated `node_pb2.py` is left byte-for-byte unchanged
because its serialized `FileDescriptorProto` embeds the original module path.

**Why acceptable for MVP.** Avoids a cross-package dependency and a publish step
while the protocol is still churning, and keeps the EP importable without the
syntara tree.

**Followup shape.** When the protocol stabilizes, either publish
`syntara-node-protocol` as a real package and depend on it, or keep it vendored
and add a drift check (a test that diffs the vendored copy against source).
Decide with the SDK owners.

### 7. Serial, synchronous dispatch — no WorkWatcher

**Shortcut.** The poll loop claims and dispatches one item at a time; the pod
lifecycle runs in a worker thread via `asyncio.to_thread`, and the poll loop
awaits it before claiming the next item. There is no `WorkWatcher` and no
concurrent dispatch.

**Why acceptable for MVP.** Correct and simple; throughput is not an MVP goal.

**Followup shape.** Introduce concurrent dispatch (bounded `asyncio` tasks or the
`WorkWatcher` in [worker-manager.md](worker-manager.md)) once capacity claiming
(shortcut 4 / reconciler placement) is in place, so concurrency does not
oversubscribe a target.

### 8. Placement uses the first default target, not the reconciler

**Shortcut.** `claim_one()` assigns every claimed item to the first enabled,
active, default `ExecutionTarget`. `build_placement_resolver()` is constructed in
`run_worker` but only logged — its ranking is not yet consulted.

**Why acceptable for MVP.** Dev clusters register a single default target, so
ranking is a no-op.

**Followup shape.** Wire the `ExecutionTarget` reconciler's ranked candidate list
into claim/placement (the Work Scheduler loop in
[worker-manager.md](worker-manager.md)), replacing the hard-coded default-target
select in `claim_one()`.

### 9. Node container image is a locally-built tag, not a published artifact

**Shortcut.** `node_container_images` (config `base.py`) defaults to `{}`, and the
value we run with — e.g. `localhost/syntara-node-script:migration-test` — is a
**local build tag**, not a registry ref. Producing it is manual: build the SDK
node Containerfiles under `backend/nodes/` and load the result into the target
cluster (`kind load docker-image <ref> --name execution-plane`). Nothing pulls or
publishes it. The dev full-stack compose wires the two required settings on the
`temporal-worker` service (that is where the AO dispatch activity runs) —
`APP_SCRIPT_NODES_ENABLED` and `APP_NODE_CONTAINER_IMAGES` — see the commented
block in [`podman-compose.yml`](../../../podman-compose.yml).

**Why acceptable for MVP.** The SDK node images (script, agent, http-request,
aap-job, aap-workflow) live on a separate, not-yet-merged branch
(`feat/sdk-node-containers`, PR #701). Until that lands and CI publishes images,
building locally and loading by tag is the only way to get an image into the
cluster. Nothing in the cold-start path runs without one, so this is called out
as an explicit, assignable obligation rather than left as a silent gap.

**Followup shape.** (1) Land the SDK node containers work so the Containerfiles
and node runtime reach `devel`/`1803`. (2) Have CI build and push immutable,
digest-pinned node images to a registry the target clusters can pull. (3) Replace
the local tag with those published refs — at which point a documented default for
`node_container_images` becomes reasonable, with the `APP_`-prefixed setting still
available for installers shipping a custom image. `script_nodes_enabled` stays
`False` by default (security gate); only per-environment config (like the dev
compose) enables it.

---

## Deliberately *not* abstracted

No receptor/transport indirection layer was introduced. The transport is a
direct Kubernetes port-forward + gRPC call (`transport.py` → `forward.py` →
`node_protocol.client.invoke`). If a second transport (e.g. receptor) is ever
needed, introduce the seam at that point against two concrete implementations —
not speculatively now. There is no followup to file for this; it is a note so a
future reader does not re-add a premature interface.
