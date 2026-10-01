# Cold-Start EP Demo Runbook (kind + podman-compose)

Status: **verified working** on 2026-09-30 (AAP-93615).

This runbook drives the cold-start node dispatch MVP end-to-end on a laptop: a
workflow with a single **script node** that echoes `hello world`, executed through
the API, dispatched as **one fresh pod** in a local **kind** cluster, returning
`COMPLETED` with the node's stdout.

It is the concrete companion to
[cold-start-node-dispatch.md](cold-start-node-dispatch.md) (what shipped + the
MVP shortcuts) and is referenced by
[`podman-compose.kind-demo.override.yml`](../../../podman-compose.kind-demo.override.yml).

Namespace, ServiceAccount, and RBAC live in
[execution-plane-init.yaml](execution-plane-init.yaml).

## Topology (two planes)

```
┌─────────────────────────────── podman-compose (app / control plane) ───────────────────────────────┐
│  syntara (API :8000)   temporal   temporal-worker (runs the AO dispatch activity)                   │
│  database   redis      execution-plane-worker  ── claims work_items, dispatches pods ──┐            │
└────────────────────────────────────────────────────────────────────────────────────────┼──────────┘
                                                                                           │ K8s API + port-forward
                                                                          ┌────────────────▼───────────────┐
                                                                          │ kind cluster (execution plane)  │
                                                                          │  ns execution-plane             │
                                                                          │  one cold-start pod per WorkItem│
                                                                          └─────────────────────────────────┘
```

Key point: the EP worker talks to the kind **API server** only. The gRPC call to
the node container tunnels **through** the API server via a port-forward, so the
EP worker never needs pod-network reachability — only API-server reachability.
It gets that by joining the `kind` podman network (see the override file), where
podman resolves the API server by container name
`execution-plane-control-plane:6443`.

## Prerequisites

- `podman`, `kind` (with `KIND_EXPERIMENTAL_PROVIDER=podman` if Docker is also
  installed), `kubectl`, `jq`.
- A **script node image**. The SDK node Containerfiles are not in this tree yet
  (they live on PR [#701](https://github.com/syntara-orchestration/syntara/pull/701),
  `feat/sdk-node-containers`; see shortcut 9 in
  [cold-start-node-dispatch.md](cold-start-node-dispatch.md)). This runbook uses
  the locally-built tag `localhost/syntara-node-script:migration-test`. From that
  checkout:

  ```bash
  make node-images CONTAINER_ENGINE=podman TAG=migration-test
  ```

Bring up the control plane in Step 1 **before** migrations or registration.
Kind cluster creation (Step 3) does not need the database; `ep-migrate` and the
SA-token registration do.

All `podman-compose` invocations below run from `backend/` via `uv run` (there is
no standalone `podman-compose` on PATH). Two file combinations are used:

```bash
cd backend
# Base stack only (database, redis, temporal). Safe before kind exists.
BASE_COMPOSE="uv run podman-compose -p syntara -f ../podman-compose.yml"
# Base + kind overlay. Requires the kind cluster (external `kind` network).
COMPOSE="$BASE_COMPOSE -f ../podman-compose.kind-demo.override.yml"
```

## Automated tests (no cluster)

These cover the worker, node-protocol codec/client, vanilla-k8s transport, and
the AO dispatch envelope. Real Kubernetes, gRPC streaming, and port-forward are
mocked. There is no pytest suite against the live kind path — that is Steps 1–8.

```bash
make install
cd backend

uv run pytest -v -n auto \
  execution-plane/tests/test_worker_manager.py \
  execution-plane/tests/test_worker.py \
  execution-plane/tests/test_config.py \
  execution-plane/tests/node_protocol/ \
  execution-plane/tests/worker_manager/ \
  tests/unit/workflows/activities/test_script_activity.py

make test-unit
```

## Step 1 — Start the podman-compose control plane

The control plane is the root [`podman-compose.yml`](../../../podman-compose.yml)
stack: PostgreSQL, Redis, Temporal, the Syntara API, and workers. The kind
override ([`podman-compose.kind-demo.override.yml`](../../../podman-compose.kind-demo.override.yml))
is applied later (Step 7). It declares `kind` as an **external** network, so
passing the override file before the kind cluster exists fails with a missing
network.

Do **not** use `make -C backend run-all` for this demo: it starts compose
*without* the kind override, so `execution-plane-worker` cannot resolve
`execution-plane-control-plane`.

### First time

From the repository root:

```bash
make setup
```

That installs Python/npm deps, generates secrets and TLS certs (Temporal and the
API need them), builds images, starts infrastructure with podman-compose, runs
Syntara + Execution Plane migrations, and seeds the admin user. After it
finishes you still need the kind cluster (Step 3) and the override (Step 7).

### Already bootstrapped

From the repository root:

```bash
make services-up
```

That runs `podman-compose -p syntara -f podman-compose.yml up` for `database`,
`redis`, `temporal`, `temporal-ui`, workers, MCP, and moto. Enough for
`ep-migrate` and registration.

Equivalent explicit command from `backend/` (Temporal TLS requires certs):

```bash
cd backend
make secrets-generate
make certs-generate
$BASE_COMPOSE up -d database redis temporal
```

Wait until Postgres accepts connections:

```bash
until podman exec syntara_database_1 pg_isready -U admin -d syntara_api; do sleep 2; done
```

Postgres is published on `127.0.0.1:5432` (`admin`/`admin`, database
`syntara_api`). That is what `make ep-migrate` and the registration snippet
talk to.

The Syntara API (`https://localhost:8000`) and `execution-plane-worker` are
started or recreated in Step 7 with the kind overlay.

## Step 2 — Apply Execution Plane migrations

The `execution_plane` schema is a **separate** Alembic tree from Syntara. The
API's `alembic upgrade head` does not create `execution_plane.clusters`.

```bash
make -C backend ep-migrate
```

Default URL: `postgresql+asyncpg://admin:admin@localhost:5432/syntara_api`.
Override with `APP_DATABASE_URL` if needed. Confirm:

```bash
podman exec syntara_database_1 psql -U admin -d syntara_api -c '\dt execution_plane.*'
```

You should see `clusters`, `execution_targets`, and `work_items`.

## Step 3 — Create or reuse the kind cluster

```bash
export KIND_EXPERIMENTAL_PROVIDER=podman   # this box also has real docker; be explicit
make -C backend ep-dev-up EP_DEV_ARGS="--provider kind"
```

This creates the `execution-plane-control-plane` container and the `kind` podman
network that the override file joins. `ep-dev-up` is idempotent: if
`execution-plane` already exists it reuses the cluster and re-registers it. To
wipe and recreate:

```bash
make -C backend ep-dev-reset EP_DEV_ARGS="--provider kind --yes"
```

Equivalent manual create:

```bash
kind create cluster --name execution-plane
```

`ep-dev-up` stores kubeconfig YAML as the target's `api_key`. The vanilla-k8s
transport needs a **Bearer token**, so that registration is not sufficient —
continue with Steps 4 and 6.

## Step 4 — Namespace, ServiceAccount, RBAC

The transport authenticates to the API server with a **Bearer token**, and kind
uses client-cert auth by default — so a ServiceAccount token is required (a raw
kubeconfig will not work). Apply
[execution-plane-init.yaml](execution-plane-init.yaml) (namespace
`execution-plane`, ServiceAccount `syntara-dispatcher`, Role, RoleBinding):

```bash
kubectl apply -f backend/docs/execution-plane/execution-plane-init.yaml
```

Mint a long-lived token for the SA and save it:

```bash
kubectl create token syntara-dispatcher -n execution-plane --duration=720h > /tmp/sa-token.txt
```

> The cluster may cap the lifetime below what you request (the API server's
> `--service-account-max-token-expiration`); kind honored 720h here (30 days). If
> the token expires, re-mint it and re-run Step 6 to update the target's `api_key`.

Sanity-check the token authenticates (should print "No resources found", i.e.
authenticated + authorized, not a 403):

```bash
PORT=$(kubectl config view -o jsonpath='{.clusters[?(@.name=="kind-execution-plane")].cluster.server}' | sed 's|.*:||')
kubectl --server="https://127.0.0.1:$PORT" --insecure-skip-tls-verify \
  --token="$(cat /tmp/sa-token.txt)" get pods -n execution-plane
```

## Step 5 — Load the script node image into kind

`kind load docker-image` fails against the podman provider here
("image not present locally"). Use the archive path, which preserves the exact
ref:

```bash
# /var/tmp is disk-backed; /tmp is often a tmpfs RAM disk too small for an image.
podman save -o /var/tmp/node-script.tar localhost/syntara-node-script:migration-test
kind load image-archive /var/tmp/node-script.tar --name execution-plane
# verify:
podman exec execution-plane-control-plane crictl images | grep node-script
```

## Step 6 — Register the ExecutionTarget

The registered target must point at the **in-network** API-server hostname
(`execution-plane-control-plane:6443`, reachable once the EP worker joins the
`kind` network) with the SA token as its `api_key`. Use
[`register_kind_sa_target.py`](../../execution-plane/tools/register_kind_sa_target.py)
— do **not** use kubeconfig YAML.

```bash
cd backend
uv run python execution-plane/tools/register_kind_sa_target.py /tmp/sa-token.txt
```

> Note: `dev_cli`'s `ep-dev-up` stores the full kubeconfig YAML as the target's
> `api_key`, which is **wrong** for the Bearer-token transport. This runbook
> registers the SA token directly instead. (Followup: teach `ep-dev-up` to mint
> and store an SA token — see shortcut 5.)

## Step 7 — Start the API and EP worker with the kind override

Step 1 left database, Redis, and Temporal running on the **base** compose file.
This step starts (or recreates) the Syntara API, the AO Temporal worker, and the
EP worker with the kind overlay so the EP worker can resolve the kind API
server.

The override file ([`podman-compose.kind-demo.override.yml`](../../../podman-compose.kind-demo.override.yml)):

- joins `execution-plane-worker` to the external `kind` network (API-server DNS), and
- sets `NODE_K8S_VERIFY_SSL=false` (kind's self-signed API cert, no CA on the target).

The base compose already wires the AO side on `temporal-worker`:
`APP_SCRIPT_NODES_ENABLED=true` and
`APP_NODE_CONTAINER_IMAGES={"script":"localhost/syntara-node-script:migration-test"}`
(shortcut 9).

The EP worker runs a **baked image** (no `src` mount). Rebuild after EP source
changes:

```bash
cd backend
$COMPOSE build execution-plane-worker syntara temporal-worker
$COMPOSE up -d syntara temporal-worker execution-plane-worker
# API: https://localhost:8000   Temporal UI: http://localhost:8081
podman logs --tail 20 syntara_execution-plane-worker_1
```

Expect `Execution Plane worker started, polling for work items`. If the worker
exits immediately, the usual causes are a missing `kind` network (Step 3 not
done) or a stale EP image (rebuild as above).

## Step 8 — Run the demo through the API

There is no pytest suite against this live kind path. Drive it through the API:

```bash
BASE="https://localhost:8000"
PW=$(podman exec syntara_syntara_1 cat /run/secrets/admin-password)
TOKEN=$(curl -sk -X POST "$BASE/api/v1/auth/login" -H "Content-Type: application/json" \
  -d "{\"username\":\"admin\",\"password\":\"$PW\"}" | jq -r .access_token)   # expires ~15 min
```

Create the workflow in a single `POST /workflows` — the request carries the full
`workflow_definition` (a manual trigger → one script node that echoes
`hello world`) and the backend creates version 1 for you. Grab the seeded default
project first:

```bash
PROJ=$(curl -sk "$BASE/api/v1/projects" -H "Authorization: Bearer $TOKEN" | jq -r '.resources[0].id')

WF=$(curl -sk -X POST "$BASE/api/v1/workflows" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d "{
    \"name\": \"hello-world-demo\",
    \"description\": \"Cold-start EP: single script node echoing hello world\",
    \"project_id\": \"$PROJ\",
    \"workflow_definition\": {
      \"name\": \"hello-world-demo\",
      \"schema_version\": \"2.0.0\",
      \"triggers\": [{\"id\": \"trigger\", \"type\": \"manual_trigger\", \"parameters\": {}}],
      \"nodes\": [{\"id\": \"script_node\", \"name\": \"Hello Script\", \"type\": \"script\",
                   \"parameters\": {\"code\": \"echo 'hello world'\", \"language\": \"bash\"}}],
      \"edges\": [{\"from\": \"trigger\", \"to\": \"script_node\"}]
    }
  }" | jq -r .id)
echo "workflow: $WF"
```

Then create an execution. `ExecutionCreate` requires `workflow_id` and
`trigger_node_id` — the latter is the **trigger** id (`"trigger"`), the entry
point to start from, *not* the script node id:

```bash
EXEC=$(curl -sk -X POST "$BASE/api/v1/executions" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"workflow_id\":\"$WF\",\"trigger_node_id\":\"trigger\",\"input_data\":{}}" \
  | jq -r .id)

# poll
for i in $(seq 1 30); do
  ST=$(curl -sk "$BASE/api/v1/executions/$EXEC" -H "Authorization: Bearer $TOKEN" | jq -r .status)
  echo "$ST"; { [ "$ST" = completed ] || [ "$ST" = failed ]; } && break; sleep 3
done
```

Easiest re-run: retry an existing execution — it reuses the same workflow
version, inputs, and trigger, so no need to restate `trigger_node_id`:

```bash
EXEC=$(curl -sk -X POST "$BASE/api/v1/executions/$EXEC/retry" \
  -H "Authorization: Bearer $TOKEN" | jq -r .id)
```

## Verification

Execution reaches `completed`, the pod is created and reaped (namespace ends
empty), and the node's stdout is stored on the work item:

```bash
podman exec syntara_database_1 psql -U admin -d syntara_api -x -c \
  "SELECT id, status, result FROM execution_plane.work_items ORDER BY created_at DESC LIMIT 1;"
# result | {"output": {"stderr": "", "stdout": "hello world\n", "return_code": 0, "stdout_json": null}}
```

EP worker happy-path log sequence:

```
Claimed work item      work_item_id=...
Work item completed    work_item_id=...
Temporal callback delivered  status=completed  work_item_id=...
```

## Bug fixed during this bring-up

**Bearer token dropped by the kubernetes python client (transport.py).**
`transport.py` set `config.api_key["authorization"]` + `api_key_prefix["authorization"]="Bearer"`.
kubernetes-client v36 renamed the bearer auth scheme `authorization` → `BearerToken`;
its backward-compat shim reads the **token** from the legacy `authorization` key
but looks the **prefix** up only under `BearerToken`
([kubernetes-client/python#2595](https://github.com/kubernetes-client/python/issues/2595)).
Result: the header went out as `Authorization: <raw-token>` (no `Bearer ` scheme),
the API server rejected it as `system:anonymous`, and the transport masked the
403 as the generic `"OpenShift request failed"`.

Fix (committed): set the token **and** prefix under both `authorization` and
`BearerToken` so every client version emits `Authorization: Bearer <token>`. This
is a real production bug, not a demo-only workaround — any deployment on
kubernetes-client ≥36 with Bearer-token targets hit it.

## Gotchas seen (and fixes)

- **`connection refused` on `localhost:5432`** — the compose database is not
  running. Start it in Step 1 (`make setup`, `make services-up`, or
  `$BASE_COMPOSE up -d database redis temporal`).
- **`missing networks: kind`** — the override was used before the kind cluster
  existed. Create the cluster (Step 3) first, then retry Step 7.
- **`credentials_created_by_fkey` / `Key (created_by)=(00000000-…)`** — `ep-dev-up`
  used to attribute the OpenShift credential to the nil UUID, which is not a
  row in `principals`. It now uses the seeded `admin` user. Re-run Step 3 after
  pulling that fix; if it still fails with "Bootstrap admin user not found",
  run `make -C backend db-seed`.
- **`relation "execution_plane.clusters" does not exist`** — the EP Alembic tree
  was not applied. Run `make -C backend ep-migrate` (Step 2).
- **`node(s) already exist for a cluster with the name "execution-plane"`** —
  `kind create` on an existing cluster. `make ep-dev-up` reuses it; `ep-dev-reset
  --yes` recreates.
- **Kubernetes API HTTP 403 / anonymous** — kubeconfig stored as `api_key`, or
  missing `Bearer` prefix. Register the SA token (Step 6); rebuild the EP image
  if the transport fix is missing.
- **`podman-compose` "missing networks: default"** — the override must re-declare
  the project `default` network (`default: {}`) alongside the external `kind`
  one, or the EP worker loses DB/Temporal DNS.
- **`NotNullViolationError: cluster_type`** on EP worker start — a **stale**
  `localhost/execution-plane:latest` image predating the `cluster_type` column.
  The EP worker runs the **baked image** (no `src` mount), so any EP source
  change — including the transport fix above — requires a rebuild:
  `$COMPOSE build execution-plane-worker && $COMPOSE up -d --force-recreate execution-plane-worker`.
- **`kind load docker-image` "not present locally"** with the podman provider —
  use `podman save` + `kind load image-archive` (Step 5).
- **Image pull errors in the pod** — the PR 701 image was not loaded into kind
  (Step 5).

## Teardown

```bash
cd backend
$COMPOSE down
kind delete cluster --name execution-plane
```
