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

- `podman`, `kind` (with `KIND_EXPERIMENTAL_PROVIDER=podman`), `kubectl`, `jq`.
- The full-stack compose already builds/runs locally (`make setup` once).
- A **script node image**. The SDK node Containerfiles are not in this tree yet
  (they live on PR #701, `feat/sdk-node-containers`; see shortcut 9 in
  [cold-start-node-dispatch.md](cold-start-node-dispatch.md)). This runbook uses
  the locally-built tag `localhost/syntara-node-script:migration-test`.

All `podman-compose` invocations below run from `backend/` via `uv run` (there is
no standalone `podman-compose` on PATH) and use both compose files:

```bash
cd backend
COMPOSE="uv run podman-compose -p syntara -f ../podman-compose.yml -f ../podman-compose.kind-demo.override.yml"
```

## Step 1 — Create the kind cluster

```bash
export KIND_EXPERIMENTAL_PROVIDER=podman   # this box also has real docker; be explicit
kind create cluster --name execution-plane
```

This creates the `execution-plane-control-plane` container and the `kind` podman
network that the override file joins.

## Step 2 — Namespace, ServiceAccount, RBAC

The transport authenticates to the API server with a **Bearer token**, and kind
uses client-cert auth by default — so a ServiceAccount token is required (a raw
kubeconfig will not work). Apply:

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Namespace
metadata:
  name: execution-plane
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: syntara-dispatcher
  namespace: execution-plane
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: syntara-node-dispatcher
  namespace: execution-plane
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods/portforward"]
    verbs: ["create", "get"]
  - apiGroups: [""]
    resources: ["pods/log", "pods/status"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: syntara-node-dispatcher
  namespace: execution-plane
subjects:
  - kind: ServiceAccount
    name: syntara-dispatcher
    namespace: execution-plane
roleRef:
  kind: Role
  name: syntara-node-dispatcher
  apiGroup: rbac.authorization.k8s.io
EOF
```

Mint a long-lived token for the SA and save it:

```bash
kubectl create token syntara-dispatcher -n execution-plane --duration=720h > /tmp/sa-token.txt
```

Sanity-check the token authenticates (should print "No resources found", i.e.
authenticated + authorized, not a 403):

```bash
PORT=$(kubectl config view -o jsonpath='{.clusters[?(@.name=="kind-execution-plane")].cluster.server}' | sed 's|.*:||')
kubectl --server="https://127.0.0.1:$PORT" --insecure-skip-tls-verify \
  --token="$(cat /tmp/sa-token.txt)" get pods -n execution-plane
```

## Step 3 — Load the script node image into kind

`kind load docker-image` fails against the podman provider here
("image not present locally"). Use the archive path, which preserves the exact
ref:

```bash
podman save -o /tmp/node-script.tar localhost/syntara-node-script:migration-test
kind load image-archive /tmp/node-script.tar --name execution-plane
# verify:
podman exec execution-plane-control-plane crictl images | grep node-script
```

## Step 4 — Register the ExecutionTarget

The registered target must point at the **in-network** API-server hostname
(`execution-plane-control-plane:6443`, reachable once the EP worker joins the
`kind` network) with the SA token as its `api_key`. Register via the dev CLI
internals:

```bash
cd backend
uv run python - "$(cat /tmp/sa-token.txt)" <<'PY'
import asyncio, sys
sys.path.insert(0, "execution-plane/tools")
from dev_cli import EnvironmentDetails, EnvironmentProvider, _register_environment_record, DEFAULT_DATABASE_URL

token = sys.argv[1].strip()
details = EnvironmentDetails(
    provider=EnvironmentProvider.KIND,
    name="execution-plane",
    endpoint="https://execution-plane-control-plane:6443",
    namespace="execution-plane",
    api_key=token,  # ServiceAccount bearer token — transport uses Bearer auth
    labels={"provider": "kind", "cluster": "execution-plane"},
)
asyncio.run(_register_environment_record(details, DEFAULT_DATABASE_URL))
print("registered cluster + default target")
PY
```

> Note: `dev_cli`'s `ep-dev-up` stores the full kubeconfig YAML as the target's
> `api_key`, which is **wrong** for the Bearer-token transport. This runbook
> registers the SA token directly instead. (Followup: teach `ep-dev-up` to mint
> and store an SA token — see shortcut 5.)

## Step 5 — Bring up the stack with the demo override

The override file ([`podman-compose.kind-demo.override.yml`](../../../podman-compose.kind-demo.override.yml)):

- joins `execution-plane-worker` to the external `kind` network (API-server DNS), and
- sets `NODE_K8S_VERIFY_SSL=false` (kind's self-signed API cert, no CA on the target).

The base compose already wires the AO side on `temporal-worker`:
`APP_SCRIPT_NODES_ENABLED=true` and
`APP_NODE_CONTAINER_IMAGES={"script":"localhost/syntara-node-script:migration-test"}`
(shortcut 9).

```bash
cd backend
$COMPOSE up -d syntara temporal-worker execution-plane-worker
# EP worker should log: "Execution Plane worker started, polling for work items"
podman logs --tail 5 syntara_execution-plane-worker_1
```

## Step 6 — Run the demo through the API

```bash
BASE="https://localhost:8000"
PW=$(podman exec syntara_syntara_1 cat /run/secrets/admin-password)
TOKEN=$(curl -sk -X POST "$BASE/api/v1/auth/login" -H "Content-Type: application/json" \
  -d "{\"username\":\"admin\",\"password\":\"$PW\"}" | jq -r .access_token)   # expires ~15 min
```

Create a workflow whose only node is a script node echoing `hello world`
(project + workflow + version). Then create an execution. `ExecutionCreate`
requires both `workflow_id` and `trigger_node_id` (the script node's id):

```bash
EXEC=$(curl -sk -X POST "$BASE/api/v1/executions" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"workflow_id\":\"<WORKFLOW_ID>\",\"trigger_node_id\":\"<SCRIPT_NODE_ID>\",\"input_data\":{}}" \
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
EXEC=$(curl -sk -X POST "$BASE/api/v1/executions/<PRIOR_EXEC_ID>/retry" \
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

- **`podman-compose` "missing networks: default"** — the override must re-declare
  the project `default` network (`default: {}`) alongside the external `kind`
  one, or the EP worker loses DB/Temporal DNS.
- **`NotNullViolationError: cluster_type`** on EP worker start — a **stale**
  `localhost/execution-plane:latest` image predating the `cluster_type` column.
  The EP worker runs the **baked image** (no `src` mount), so any EP source
  change — including the transport fix above — requires a rebuild:
  `$COMPOSE build execution-plane-worker && $COMPOSE up -d --force-recreate execution-plane-worker`.
- **`kind load docker-image` "not present locally"** with the podman provider —
  use `podman save` + `kind load image-archive` (Step 3).

## Teardown

```bash
cd backend
$COMPOSE down
kind delete cluster --name execution-plane
```
