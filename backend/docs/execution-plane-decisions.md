# Execution Plane — Key Design Decisions

[AAP-90685](https://redhat.atlassian.net/browse/AAP-90685) · Parent: [ANSTRAT-1803](https://redhat.atlassian.net/browse/ANSTRAT-1803)

Key architectural options — either still live, or designated as formally closed in this document. For implementation requirements see [execution-plane.md](execution-plane.md).

Each section states the options, the current working position, and what would change that position.

---

## D1: Control plane topology — Centralized Scheduler vs. Distributed Schedulers

**Question:** Does scheduling and work queue management happen in one central Task Executor, or does each target cluster get its own Task Executor instance?

Note: execution is already distributed in both options — worker pods run per-cluster regardless. The question is where the *scheduling logic and Work Store* live.

**Centralized Scheduler (current position)**

One Task Executor deployment, one PostgreSQL Work Store. All capacity reservations, back-pressure decisions, and result persistence happen in one place. Per-cluster operations (pod exec, OpenShell gRPC calls) are delegated to a lightweight per-cluster agent (Pool Agent for vanilla K8S, OpenShell Gateway for OpenShell clusters).

```mermaid
graph LR
    subgraph AO["Automation Orchestrator"]
        API["Syntara API"]
        TW["Temporal Worker"]
        TE["Task Executor"]
        PG[("PostgreSQL<br/>(shared)")]
    end
    PA1["Pool Agent<br/>(cluster A)"]
    PA2["Pool Agent<br/>(cluster B)"]
    GW["OpenShell Gateway<br/>(cluster C)"]

    API -->|"create execution"| PG
    API -->|"start execution"| TW
    TW -->|"write work item"| PG
    TW -->|"POST /schedule"| TE
    TE -->|"read pending work"| PG
    TE -.->|"work complete"| TW
    TE -->|"Agent API"| PA1
    TE -->|"Agent API"| PA2
    TE -->|"gRPC"| GW
```

Within Centralized Scheduler there is a sub-option on the database — see D5.

**Distributed Schedulers**

Each target cluster runs a full Task Executor instance with its own Work Store. The central Temporal Worker dispatches to cluster APIs rather than a single TE. Capacity tracking and back-pressure are local to each cluster.

```mermaid
graph LR
    subgraph AO["Automation Orchestrator"]
        API["Syntara API"]
        TW["Temporal Worker"]
    end

    subgraph CLA["Cluster A"]
        TE_A["Task Executor"]
        PG_A[("PostgreSQL")]
        WP_A["Worker Pods"]
    end

    subgraph CLB["Cluster B"]
        TE_B["Task Executor"]
        PG_B[("PostgreSQL")]
        WP_B["Worker Pods"]
    end

    API -->|"start execution"| TW
    TW -->|"POST /submit<br/>(work item + handle)"| TE_A
    TW -->|"POST /submit<br/>(work item + handle)"| TE_B
    TE_A --> PG_A
    TE_A --> WP_A
    TE_B --> PG_B
    TE_B --> WP_B
    TE_A -.->|"work complete"| TW
    TE_B -.->|"work complete"| TW
```

Back-pressure is the load-bearing problem here. A per-cluster API doesn't escape the need for a global view of capacity — you still need something that decides whether to queue or dispatch when the sum of cluster capacity is exhausted. Without a meta-scheduler, you get races. With one, you've rebuilt the singleton.

**Centralized Scheduler drawback:** Some backends (e.g. podman warm containers) would require the Task Executor to manage worker pool lifecycle directly — pre-warming containers, replenishing pools, tracking readiness. That work mostly duplicates what OpenShell already does, and it sits awkwardly inside a service whose job is to claim and dispatch, not to manage pool infrastructure. We are mostly not interested in those backends; this drawback is noted for completeness rather than as a live concern. A planned mitigation is a custom-service backend type — the TE dispatches to an operator-provided API that owns its own worker management, keeping pool lifecycle concerns out of the TE entirely (to be documented separately).

**Distributed Schedulers drawback:** The feature set we would develop in a per-cluster Task Executor — sandbox lifecycle, warm pools, capacity tracking, credential injection — overlaps heavily with what OpenShell already provides. Building it is largely reinventing OpenShell for backends where OpenShell isn't used.

**Working position:** Centralized Scheduler. The Pool Agent / OpenShell Gateway per-cluster pattern achieves locality for K8S operations without distributing the scheduling and capacity problem. Distributed Schedulers is not entirely ruled out — a future requirement (e.g. independently operable per-cluster scheduling, or a backend whose worker management cannot be expressed as a TE backend type) could push back toward it. It is not a live concern today.

**What would change this:** A concrete requirement that the per-cluster scheduler must be independently operable (e.g., the cluster owner installs and manages it without Syntara connectivity). That's an offline/air-gap topology question, not a normal-path question.

---

## D2: OpenShell installation — who owns it?

**Question:** Does Syntara install and manage OpenShell, or does the operator install OpenShell independently and connect it to Syntara?

**Option A — Operator installs OpenShell (current position)**

OpenShell is installed by the platform operator as a day-2 operation (OLM, Helm, or manual). Syntara registers an Execution Target pointing at the OpenShell gRPC endpoint. Syntara does not manage the OpenShell lifecycle.

**Option B — In-app OpenShell provisioning**

Syntara's UI includes an "Install OpenShell" flow that drives installation onto a registered cluster. Syntara owns the OpenShell lifecycle.

**Working position:** Option A. In-app installation is installer-scale work (OLM integration, cluster admin privilege escalation, upgrade coordination) that is out of scope for Phase 2. Registering an already-installed OpenShell as an Execution Target is workable with no installer changes.

**PM question:** Is there a customer expectation that the AO installer handles OpenShell installation, or is day-2 manual connection acceptable?

---

## D3: OpenShell rollout — cold sandboxes before warm pools

**Question:** Should Phase 2 implement warm pools from the start, or prove OpenShell API compatibility with cold sandboxes first?

**Option A — Cold sandboxes first (current position)**

Phase 2 uses `CreateSandbox` + `ExecSandbox` per task. Each task incurs sandbox startup latency. No `SandboxWorkloadTemplate` or warm pool configuration required.

Rationale: Cold sandboxes exercise the full OpenShell dispatch path (gRPC stream, exit code handling, credential injection) with minimal moving parts. Warm pools add template lifecycle, readiness waiting, and `--max-burst` configuration complexity. Once cold dispatch works, warm pools are an additive change to the provisioning step.

**Option B — Warm pools from day one**

Implement `SandboxWorkloadTemplate` + `ClaimSandbox` in Phase 2. Accepts the additional complexity in exchange for acceptable latency from the start.

**Working position:** Option A. Startup latency is a performance issue, not a correctness issue. Proving the dispatch path is more valuable than optimizing it before it exists.

---

## D4: Container image management — user-driven vs. operator-configured

**Question:** Can a user register a new worker container image through the Syntara UI, and does that trigger pool bootstrapping?

**Option A — User-driven image registration**

The UI has a "Containers" tab where users (or operators) add OCI image references. Syntara handles pool bootstrapping when a new Container is registered. This implies Syntara owns the worker Deployment lifecycle and can create new pools on demand.

**Option B — Operator-configured (current position)**

Built-in container images ship with AO and are pre-registered. Custom images are registered by operators through configuration or API, not through a self-service UI flow. The UI shows registered Containers but does not expose a create/import flow to end users.

**Working position:** Option B for MVP. Option A requires Syntara to manage image pull secrets, Deployment rollout, and pool readiness — significant scope. Custom images are an operator concern.

**PM question:** Is there a customer use case where a non-operator user needs to register a custom container image at runtime without operator involvement?

---

## D5: Execution plane database — shared vs. split

**Question:** Does the Task Executor share Syntara's PostgreSQL instance, or does it have its own?

Note: "separate database" means a separate PostgreSQL *instance*, not a separate schema. A separate schema in the same instance is what both options below use internally — see the `execution_plane` schema in the implementation. The question here is whether that schema lives on the same instance as the rest of Syntara.

**Option A — Shared instance (current position)**

TE tables live in the `execution_plane` schema on the same PostgreSQL instance as Syntara. Temporal Worker writes the work item to `execution_plane.work_items` before calling `POST /schedule`. `/schedule` carries no data — it is a pure wakeup. TE polls the shared database for pending work.

```mermaid
graph LR
    TW["Temporal Worker"]
    TE["Task Executor"]
    PG[("PostgreSQL<br/>(shared instance)")]

    TW -->|"write work item<br/>(incl. activity handle)"| PG
    TW -->|"POST /schedule<br/>(no data)"| TE
    TE -->|"read pending work"| PG
```

Syntara API can also read `execution_plane.*` tables directly for the admin UI — no extra API hop.

**Option B — Separate TE instance**

TE has its own PostgreSQL instance. Temporal Worker cannot write to it directly. The work item must travel to TE via its API before TE can persist it. `POST /schedule` must carry the work item payload (or a separate `POST /submit` endpoint must exist). This changes the handoff protocol.

```mermaid
graph LR
    TW["Temporal Worker"]
    TE["Task Executor"]
    PG_AO[("PostgreSQL<br/>(AO instance)")]
    PG_TE[("PostgreSQL<br/>(TE instance)")]

    TW -->|"POST /submit<br/>(work item + activity handle)"| TE
    TE -->|"persist work item"| PG_TE
    TE -.->|"work complete"| TW
```

A separate instance could be shared with AAP if there is a requirement for AAP to read execution plane data directly at the database level.

**Working position:** Option A. Option B reintroduces a data-in-the-HTTP-call design that complicates idempotency and recovery. Shared instance with schema separation gives strong isolation without the protocol change.

**Open question:** Is there a proposal to give AAP direct database-level access to execution plane data? If yes, that is the only concrete reason to split the instance.

---

## D6: Remote cluster scope — Phase 3 boundary

**Question:** Is multi-cluster execution (targets on remote OpenShift clusters or RHEL) in scope for Phase 1/2, or is it Phase 3?

**Current position:** Phase 1 and 2 are on-cluster only (Syntara and workers on the same cluster). Remote clusters are ANSTRAT-2337 scope.

**What's in scope at each phase:**

| Phase | Topology |
|---|---|
| 1 (MVP) | On-cluster vanilla K8S only |
| 2 | On-cluster OpenShell (cold sandboxes) |
| 3 | Warm pools; remote clusters via Pool Agent or OpenShell; installer integration TBD |

**PM question:** Do customers expect to register remote clusters from day one (Phase 1), or is on-cluster MVP sufficient for initial release?

---

## PM Feed-In Questions

These require PM input to resolve — engineering cannot answer them from architecture alone.

| # | Question | Why it matters |
|---|---|---|
| PM-1 | Is on-cluster-only MVP sufficient, or do customers expect remote clusters on day 1? | Determines Phase 1 scope and Pool Agent priority |
| PM-2 | Does AO need to install OpenShell, or is operator day-2 connection acceptable? | Determines whether installer-scope work is needed for Phase 2 |
| PM-3 | Is there a use case where non-operators register custom container images at runtime? | Determines whether self-service image registration is in scope |
| PM-4 | Is there a requirement to share execution plane data with AAP directly (database-level)? | Determines whether a separate execution plane database is needed |
| PM-5 | What customer data is available on which workflow activity types are most used? | Drives prioritization of which worker containers ship first |
| PM-6 | Is startup latency for OpenShell cold sandboxes acceptable for Phase 2, or is warm-pool performance a launch requirement? | Determines whether warm pools must be in Phase 2 |
