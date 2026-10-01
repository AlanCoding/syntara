# Follow-up: dispatch via a Kubernetes **Job** instead of a bare Pod

Status: **future work / evaluation.** Not implemented. Requested by product (Ron):
use Kubernetes *Jobs* for cold-start dispatch rather than bare Pods.

This documents what changes, why it's a real improvement (especially the cleanup
point that's easy to half-remember), the one non-obvious implementation wrinkle,
and a ready-to-execute plan. Scoped as its own story.

---

## TL;DR

Switching from a bare Pod to a Job is a **small change with a real, product-facing
benefit: lifecycle robustness.** A Job's `ttlSecondsAfterFinished` makes the
**Kubernetes control plane** guarantee cleanup of the finished workload — so
completed pods are reaped **even if our EP worker crashes, restarts, or loses the
cluster** between dispatch and reap. Today we create a bare Pod
(`transport.run_pod` → `CoreV1Api.create_namespaced_pod`, spec from `pod_body`) and
reap it ourselves in a `finally`; that cleanup is only as reliable as our process.
This is the concrete thing to push on.

The gRPC invocation and the security-hardened pod spec are unchanged. **One thing
does get more expensive:** a Job generates its pod's name, so reaching the pod's
gRPC port costs an **extra Kubernetes API call per dispatch** (discover the pod by
label, then port-forward). That tradeoff is real and is a direct consequence of
the Job requirement — see ["Reaching the pod's port under a Job"](#reaching-the-pods-port-under-a-job).

---

## The benefit: lifecycle robustness, broken down

This is the part worth getting precise, because it's the actual reason to do it —
and it's a genuine robustness improvement, not cosmetic. **With a Job, cleanup of
finished work is guaranteed by the cluster itself and survives any failure of our
EP worker.** Here is exactly why that's true and why the bare-Pod status quo
can't offer it.

**A bare Pod is not garbage-collected when it finishes.** Our pod runs with
`restartPolicy: Never`; when the process exits, the Pod enters `Succeeded` or
`Failed` and **stays there**. Kubernetes does *not* promptly delete terminated
pods — the pod-GC controller only prunes terminated pods once a *cluster-wide* cap
(`--terminated-pod-gc-threshold`, default 12500) is exceeded. So in practice a
completed pod lingers in the namespace indefinitely, holding an API object and
counting against pod quota.

**So something has to delete it.** Today that's us: `run_pod` calls
`delete_namespaced_pod` in its `finally`. AWX does the equivalent — it reaps
**client-side** (its `administrative_workunit_reaper` + receptor `work release`).
This is exactly the weakness you half-remembered: because cleanup depends on the
*controller* staying alive and tracking the pod, a crash/restart/partition between
"pod finished" and "controller reaps it" **leaves orphaned pods behind in a
not-running (Completed/Error) state**. They accumulate until someone notices. We
have the *same* leak window AWX has (if the EP worker is killed between create and
the `finally`, the pod leaks).

**A Job fixes this by moving cleanup into the control plane.** A Job *owns* its
pod(s) via `ownerReferences`, and `spec.ttlSecondsAfterFinished: N` tells the
built-in **TTL-after-finished controller** to delete the Job `N` seconds after it
Completes/Fails — which cascades to the owned pod. This happens **server-side,
with no dependency on our worker being up**. That's the legitimate improvement
over AWX's client-side reaping: cleanup is guaranteed by the cluster, not by our
process.

**One clarification on "old images".** Images are a separate matter — the kubelet
caches pulled images and reclaims them on its own disk-pressure schedule
(`imageGCHighThresholdPercent`), the same whether you use a Pod or a Job. Jobs do
**not** change image lifecycle; the improvement is purely about not leaking
*terminated pod objects*.

**Note for our model specifically:** we read the result over **gRPC**, not from the
pod's exit status, so we don't need the Job's success/failure tracking for
*correctness* — the win here is lifecycle/GC robustness, not result semantics. We
keep our explicit delete for prompt cleanup and let the Job TTL be the safety net
for the crash window.

---

## What changes (and what doesn't)

**Unchanged:** the gRPC invocation (`invoke` over the port-forward), the
security-hardened container spec (`pod_body`: read-only root FS, dropped caps,
`automountServiceAccountToken: false`, memory `tmp`, optional `agent-tls`), image
selection, secrets policy (none in the spec), retry classification.

**Changed:**

1. **Object created.** A `batch/v1` `Job` whose `spec.template` is today's pod
   spec, instead of a bare Pod. Create via `BatchV1Api.create_namespaced_job`.
   Job spec: `backoffLimit: 0` (one attempt — no retry; our requeue logic owns
   retries), `completions: 1`, `parallelism: 1`, `ttlSecondsAfterFinished:
   <small, e.g. 300>`. Keep `activeDeadlineSeconds` on the pod template (hard
   kill) and optionally mirror it on the Job.

2. **Pod discovery — the one real wrinkle, and the one real cost.** A Job
   **generates** its pod name, but port-forward needs a *concrete pod name*, so we
   must discover the pod before we can reach its gRPC port. This is the only part
   of the change that is not a free swap — it adds a Kubernetes API call per
   dispatch. It has its own section below:
   ["Reaching the pod's port under a Job"](#reaching-the-pods-port-under-a-job).

3. **Cleanup.** Delete the **Job** (not the pod) with
   `propagationPolicy: Background` so the pod cascades; `ttlSecondsAfterFinished`
   is the backstop if our delete never runs.

4. **RBAC.** Grant `batch`/`jobs`: `create, get, list, watch, delete`. Keep
   `pods`: `get, list, watch` and `pods/portforward`. `pods delete` is no longer
   required (the Job cascade handles it) but is harmless to keep. Update the
   runbook's Role ([kind-demo-runbook.md](kind-demo-runbook.md) Step 2).

5. **Tests.** `test_vanilla_k8s_transport.py` fakes the client, so no cluster is
   needed. Add a `BatchV1Api` fake with `create_namespaced_job`, make the
   `CoreV1Api` fake's `list_namespaced_pod` return the one Job pod, and reassert
   the lifecycle contract (create Job → discover pod → invoke → delete Job). Keep
   `pod_body` as the (now template) spec builder; add a `job_body` wrapping it.

---

## Reaching the pod's port under a Job

This is the part to be explicit about, because it is the **only** cost of the
change and it is a direct, unavoidable consequence of the Job requirement.

**Why it's different.** Our transport reaches the node over a port-forward, and
`connect_get_namespaced_pod_portforward` requires a **concrete pod name**. Today we
create a bare Pod with a name *we* choose (`syntara-node-<sha256(identity)>`), so we
can forward to it immediately — no lookup. A Job does **not** let us name its pod;
it sets `generateName` and the Job controller appends a random suffix
(`<job>-<rand>`). So under a Job we must **discover the pod's actual name before we
can port-forward to it.** There is no way around this with a Job — the pod name is
the control plane's to assign, by design.

**The mechanism (concrete).** Fold discovery into the readiness poll we already
run. Today the loop is:

```
create_namespaced_pod(name=...)                      # 1 create
loop: read_namespaced_pod(name) until phase==Running # get-by-name, N times
```

Under a Job it becomes:

```
create_namespaced_job(name=...)                                        # 1 create
loop: list_namespaced_pod(label_selector="job-name=<job>")            # list-by-label, N times
      → wait for the Job controller to create the pod (list may be empty at first)
      → take the single item, check phase==Running, capture its real name
port-forward to that captured pod name                                 # unchanged thereafter
```

Everything after we have the pod name — `forward_socket`, the gRPC channel,
`invoke`, the result — is **identical**. Cleanup deletes the **Job**
(`propagationPolicy: Background`) and the pod cascades; `ttlSecondsAfterFinished`
is the backstop.

**The tradeoff, stated plainly (and pinned to the request).** The readiness poll
changes from a `get`-by-name to a **`list`-by-label**, and it gains one new
transient state: a short window right after Job creation where the pod does not
exist yet, so the first list(s) return empty and we keep polling until the Job
controller spawns the pod (bounded by the same `startup` deadline that already
governs "pod did not become ready" → retryable). Net cost per dispatch:

- a **heavier API call** in the poll loop (a label-indexed collection query
  instead of a single-object read), and
- a **slightly longer, more failure-prone path to "Running"** (the Job
  controller's pod-creation step is now inside our critical path).

This is modest, but it is **not free**, and it buys us nothing on the execution
path itself — it is purely the access cost of letting the control plane own the pod
name. **It exists solely because Jobs were requested (by Ron, Product).** Flagging
it here so the cost is attributed to that decision rather than showing up later as
an unexplained tax on dispatch latency/API load. If dispatch throughput becomes a
concern, the list-poll can be upgraded to a single pod `watch` scoped to the
label selector (one streaming call instead of repeated lists) — note it as an
optimization, not a requirement.

> Alternative considered and rejected: create the Pod ourselves (named) *and* a
> Job that adopts it. That reintroduces client-managed pod naming but forfeits the
> whole point — the TTL/ownership cleanup guarantee only applies to pods the Job
> controller created and owns. Not worth it.

## Implementation sketch

```python
# new: wrap the hardened pod spec as a Job template
def job_body(name, image, invocation, *, startup, grace, tls_secret=None):
    pod = pod_body(name, image, invocation, startup=startup, grace=grace, tls_secret=tls_secret)
    pod["metadata"].pop("name", None)          # Job generates the pod name
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "labels": pod["metadata"]["labels"]},
        "spec": {
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "ttlSecondsAfterFinished": JOB_TTL_SECONDS,
            "template": {"metadata": pod["metadata"], "spec": pod["spec"]},
        },
    }
```

`run_pod` then: `BatchV1Api.create_namespaced_job(...)` → poll
`CoreV1Api.list_namespaced_pod(namespace, label_selector="job-name="+name)` for a
single `Running` pod → port-forward to that pod's name (rest identical) →
`finally`: `BatchV1Api.delete_namespaced_job(name, propagation_policy="Background")`.

Minor rename consideration: `run_pod` becomes a Job runner, but the public shape
(args, `TransportError` contract) is unchanged, so the manager need not change.

---

## Recommendation

Low-risk, clear upside; I'd take it. The payoff — cluster-guaranteed cleanup that
survives an EP-worker crash — directly fixes the orphaned-terminated-pod weakness
that AWX's client-side reaping has (and that our current `finally`-based reap
shares).

I did **not** implement it in the same pass as the logging/doc work because it
can't be verified end-to-end without rebuilding the EP worker image (declined for
now), and it touches the demo-critical transport right before a recording — a
Path-B (rebuild-from-scratch) recording would otherwise exercise unverified code.
The change is well-scoped and unit-testable against the faked client; say the word
and I'll implement it together with a kind verify pass.
