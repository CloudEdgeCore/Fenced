# DevOps Agent Workflow (Reference Workload)

A second reference workload proving the Fenced kernel is generic (design
plan §5, Phase 4): the same kernel that runs the multi-agent research
workflow runs a **DevOps incident workflow** with human approval and
rollback — no kernel changes, only a new application layer.

```
User → planner → observe → diagnose → execute (requires human approval)
      → verify → rollback (conditional) → finish
```

## Roles

| Agent | Runtime class | What it does |
|---|---|---|
| `devops-planner` | reasoning | decompose the incident goal into checks |
| `devops-observer` | network | `kubernetes.get` / `kubernetes.logs` → detect the anomaly |
| `devops-diagnoser` | reasoning | root-cause diagnosis + fix plan |
| `devops-executor` | network | `kubernetes.restart` — the high-risk step, gated by **workflow-level approval** (`requiresApproval`) |
| `devops-verifier` | network | `kubernetes.get` again → `HEALTHY` or `ROLLBACK_REQUIRED` |
| `devops-rollback` | network | `kubernetes.scale` to restore — runs only when the verify condition matches |

The fake cluster (`tools/cluster`) starts with the `checkout` service
unhealthy (OOMKilled pod); `restart` heals it. A **stubborn** cluster mode
makes restart fail, driving the verify → rollback path.

## Layout

```
agents/                   6 AgentVersion manifests
workflow/devops-workflow.json   template (approval + conditional rollback)
runtime/                  all six roles behind one adapter-http endpoint
tools/cluster/            deterministic fake-cluster webhook (k8s tools + server.exec)
tests/e2e/                harness + acceptance suite (build tag integration)
```

## Acceptance tests

| Test | Proves |
|---|---|
| `TestDevOpsWorkflowFull` | observe → diagnose → approval → execute → verify HEALTHY → rollback skipped; cluster healed |
| `TestDevOpsWorkflowApprovalRejected` | rejecting the approval SKIPs execute (`APPROVAL_REJECTED`), downstream skips, cluster untouched |
| `TestDevOpsWorkflowRollback` | stubborn cluster → verify outputs `ROLLBACK_REQUIRED` → rollback runs |
| `TestThirdPartyAgentOnboarding` | Phase 6: an opaque third-party agent (`hello-agent`) publishes and runs as a plain task with platform-provided scheduling/capability/budget/audit |
| `TestThirdPartyAgentRejectsUnknownTool` | the capability boundary restricts the third-party agent to its declared tool grant |

Run with PostgreSQL at `FENCED_TEST_DATABASE_URL`:

```bash
go test -tags=integration -count=1 ./examples/devops-workflow/tests/e2e/
```

## Third-party agent (`examples/third-party/hello-agent`)

The hello agent demonstrates the "Existing Agent -> Manifest -> Adapter -> Publish ->
Run" flow: its author wrote only `runtime.go` (call one tool, return the
result). The platform supplies scheduling, capability, budget, isolation,
recovery and audit — the agent never sees leases, fencing tokens, schedulers,
pools, or budget ledgers.
