# Multi-Agent Research Workflow (Reference Application)

A complete, runnable Fenced-native implementation of the multi-agent deep
research workflow from the project's internal design doc (not published in
this repository; this README is the complete behavioral specification).
Seven agent roles, dynamic spawn fan-out, Evidence Memory, critic-driven
second-round research, budget governance, citation validation, and
failure-injection recovery — every layer running on the real kernel stack
(gateway, brokered MCP, admission/scheduling/recovery/workflow controllers,
fenced runtime adapter workers), no mocks inside the pipeline.

## Layout

```
agents/                  8 AgentVersion manifests (7 roles; collector is the
                         routing hop described below)
workflow/research-workflow.json   static workflow template + budgets + dynamic limits
workflow/schemas/        planner / evidence / analysis / critic / report JSON schemas
runtime/                 the Go runtime implementing all roles behind one
                         fenced.adapter-http/v1 endpoint (protocol.go, roles.go)
app/                     application layer (design doc §3): domain objects (§5),
                         the §9 state machine, the Control API client, the
                         report renderer + artifact store, and the §13 REST API
                         (app/api, app/repository, app/report, app/domain,
                         app/cmd/research-api)
tools/webtools/          web.search / web.fetch / citation.check webhook tool
                         endpoint (offline deterministic corpus, SSRF-guarded
                         fetch, deterministic citation grading)
config/                  deployment configuration examples
scripts/                 bootstrap / publish / run / inject-failure / cleanup
tests/e2e/               end-to-end scenario suite (build tag `integration`)
```

## Architecture

```
                    Control API (fenced workflow create)
                                   │
                     workflow/research-workflow.json
                                   ▼
        ┌────────────── kernel controllers (25ms reconcile) ──────────────┐
        │ admission → scheduling → workflow orchestration → recovery      │
        └──────────────┬──────────────────────────────────────────────────┘
                       │ fenced assignments (lease + fencing token)
                       ▼
      runtime adapter workers ── MCP over HTTP (execution-window registry)
                       │                │
                       ▼                ├── fenced.model.invoke  → Model Gateway → provider
      research runtime (roles.go) ◄─────┤ fenced.memory.put/search → Memory Gateway (Evidence Memory)
                       │                ├── fenced.tool.invoke web.search/fetch → Tool Gateway → webtools
                       └─ fenced.task.spawn ────► WorkflowSpawnService (dynamic steps)
```

### Roles and data flow

1. **planner** decomposes the goal into ≥3 research questions (`rq-*`) and
   spawns one **search** child per question (`spawn:planner` join group).
2. **search** runs corpus queries through `web.search`, persists discovered
   sources to the `sources` Evidence-Memory namespace AND seeds snippet-level
   evidence bundles so round-1 analysis has material before readers finish.
3. **collector** drains every source from its dependency group plus the
   sources namespace, deduplicates by normalized URL, skips sources already
   read (`read-<sourceId>` markers), and spawns one **reader** per unread
   source — capped at 8 per drain (see deviation below), surplus deferred.
4. **reader** fetches the full document via `web.fetch` (SSRF-guarded),
   extracts 2-6 verbatim-evidenced claims via the reader model tier, and
   GROUNDS them before persisting: each claim's evidence must appear verbatim
   (whitespace-normalized) in the fetched source text, else the claim is
   rejected; survivors are stamped with `sourceHash` (sha256 of the source)
   and `grounded:true`, then written to `ev-<sourceId>` + read marker. Zero
   grounded claims is a hard error (the attempt retries).
5. **analyst** synthesizes findings/contradictions/unknowns from evidence,
   citing claim ids; a carried PASS verdict replays the stored analysis.
6. **critic** scores the analysis with THREE terminal states:
   `PASS`; `NEEDS_MORE_RESEARCH` (gaps spawn round-N+1 **search** children
   via `spawn:critic-rN` join groups); or — from round three on, never
   forced into PASS — `INSUFFICIENT_EVIDENCE`.
7. **writer** renders the report strictly from findings+evidence with
   verbatim quotes, declaring `"insufficientEvidence":true` when the final
   verdict demands it; **citation-validator** grades coverage under STRICT
   grounding rules (≥0.90, zero unsupported), requests revisions with
   retry-scoped feedback, and ships an honest verdict after 2 failed
   revisions.

### Provenance chain: Statement → Marker → Citation → Evidence → Source

Citation validation is as strict as extraction: a citation counts as
supported ONLY when its evidenceId names an existing claim AND its quote is
a non-empty whitespace-normalized substring of THAT claim's evidence AND
its marker actually appears in the report body, with each body marker
binding at most one citation. Empty quotes, unknown claim ids, passages
borrowed from different evidence, citations never referenced in the body,
and dangling body markers with no citation object are all rejected — the
citations array alone cannot manufacture coverage. The canonicalizer closes
the chain deterministically (it rewrites quotes to verified evidence text
and appends any missing markers as a `References:` line), and the validator
audits the completed chain; revisions carry per-marker feedback. Combined
with reader-side grounding, every accepted citation traces to verified
source text end to end.

### Documented deviation: the collector role

The draft plan has search results flow straight into per-source readers.
The kernel's spawn-group join covers DIRECT children of the spawning step
only, so a search wave cannot itself be joined by later steps once it fans
out again. The collector exists as that direct-child boundary: it turns each
finished search wave into ONE bounded, joinable reader wave
(`maxChildrenPerStep=8` respected locally with deferral instead of failing
the step — a failed parent leaves its join group permanently open). This is
the same shape production pipelines use as a "fan-in/fan-out router" step.

### Governance surfaces exercised

- **Budgets**: per-role task budgets in dollars (`costUsd`), workflow cap
  (maxTasks/maxTokens/maxCostUsd), dynamic-spawn ceilings
  (maxDynamicSteps/maxChildrenPerStep/maxSpawnTokens...). Exhaustion stops
  cleanly (`TestResearchWorkflowBudgetStop`).
- **Retries**: multi-attempt tasks requeue for a fresh dispatch on clean
  failure (`retryPolicy.maxAttempts`); lease-expiry recovery places attempt
  N+1 inside the original run; both paths are covered by kernel integration
  tests and the e2e failure scenarios.
- **Fencing**: every tool/model/memory call carries attempt identity +
  fencing token; stale writers are rejected at the gateway.
- **Idempotency**: spawn replays return `replayed`; checkpoints, completions
  and model calls are idempotent-keyed.

## Application API (§13)

The application layer exposes the research REST API from design doc §13. It
composes the workflow document (envelope substitution + user budget override),
submits it through the Control API v1, and materializes the §5 domain state
from observable kernel surfaces (workflow + memory namespaces) — it never
touches kernel internals or the database directly. Start it with
`app/cmd/research-api` (bootstrap.sh does this for you on `127.0.0.1:9095`):

```bash
go run ./examples/research-workflow/app/cmd/research-api \
  -control-endpoint http://127.0.0.1:9092 \
  -listen 127.0.0.1:9095 \
  -workflow-template examples/research-workflow/workflow/research-workflow.json \
  -artifact-root tmp/research-artifacts
```

| Endpoint | Description |
|---|---|
| `POST /research` | `{"goal":"...","budget":{"maxTasks":80,"maxTokens":250000}}` → `202 {researchId, workflowId, status:"CREATED"}` |
| `GET /research/{id}` | materialized run: §5 domain objects + `criticVerdict` + statistics |
| `GET /research/{id}/report` | final report: `{report:{id,researchRunId,artifactRef,citationCoverage}, markdown}` (`409 REPORT_NOT_READY` until COMPLETED) |
| `POST /research/{id}/cancel` | maps to the kernel workflow cancel (CAS `If-Match`) |

The `researchId` is `research-<namespaceKey>`; the server persists the
namespace-key → kernel-workflow-id mapping under the artifact root so it
survives restarts. Reports are stored as content-addressed artifacts
(`artifact://…` URIs) with the Markdown deliverable and the citation verdict.

## CLI: `fenced research` (§17)

The `research` subcommand drives the §17 showcase through the app API with a
live timeline and a final statistics block:

```bash
go run ./cmd/fenced research --endpoint http://127.0.0.1:9095 \
  --goal "Analyze the next three years of agent runtime infrastructure evolution" --max-tokens 2000000

[00:00] Research created research-… (workflow …)
[00:02] Planner running
[00:06] 6 research questions created
[00:07] Research in progress
[00:20] 28 sources discovered
[00:45] 137 evidence records extracted
[00:50] Analyst running
[00:54] Critic: PASS
[01:21] Writer running
[01:39] Citation validator running
[01:42] Research completed
[01:42] Report ready (artifact artifact://research-tenant/sha256/…)

Statistics:
Workflow Tasks     37
AgentVersions       8
Sources            31
Evidence          152
...
Citation Coverage  95%
```

Exits non-zero when the run ends in a non-COMPLETED state.

## Running locally

```bash
# 1. Infrastructure (Postgres required)
export DATABASE_URL='postgres://fenced:...@127.0.0.1:5432/fenced?sslmode=disable'
export ADAPTER_ENDPOINT='http://127.0.0.1:8090'   # research runtime HTTP endpoint
examples/research-workflow/scripts/bootstrap.sh

# 2. Publish the eight agents
MODEL_REF=openai/gpt-4o-mini \
  examples/research-workflow/scripts/publish-agents.sh

# 3. Run a research workflow end to end
examples/research-workflow/scripts/run-research.sh \
  "Three-year outlook on agent runtime infrastructure"

# Fault injection (crash a worker mid-flight; recovery finishes the work)
examples/research-workflow/scripts/inject-failure.sh crash research-worker-00

# Tear down processes (data preserved)
examples/research-workflow/scripts/cleanup.sh
```

The example runtime (`runtime/cmd`) serves all roles on one endpoint;
`config/runtime-bindings.json` maps each published ref to it.

## Live mode: real models and real web (roadmap P0/P1/P2)

The deterministic stack stays the CI default. Two independent switches
upgrade fidelity without touching the agents — they keep calling the same
logical tiers (`research-fast` / `research-reader` / `research-reasoning`
manifest placeholders) and the same `web.search@1.0.0` / `web.fetch@1.0.0`
tools:

```bash
# P0 — real model (OpenAI-compatible / vLLM / Qwen / DeepSeek / GLM …)
FENCED_RESEARCH_LIVE=1
FENCED_RESEARCH_MODEL_BASE_URL=https://api.example.com/v1
FENCED_RESEARCH_MODEL_KEY=sk-...
FENCED_RESEARCH_MODEL_PROVIDER=openai            # registry name, default openai
FENCED_RESEARCH_MODEL_FAST=gpt-4o-mini           # wire model per tier (optional)
FENCED_RESEARCH_MODEL_READER=gpt-4o-mini
FENCED_RESEARCH_MODEL_REASONING=gpt-4o

# P1 — real internet
FENCED_RESEARCH_LIVE_WEB=1
FENCED_RESEARCH_SEARCH_PROVIDER=doubao           # doubao | brave | bing
FENCED_RESEARCH_SEARCH_KEY=...
```

Live backends: `webtools.DoubaoSearch` / `webtools.BraveSearch` /
`webtools.BingSearch` provider
adapters behind one `Backend` interface, and `webtools.LiveFetch` — a
hardened fetcher (DNS resolve/validate/IP-pin/dial with mixed-address
rejection, SSRF policy incl. per-hop redirect re-checks, ≤5 hops, 10 MiB
cap, 20 s timeout, content-type allowlist, HTML→readable-text, final URL +
stable source id). Proxy and alternate TLS dial hooks are disabled at this
boundary so they cannot bypass DNS pinning. The deterministic corpus remains
an in-process fallback.
The Doubao adapter follows the
[official Search Custom API](https://docs.volcengine.com/docs/87772/2272953?lang=zh):
Bearer API-key authentication, the documented `Query` / `SearchType` /
`Count` / `Filter` envelope, URL-bearing web results only, and a client-side
4 QPS limiter below the default 5 QPS quota.

Gated acceptance tests (skip unless their env is present):

| Test | Gate | Asserts |
|---|---|---|
| `TestResearchWorkflowLiveModel` | `FENCED_RESEARCH_LIVE=1` | SUCCEEDED, coverage ≥ 0.90, zero unsupported, all evidence grounded; prints §5 metrics JSON |
| `TestResearchWorkflowLiveFull` | + `LIVE_WEB=1`, `…_GOAL="…"` | + grounded rate = 100 %, unique domains ≥ 3, no INSUFFICIENT_EVIDENCE |
| `TestResearchWorkflowLiveRecovery` | same as LiveFull | + kills one active Reader worker, expires/fences its lease, requires a recovered Attempt and final SUCCEEDED |

Metrics (workflowId, questions, sources, uniqueDomains, evidenceCount,
groundedEvidenceRate, citationCoverage, unsupportedCitations, criticRounds,
modelCalls/failures, toolCalls, recoveredAttempts, tokens, costUsd,
duration) are aggregated from the durable store and logged with the run —
identical shape for deterministic and live executions. Every live acceptance
also writes a credential-scanned JSON evidence document containing the exact
commit SHA and all metrics. The default output directory is
`artifacts/research-live/` (generated files are git-ignored); override it with
`FENCED_RESEARCH_EVIDENCE_DIR` when a CI job will upload the documents as PR
or release artifacts.

## End-to-end test suite

Requires PostgreSQL at `FENCED_TEST_DATABASE_URL`. Each scenario gets its
own schema and a full in-process kernel stack:

```bash
export FENCED_TEST_DATABASE_URL='postgres://...'
go test -tags integration -count=1 -timeout 12m \
  -run '^TestResearchWorkflow' ./examples/research-workflow/tests/e2e/
```

| Scenario | Proves |
|---|---|
| `Basic` | goal → ≥3 questions → ≥10 sources → ≥30 claims → report |
| `EvidenceGrounding` | every persisted claim grounded + sourceHash-stamped; nothing ungrounded reaches memory |
| `CriticRetry` | round-1 NEEDS_MORE_RESEARCH spawns gap searches/readers; final PASS |
| `InsufficientEvidence` | round-3 non-PASS becomes INSUFFICIENT_EVIDENCE (never forced PASS); writer+validator declare the shortfall |
| `CitationCoverage` | fabricated quotes trigger writer revision; gate ≥0.90 enforced honestly |
| `InvalidCitation` | persistent unknown-evidence citations never pass the hardened validator; honest best-effort ships |
| `ToolFailureRecovery` | injected `web.fetch` 500s absorbed by attempt retries, no duplicate side effects |
| `ReaderModelRetry` | Reader provider failure creates a failed Attempt and a new Run/Attempt without prematurely writing the read marker |
| `ModelFailure` | provider 429 absorbed by bounded provider retry |
| `Recovery` | SIGKILL-equivalent of both workers mid-run; lease expiry + recovery + restarted instances complete the workflow |
| `BudgetStop` | undersized budget settles instead of running unbounded |
| `LiveModel` / `LiveFull` / `LiveRecovery` (env-gated) | real-model, full live-internet, and live worker-recovery acceptance with durable §5 evidence (see Live mode above) |
| `100Concurrent` (gate `FENCED_RESEARCH_SCALE=1`) | 100 simultaneous workflows all reach SUCCEEDED |
| `AppAPIAndReport` | the §13 application API over real HTTP: create → COMPLETED → report artifact with coverage ≥ 0.90 |
| `AppAPICancel` | §13 cancel path: a running research run settles CANCELLED |
| `TaskSSEDisconnect` | §14-P4 SSE disconnect: a client that drops mid-stream reconnects and still receives `task.terminal` |
| `1000Runs` (gate `FENCED_RESEARCH_SCALE_1000=1`, count via `FENCED_E2E_RESEARCH_RUNS`) | 1000 total ResearchRuns all settle SUCCEEDED (the §16 scale gate) |
| `Soak` (gate `FENCED_RESEARCH_SOAK=1`, duration via `FENCED_E2E_SOAK_MINUTES`, default 10; 1440 = 24h) | continuous runs with 100% completion and zero residual capacity reservations (§18 soak) |
| `MultiRuntimeRolePlacement` | every role runs on its mapped runtime class (reasoning / network / sandbox) with the workflow declaring only the class set |
| `MultiRuntimeMigration` | the same workflow document runs readers on the surviving sandbox pool when the original pool is cordoned |
| `MultiRuntimeCapacityExhaustion` | capacity-exhausted sandbox pool → placement walks candidates to the other sandbox pool |
| `MultiRuntimeWorkerRecoveryReplacement` | sandbox worker crash + lease expiry → requeued attempts complete on the restarted worker; workflow SUCCEEDs |

Kernel-level regression tests for the retry semantics live in
`internal/kernel/store/postgres/runtime_requeue_integration_test.go`
(requeue on clean failure, exhausted-budget finalization) alongside the
existing checkpoint/recovery tests.

## Configuration reference

See `config/`: tenant policies (`research-policy.json` — the §3 name; bare
tool names, since the Rego policy matches `input.tool.name`), model providers
(API keys referenced by env var name only), tool endpoints (immutable version
→ HTTPS URL; `citation.check@1.0.0` included), runtime pools (four pools per
the multi-runtime plan: `reasoning-pool` / `network-pool` / `sandbox-pool` /
`remote-pool`), and runtime bindings (published ref → runtime endpoint).

## Multi-runtime placement (next-phase plan §2)

The research workflow runs as a heterogeneous multi-runtime workload: each
role declares its runtime class in the AgentVersion manifest
(`runtimes[].class` + `runtimeClassPolicy.allowed`), the workflow document
declares the class set it permits, and the kernel admission → scheduler chain
places every task onto the matching pool:

| Role | Class | Pool |
|---|---|---|
| planner / analyst / critic / writer / citation-validator | `research-reasoning` | `reasoning-pool` |
| search / collector | `research-network` | `network-pool` |
| reader | `research-sandbox` | `sandbox-pool` |
| external specialist (planned) | `research-remote` | `remote-pool` |

Static steps narrow their placement via the per-step `spec.placement` overlay
(merged field-wise over the workflow default placement); dynamic children
(search / reader) carry their own placement overlay in the spawn `spec`, so a
reader spawned by the network-class collector still lands on the sandbox
class. Cordon/drain a pool and the same workflow document keeps running on
the surviving pool of the same class (see the migration acceptance below).

Lease-expiry recovery is pool-aware: when the expired attempt's pool is
cordoned, draining, or no longer ready, the kernel reopens the task for
scheduling instead of requeueing onto the dead pool, so the scheduler
re-places it onto a live pool of the same runtime class (the multi-runtime
re-placement fix in `RecoverExpiredAttempt`).

## Failure injection (§15)

`scripts/inject-failure.sh` supports every §15 scenario against the bootstrap
stack: `kill-worker` / `kill-reader` (SIGKILL a runtime adapter; lease-expiry
recovery requeues its attempts), `runtime-disconnect` (SIGSTOP — connections
drop without releasing leases), `model-429` (SIGSTOP the local model provider
for N seconds so bounded retries absorb the stall), `tool-500` (SIGSTOP or
kill the webtools webhook), `sse-reset` (SIGSTOP/SIGCONT the Control API so
event streams stall and clients reconnect + reconcile), plus the operator
`cordon` / `uncordon` pool controls. Each injection prints the §15
observation chain (`failure injected → attempt failed → recovery → new
attempt → workflow continued`) to verify against `fenced research` or the
e2e recovery scenarios. The same scenarios are exercised deterministically
in-process by the failure-injection e2e tests (`ToolFailureRecovery`,
`ModelFailure`, `Recovery`, `ReaderModelRetry`).
