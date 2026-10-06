# Changelog

All notable changes use [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
categories. Fenced product version `X.Y.Z.0` maps to SemVer tag `vX.Y.Z`;
public protocol versions evolve independently.

## Unreleased

### Changed
- The public README uses the Fenced brand. The repository, CLI binaries, SDK
  package names, Go module path, and protocol identifiers were renamed from
  `AgentOS` / `agentos` to `Fenced` / `fenced`; see `docs/NAMING.md` for the
  migration map and the compatibility consequences. A minimal Runtime Interface
  trial is shown first; full setup and validation commands are retained in
  `docs/development.md` with explicit evidence limits.
- The developer interface is CLI-only: `agent ui` no longer starts for external access and is limited to the loopback-bound internal preview (`agent ui --preview`); `agent config wizard` is the supported interactive configuration path.
- AgentService replicas execute through durable Tasks and the existing admission,
  scheduler, and Runtime Protocol. Service creation requires a published version
  reference and workload specification (`fenced service create -spec ...`).
- The Helm chart requires production HTTPS/OIDC configuration and existing
  database, TLS, audit-signing, and package-trust Secrets. See its deployment guide
  for migration from the earlier skeleton chart.

### Fixed
- Persist service execution and drain state in migration `000037`, serialize API
  mutations and controller reconciliation per service, and enumerate all tenants
  in the internal controller. Legacy state-only readiness is reset during upgrade;
  legacy services need valid launch configuration before execution can begin.
- Derive service readiness from a live runtime attempt and lease, wait for Task
  termination before restarting, respect restart limits, and prevent retired
  version backoffs from creating surplus rollout tasks.
- Service CLI commands send `FENCED_TOKEN` for authenticated deployments;
  `service stop` requests cancellation and disables autowake through the new
  `POST /v1/services/{id}/stop` endpoint.

### Added
- Service-to-reference-worker PostgreSQL integration tests for actual completion
  and cancellation, and Helm rendering checks for production configuration.

## 1.3.0 - 2026-10-01

### Added
- **Visual Design Overhaul ("Cyber Obsidian & Electric Lumina")**: Modernized console with deep obsidian surfaces, electric indigo, neon cyan, and emerald luminous highlights, glassmorphic cards, and custom cyber scrollbars.
- **3-Palette Live Theme Switcher**: Real-time theme engine supporting Cyber Obsidian (Default), Solar Amber (Industrial), and Quantum Emerald (Matrix) with localStorage persistence and instant DOM class synchronization.
- **Custom CloudEdge Vector Brand Mark**: Bespoke vector SVG logo created via Stitch incorporating Cloud elasticity, Edge crystallographic polygonal facets, and circuit bus routes into the header.
- **Dual-Protocol Model Gateway**: Native dual-protocol switching supporting both OpenAI-compatible (/chat/completions) and Anthropic Claude (/v1/messages) APIs across all runtime providers.
- **Streamlined 4-Parameter Gateway Configuration**: Simplified model configuration removing redundant provider type dropdown in favor of core parameters (Model, Protocol, Base URL, API Key) and 1-click Preset chips.
- **Full Bilingual i18n Engine**: Seamless live switching between English and Simplified Chinese across the entire dashboard and controls.
- **Web-Based Process & Agent Orchestrator**: Interactive graphical workbench for testing model connectivity, probing MCP tool endpoints, and hot-reloading configurations.

## 1.2.1 - 2026-10-01

### Added
- **Unified `agent` CLI (`cmd/agent`)**: Introduced developer-first `agent` command replacing `fenced` as the primary CLI entrypoint with backward compatibility fallback.
- **Hierarchical YAML Configuration (`agent.yaml`)**: Multi-level cascading configuration system with 5-tier precedence (Builtin Defaults < Global `~/.agent/agent.yaml` < Project `./agent.yaml` < Environment Profiles < Environment Variables).
- **Multi-Provider LLM Matrix**: Native support for OpenRouter, DeepSeek, Qwen (Aliyun), and local Ollama with instant provider switching (`agent config set llm.default_provider <name>`).
- **CoT Streaming & Zero-Timeout Engine**: Native dual-channel stream parser separating `delta.reasoning` (thought stream) and `delta.content` (structured result), eliminating `context deadline exceeded` timeouts on multi-minute reasoning models.
- **Agent Scaffolding (`agent init`)**: One-command generator creating decoupled `agent.yaml`, `agent.manifest.json`, `prompt.md`, and custom tool microservices.
- **PRD Industrial Verification Suite (`agent demo`)**: Built-in scenarios for equipment fault diagnosis (CNC-03 E102) and quality defect root-cause tracing (Product-A Pareto & case-based reasoning).
- **Automated Masking & Precedence Auditing**: `agent config` and `agent config path` for transparent configuration inspection and credential masking.


### Added

- Public evidence layer under `docs/evidence/`: 100k/baseline benchmark records, the
  database consistency contract, an isolation-drill delivery report, and a
  multi-runtime takeover evidence report (cordon-migration and kill+lease-expiry
  takeover across runtimes, 4/4 scenarios). Internal plans stay untracked under
  `docs/internal/`.
- README section documenting non-preemptible execution semantics: cancellation is
  cooperative (`cancel_requested` + `AcknowledgeCancellation`), the only forceful
  path is lease expiry + fencing takeover, and cross-runtime recovery is
  checkpoint-based.

### Changed

- Self-hosted evidence jobs (nightly soak matrix, 1M capacity baseline, Firecracker
  KVM) are gated behind explicit runner-preflight repository variables; when runners
  are not provisioned the jobs skip immediately with a notice instead of queueing
  indefinitely.
- README states the evidence gaps explicitly instead of presenting scheduled jobs as
  if they produced evidence: performance stability is not certified (the 100K pipeline
  is verified for correctness, but the ≤10% throughput / ≤15% P95 spread targets assume
  fixed hardware and were not met on shared runners), the 1M capacity baseline has never
  run, and the 24h/72h/7d soak and KVM jobs skip because no self-hosted runner is
  provisioned.
- The TypeScript SDK checks in CI and in the README run from `sdk/typescript` in a `cd`
  subshell instead of `npm --prefix`, matching the release workflow. npm 11 (Node 24)
  resolves the project root from the current directory when no `package.json` is present
  there, so `--prefix` failed with ENOENT in the release pipeline.

### Security

- Bump `google.golang.org/grpc` from 1.83.0 to 1.83.2 (xDS server DoS via missing
  `:authority`/`Host` headers; heap memory exhaustion via HTTP/2 DATA frame
  fragmentation; xDS RBAC header-matching bypass).
- Bump `setuptools` from 80.9.0 to 83.0.0 in `/sdk/python` (sdist MANIFEST.in
  exclusion bypass).

### Fixed

- TypeScript SDK version aligned with the product version: `sdk/typescript/package.json`
  and its lockfile now declare `1.1.0`, so `npm pack` produces `fenced-sdk-1.1.0.tgz`
  instead of `fenced-sdk-1.0.0.tgz`.
- The single-agent acceptance e2e settled-usage invariant credited only succeeded
  tasks' first rounds, but failed tasks' completed first round is settled by design;
  the invariant now requires each SUCCEEDED task to settle exactly the per-task
  amount plus global lower/upper bounds, which still catches double settlement.

### Added

- Tokenizer plugin registry: deployments can register exact per-provider tokenizers (`tokens.Register`) under provider-config names; built-in `heuristic`/`conservative` names are reserved and duplicates are rejected. The conservative-reservation-envelope + exact-provider-settlement product semantics stay unchanged.
- Public error-code dispositions: every stable error code in the canonical registry now carries exactly one public class (`retryable`, `terminal`, `user-action-required`, `operator-action-required`) so callers can pick a retry policy without parsing messages.
- Workflow budget reservations close the spawn-time commitment loop: declaring or dynamically spawning a step reserves its future Task's token/cost ceiling and task slot on the workflow usage ledger in the same transaction, transfers to the task's own budget ledger at admission, and is released on skip, cancellation, rejection, or terminal tasks without a ledger. Concurrent spawns can no longer collectively promise past a workflow budget, retries re-reserve under the same guard, and the ledger reconciles against per-step reservations after crashes.
- Runtime pool operator grants: cordon/drain/activate now requires a separate `runtime_pool_operator_grants` row for the requesting subject; tenant usage grants no longer imply operator authority on shared pools, and status changes record the deciding operator subject in the audit chain.
- Runtime bindings decouple deployment endpoints from immutable AgentVersions: `fenced init` writes an environment-independent `fenced-binding://<name>/remote` entrypoint by default, and `fenced-runtime-adapter -runtime-bindings` maps version refs (or `name@*` wildcards) to concrete Runtime Interface endpoints. Unresolved logical entrypoints fail closed.
- Runtime Interface streaming extension: `GET /executions/{id}/events/stream` serves one long-lived SSE connection that pushes events after the `after` cursor and terminates with a result frame. The Go and Python hosts implement it, the Go client consumes it with automatic fallback to v1 polling for runtimes without the route, and the adapter worker uses streaming-first observation with mid-flight fallback.
- Tokenizer-aware token estimation: provider configs accept a `tokenizer` field (`heuristic` by default, `conservative` for uncharacterized tokenizers). The estimator is script-aware (dense CJK/kana/hangul text no longer under-reserves against bytes/4), never estimates below the legacy floor, and provider-reported usage remains the authoritative settlement.
- A durable runtime-pool registry with tenant grants.
- Workflow CLI operations and one-command Runtime Interface conformance.
- A typed TypeScript Control API and Runtime Interface SDK.
- Python RuntimeHost watchdogs, bounded concurrency and payloads, unit tests, and PEP 561 metadata.
- Scheduled scale, fuzz, live-model, and real-KVM evidence workflows.
- Recovery-soak duration matrix: weekly 24h and monthly 72h scheduled runs, a monthly 7d run chunked into two sequential 84h jobs (GitHub terminates self-hosted jobs after 5 days), and on-demand `soak_hours` dispatch (24h/72h/168h). Long soaks use a dedicated concurrency group so a multi-day soak no longer delays the daily nightly evidence run.
- Multi-language release SBOMs and SLSA build provenance attestations.
- Typed workflow output contracts with JSON Schema validation and RFC 6901 conditions.
- Logical-model routing independent of provider endpoint and wire-model selection.

### Changed

- Model tool declarations now cross the wire in the OpenAI-compatible function-tool shape (`{"type":"function","function":{…}}`); strict providers (vLLM/Qwen/DeepSeek/GLM/OpenAI) no longer risk rejecting or ignoring flat tool objects. The internal `ToolDefinition` stays flat.
- Nightly live-model acceptance runs `TestRealModelToolCalling` in addition to the v1.1 real-model suite, so the nightly log proves the model→tool→model closed loop.
- MCP tool idempotency keys bind the resolved tool version: the same arguments against two versions of one tool never share replay semantics.
- Tool discovery shows exactly one entry per tool name — the latest granted version, which is what a bare-name invocation resolves to — while explicit `name@version` pins still reach older granted versions.
- Runtime Interface HTTP client caching includes the binding's TLS material fingerprint: bindings sharing an endpoint and SNI but carrying different certificates resolve to distinct clients.
- Scheduling uses effective capacity: pool listing subtracts the durable active reservation ledger from declared totals, placement hard filters and headroom scores use the result, and pools without a registered capacity ledger fail placement closed.
- Capacity races fall back: the scheduler walks the full ranked candidate list in one reconcile pass and defers with per-candidate diagnostics only after every candidate lost the transactional reservation.
- `ScheduleTask` never writes pool total capacity; the runtime pool registry is the single authoritative writer (with a shrink-below-active-reservation guard), and placement on an unregistered capacity ledger fails closed.
- Workflow idempotency scope now includes the namespace (`tenant_id + namespace + idempotency_key`), matching the task scope; the same key under different namespaces creates independent workflows.
- MCP execution identity is mandatory in production and idempotency hashes use full SHA-256 values.
- The runtime adapter's sandbox MCP listener is loopback-only in every mode, including fully configured production mTLS.
- Model token and cost guards account for streaming tool calls, ambiguous outcomes, and outstanding reservations.
- Failed model calls record known-zero, known, or unknown usage instead of treating uncertainty as free usage.
- Provider, tool, Runtime Adapter, and artifact boundaries share credential redaction rules.
- Runtime Adapter polling is adaptive and immutable AgentVersion decoding is forward-compatible.
- Workflow approval decisions preserve both the deciding principal and the decision.
- The Python RuntimeHost releases a stuck execution's ledger capacity after the termination grace and documents its protocol-host (non-isolation) boundary.
- The 1M capacity-baseline job allows 20h with an 18h test timeout (was 10h/9h): the 100k baseline takes 1-1.4h, so 1M is expected to need 8-13h and the previous test timeout could kill the leg mid-pipeline.

### Security

- Remote production runtime bindings must present a mutual-TLS client certificate identity; loopback endpoints and deployments that explicitly acknowledge the development policy are exempt. Server verification alone no longer satisfies the production policy for remote Runtime Interface endpoints.
- Tenant-owned mutations and lineage foreign keys are tenant-scoped.
- System tool namespaces are reserved, capability wildcard behavior is unified, and memory sensitivity is authorization-enforced.
- Remote Runtime Adapter control traffic requires SPIFFE/mTLS outside explicit loopback development mode.

### Fixed

- Real model→tool→model closed loop: the runtime adapter's gRPC model broker dropped the agent-resolved tool definitions from `InvokeRequest.tools`, so providers never saw the tool surface and answered from parametric memory; and assistant turns serialized tool calls as flat `{id,name,arguments}` instead of the OpenAI-compatible `{"id","type":"function","function":{…}}` envelope, so strict providers rejected the follow-up turn carrying tool results. Both surfaces now convert at the wire boundary only; regression tests pin the mapping. Verified end-to-end against a live vLLM endpoint (`TestRealModelToolCalling`: model calls → webhook tool execution → grounded answer with exact usage settlement).

## 1.0.0 - 2026-08-21

### Added

- GA Fenced control plane, task kernel, scheduling, recovery, runtime providers, governed gateways, stable v1 contracts, audit ledger, and signed release pipeline.
