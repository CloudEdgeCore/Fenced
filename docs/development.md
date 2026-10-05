# Fenced development and deployment reference

For a first trial, use [the README](../README.md#try-locally). This guide keeps
the full local control-plane setup, developer commands, and validation steps.
Run all commands from the repository root. PowerShell examples set environment
variables in the current terminal; set them again in each new terminal.

- [Requirements and source build](#requirements-and-source-build)
- [Reproduce a takeover](#reproduce-a-takeover)
- [Start the local control plane](#start-the-local-control-plane)
- [Task-backed services](#task-backed-services)
- [Production security baseline](#production-security-baseline)
- [Tests and quality gates](#tests-and-quality-gates)
- [Stable contracts and compatibility](#stable-contracts-and-compatibility)

## Requirements and source build

- Go `1.26.x`; CI and official releases use `1.26.6`.
- Python `3.11+` when developing Python or framework adapters.
- Rust `1.97.1` when building the Wasmtime provider.
- Docker and Docker Compose for local PostgreSQL, NATS, and optional infrastructure.
- Linux, containerd, and runsc for real OCI/gVisor provider validation.

Inspect the source release identity:

```shell
go run ./cmd/fenced version -json
```

Build every Go command:

```shell
go build ./cmd/...
```

## Reproduce a takeover

This integration check uses real PostgreSQL storage and the Runtime Control
service. It expires a lease deliberately, invokes recovery, and verifies a higher
fencing token and rejection of the previous owner's heartbeat, checkpoint,
state transition, and assignment read. It checks that no stale checkpoint is
persisted. It does not kill a worker process or deploy a multi-host runtime.

Requirements: Go 1.26.x and Docker running Linux containers. No Python, model
service, or API key is needed. The database account must be able to create
databases: the test creates and resets a database named `fenced_security`.
Use the disposable PostgreSQL instance below; the test clears its test tables.

Start the pinned PostgreSQL/pgvector image used by this repository:

```shell
docker run --detach --name fenced-readme-postgres --publish 127.0.0.1:55434:5432 --env POSTGRES_USER=fenced --env POSTGRES_PASSWORD=fenced-readme-only --env POSTGRES_DB=fenced_readme pgvector/pgvector:pg18@sha256:2ba9ca5f2e7daa0f0e7723cba1ee9167bab54efd3640516a44ac1a928dd67e7a
docker exec fenced-readme-postgres pg_isready -U fenced -d fenced_readme
```

Wait until `pg_isready` reports `accepting connections` before running the test.
The first image pull and Go compilation can take longer than the check itself.

**PowerShell:**

```powershell
$env:FENCED_TEST_DATABASE_URL = "postgres://fenced:fenced-readme-only@127.0.0.1:55434/fenced_readme?sslmode=disable"
go test -tags=integration -count=1 -run '^TestFencingReplayRejectedAfterTakeover$' -v ./internal/security
```

**Bash / zsh:**

```bash
FENCED_TEST_DATABASE_URL='postgres://fenced:fenced-readme-only@127.0.0.1:55434/fenced_readme?sslmode=disable' go test -tags=integration -count=1 -run '^TestFencingReplayRejectedAfterTakeover$' -v ./internal/security
```

Expect `--- PASS: TestFencingReplayRejectedAfterTakeover` followed by `PASS`.
`SKIP` means the database environment variable is missing and the check did not
run. See the [test source](../internal/security/negative_integration_test.go) for
the complete assertions.

Remove the disposable container and its test data when finished:

```shell
docker rm --force --volumes fenced-readme-postgres
```

## Start the local control plane

```powershell
docker compose -f deploy/dev/compose.yaml up -d --wait postgres nats

$env:DATABASE_URL = "postgres://fenced:fenced-dev-only@127.0.0.1:55432/fenced?sslmode=disable"
go run ./cmd/fenced-migrate -database-url $env:DATABASE_URL
```

The repository-root `docker-compose.yml` includes the same definitions (project `fenced-dev`), so `docker compose up -d --wait postgres nats` from the repository root starts the identical stack.

For local development, run each process below in its own terminal:

```powershell
# HTTP Control API
go run ./cmd/fenced-control `
  -database-url $env:DATABASE_URL `
  -dev-tenant dev

# Admission, Scheduler, and Recovery
go run ./cmd/fenced-controller `
  -database-url $env:DATABASE_URL `
  -controller-id dev-controller `
  -runtime-pools deploy/dev/runtime-pools.json `
  -tenant-policies deploy/dev/tenant-policies.json `
  -dev-mode

# Transactional outbox to NATS JetStream
go run ./cmd/fenced-outbox `
  -database-url $env:DATABASE_URL `
  -nats-url nats://127.0.0.1:54222 `
  -dispatcher-id dev-outbox

# Worker Runtime Protocol
go run ./cmd/fenced-runtime-control `
  -database-url $env:DATABASE_URL `
  -listen 127.0.0.1:9090 `
  -dev-tenant dev `
  -dev-mode

# Tool, Model, and Memory Gateway; add -model-providers to activate the
# OpenAI-compatible execution layer (vLLM/Qwen/DeepSeek/GLM endpoints).
# deploy/dev/*.local.json is gitignored for private endpoint configurations:
#   -model-providers deploy/dev/model-providers.example.json
go run ./cmd/fenced-gateway `
  -database-url $env:DATABASE_URL `
  -listen 127.0.0.1:9091 `
  -tenant-policies deploy/dev/tenant-policies.json `
  -tenant dev `
  -seed-dev-tools `
  -dev-mode

# Non-sandboxed reference provider for development and deterministic tests
go run ./cmd/fenced-runtime-reference `
  -control-address 127.0.0.1:9090 `
  -gateway-address 127.0.0.1:9091 `
  -model-gateway-address 127.0.0.1:9091 `
  -mcp-listen 127.0.0.1:9092 `
  -tenant dev `
  -runtime-instance-id dev-worker-1 `
  -artifact-root tmp/artifacts `
  -dev-mode
```

`fenced init` writes an environment-independent logical entrypoint
(`fenced-binding://<agent-name>/remote`) into the manifest, so one immutable
AgentVersion deploys across dev/staging/prod without re-signing. Map version
refs (or `name@*` wildcards) to concrete Runtime Interface endpoints with
`fenced-runtime-adapter -runtime-bindings deploy/dev/runtime-bindings.example.json`;
an explicit `-adapter-endpoint` still overrides bindings, and unresolved
logical entrypoints fail closed. The `-mcp-listen` sandbox MCP endpoint is
loopback-only in every mode, including configured production mTLS.

The adapter runtime additionally exposes a loopback MCP endpoint for its
sandboxed agents (`-mcp-listen 127.0.0.1:9095 -gateway-address 127.0.0.1:9091`):
tenant tools plus the brokered system tools `fenced.model.invoke`,
`fenced.memory.put`, and `fenced.memory.search`, fenced to the open attempt
via the `X-Fenced-Execution` identity the worker injects (default deny
outside execution windows). A real model-backed Python agent lives at
`examples/agents/python_remote/real_agent.py`.

These commands use a fixed development tenant, loopback plaintext connections, and development executors. They are only safe for local development. Production mode rejects these downgraded settings.

### CLI workflows

The developer-facing entrypoint is the unified `agent` CLI introduced in
v1.2.1 (`agent config`, `agent config wizard`, `agent test-llm`, `agent mcp`,
`agent init`, `agent demo`). This release is deliberately CLI-only: `agent ui`
is disabled for external access and only runs as the loopback-bound internal
polishing preview (`agent ui --preview`). Commands the `agent` CLI does not own
are delegated to the stable `fenced` workflows below. The `fenced` CLI exposes:

```text
fenced version   Print product, build, and protocol versions
fenced init      Create a Go/Python/LangGraph/A2A agent project
fenced migrate   Promote a legacy manifest to v1
fenced validate  Strictly validate an Agent Manifest
fenced package   Generate a package manifest with provenance
fenced sign      Sign a package with an Ed25519 key
fenced publish   Publish an immutable AgentVersion
fenced run       Submit a durable task
fenced logs      Stream task events over SSE
fenced workflow  Create, inspect, cancel, approve/reject, and render workflow trees
fenced runtime   Activate, cordon, or drain a runtime pool with CAS protection
fenced conformance  Check a running Runtime Interface adapter
```

`publish`, `run`, and `logs` use `http://127.0.0.1:8080` by default. In production, pass the HTTPS Control API through `-endpoint` and provide a bearer token through `FENCED_TOKEN`.

Model provider configuration may also declare `routes`, mapping a stable
tenant-visible `modelRef` to an independently selected provider and wire model.
This keeps workflow and Agent manifests stable while operations change model
hosts or aliases. See `deploy/dev/model-providers.example.json` for the strict
configuration shape; keep private endpoint files in `deploy/dev/*.local.json`.

### Safety limits reference

| Boundary | Default hard limit |
| --- | ---: |
| Control API request body | 1 MiB |
| Workflow document / declared steps | 1 MiB / 1,024 |
| Dynamic workflow total steps | 100,000 |
| Step goal / retry attempts | 8 KiB / 10 |
| Runtime interface body / event payload | 2 MiB / 256 KiB |
| Runtime event page | 256 events and 1 MiB |
| Model provider request / response | 4 MiB / 32 MiB |
| MCP memory response aggregate | 1 MiB |

Deployment-specific admission, tenant quota, concurrency, and workflow budget
limits may be lower; zero never silently disables a required sandbox limit.

## Task-backed services

Services require a published `spec.agentVersionRef` and a Task specification
in `spec.workloadSpec`; `fenced service create` accepts it with
`-spec task-spec.json`. Each replica consumes a real worker execution slot
and retains the Task budget and runtime timeout limits. Apply migration
`000037` before using the Task-backed supervisor.

Ready replicas reflect active fenced runtime leases. Application readiness
and traffic switching require deployment integration. See the
[service guide](user-guide.md#71-服务注册与启动) and
[Helm chart guide](../deploy/helm/fenced/README.md) for configuration.

## Production security baseline

Fenced production mode is fail-closed. Processes refuse to start when required security configuration is missing.

- **Control API:** requires HTTPS, OIDC, a production embedding endpoint, an audit signing key, and at least one package trust key.
- **Runtime Protocol:** requires SPIFFE X.509-SVID mTLS and binds worker identity to the tenant.
- **Gateway:** requires mTLS, derives the tenant from the peer SVID, and maps immutable tool versions to HTTPS endpoints.
- **Secret Broker:** obtains controlled secrets or dynamic database credentials through OpenBao instead of exposing platform credentials to agents.
- **OCI Provider:** requires digest-pinned production images and uses containerd with gVisor/runsc on the Linux isolation path.
- **Agent Package:** validates signatures, provenance, image signatures, and CycloneDX SBOMs during publication.
- **Audit:** records security-relevant events in a transactional hash chain and supports signed export and integrity verification.

Development mode is restricted to loopback or requires explicit `-dev-mode`; never expose it to an untrusted network.

## Observability and optional services

Start the reference OpenTelemetry stack:

```powershell
docker compose -f deploy/dev/compose.yaml --profile observability up -d
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "127.0.0.1:4317"
```

- Grafana: `http://127.0.0.1:3300`
- Prometheus: `http://127.0.0.1:9093`
- Tempo: `http://127.0.0.1:3320`
- Loki: `http://127.0.0.1:3310`

Optional development services:

```powershell
docker compose -f deploy/dev/compose.yaml --profile secrets up -d  # OpenBao
docker compose -f deploy/dev/compose.yaml --profile search up -d   # OpenSearch
```

Control API operational endpoints:

- `GET /healthz`: process liveness.
- `GET /readyz`: readiness of PostgreSQL and other required dependencies.
- `GET /versionz`: product identity, build commit, and every stable protocol version.

## Tests and quality gates

Formatting, static analysis, unit tests, and Go vulnerability scanning:

```shell
gofmt -l .
go vet ./...
go test -race -count=1 ./...
go tool govulncheck ./...
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
(cd sdk/python && python -m unittest discover -s tests -v)
(cd sdk/typescript && npm ci)
(cd sdk/typescript && npm test)
```

Real PostgreSQL/NATS integration tests use the disposable PostgreSQL instance
from [the takeover check](#reproduce-a-takeover). If you removed that container,
repeat its startup and readiness check first. Start NATS separately:

```powershell
docker compose -f deploy/dev/compose.yaml up -d --wait nats
$env:FENCED_TEST_DATABASE_URL = "postgres://fenced:fenced-readme-only@127.0.0.1:55434/fenced_readme?sslmode=disable"
$env:FENCED_TEST_NATS_URL = "nats://127.0.0.1:54222"

go test -race -tags=integration -count=1 -timeout 60m ./...
```

The suite applies migrations and clears test tables. Keep this instance separate
from development or business data. Runtime-specific and live-model checks need
their own prerequisites; a skipped check is not verification.

Production migrations are forward-only and use expand/contract deployment:
take and verify a restorable backup, apply additive schema changes, deploy
code compatible with both shapes, backfill and reconcile, then remove the old
shape in a later release. Rollback means rolling back the application while
the expanded schema remains; destructive data rollback requires restoring the
verified backup into a replacement database and switching traffic after
integrity checks. Never edit an already-applied migration.

Protobuf compatibility and deterministic generation:

```shell
buf lint
buf generate
git diff --exit-code -- gen/go
```

Wasmtime provider:

```shell
cargo +1.97.1 fmt --all -- --check
cargo +1.97.1 clippy --workspace --all-targets --locked -- -D warnings
cargo +1.97.1 test --workspace --locked
```

CI runs the real Linux OCI/gVisor isolation suite in the `runtime-linux-leg` job. The pinned toolchain and acceptance mapping are defined in [`deploy/ci/runtime-matrix.md`](../deploy/ci/runtime-matrix.md).

v1.2 workflow acceptance (multi-agent orchestration: WorkflowRun with
dependency/parallel/join/condition/retry/approval/cancel/recovery, the
1,000-workflow dual-agent regression, and the Phase 3 scale gates — one
1,000-step workflow, 100 concurrent workflows, orchestrator P95 < 500ms):

```powershell
go test -race -tags=integration -count=1 -timeout 60m ./e2e/workflows/
```

Counts are tunable (`FENCED_E2E_WORKFLOWS`, `FENCED_E2E_WF_STEPS`,
`FENCED_E2E_CONCURRENT_WF`). The workflow orchestrator runs as its own
process (`go run ./cmd/fenced-orchestrator -database-url $db
-orchestrator-id dev-orchestrator -artifact-root tmp/artifacts`) and the
Control API exposes `POST/GET /v1/workflows`, `POST /v1/workflows/{id}/cancel`
and `POST /v1/workflows/{id}/steps/{name}/approval`.

v1.3 dynamic and distributed orchestration adds fenced `fenced.task.spawn`,
workflow-wide budgets and deadlines, recursion/fan-out/total-step guards,
dynamic group joins (`spawn:<parent>`), and lease-based fair sharding across
orchestrator instances. The normal integration leg covers capability denial,
stale-attempt fencing, tenant isolation, concurrent spawn idempotency, 120
tenant rotation, claim exclusivity, and expired-owner recovery:

```powershell
$env:FENCED_TEST_DATABASE_URL = "postgres://fenced:fenced-readme-only@127.0.0.1:55434/fenced_readme?sslmode=disable"
go test -tags=integration -count=1 -run '^TestV13' ./internal/kernel/store/postgres
```

The opt-in lower-bound scale leg commits 10,000 dynamic steps as independent
transactions and verifies the final 10,001-step workflow:

```powershell
$env:FENCED_V13_SCALE_TEST = "1"
go test -tags=integration -count=1 -run '^TestV13DynamicSpawnScale10K$' -v ./internal/kernel/store/postgres
go test -count=1 -run '^TestV13Orchestrates10KDynamicTasks$' -v ./internal/kernel/workflow
```

For local plaintext development, start the orchestrator with
`-claim-lease 30s -listen 127.0.0.1:9094 -dev-mode` and give the adapter
`-spawn-address 127.0.0.1:9094`. Production spawn transport requires the
same worker X.509-SVID and trust bundle used by the Runtime and Gateway
protocols; the server binds the verified SPIFFE tenant to the fenced request.

v1.1 real-agent acceptance (a real Python agent, real OpenAI-compatible model
execution, MCP tools and memory, lease-expiry recovery, 1,000-task pipeline
and 100 fault injections):

```powershell
$env:FENCED_E2E_PYTHON = "python"
go test -race -tags=integration -count=1 -timeout 30m ./e2e/single-agent/
```

See [`e2e/single-agent/README.md`](../e2e/single-agent/README.md) for what each
test proves and how to tune the counts (`FENCED_E2E_TASKS`,
`FENCED_E2E_FAULTS`).

Evaluate a measured SLO sample:

```shell
go run ./cmd/fenced-slo -sample measured-slo.json
```

## Stable contracts and compatibility

See the comprehensive [v1.2 Contract Freeze](contracts/v1.2-contract-freeze.md) for frozen specifications.

| Contract | Stable version | Source |
| --- | --- | --- |
| **Syscall ABI** | `1.0.0` | [`proto/fenced/syscall/v1/syscall.proto`](../proto/fenced/syscall/v1/syscall.proto) |
| **IPC Subsystem** | `fenced.ipc.v1` | [`proto/fenced/ipc/v1/ipc.proto`](../proto/fenced/ipc/v1/ipc.proto) |
| **Agent Service & Supervisor** | `fenced.service.v1` | [`proto/fenced/service/v1/service.proto`](../proto/fenced/service/v1/service.proto) |
| **External Effect API** | `fenced.effect.v1` | [`proto/fenced/effect/v1/effect.proto`](../proto/fenced/effect/v1/effect.proto) |
| Control REST API | `v1` | [`api/openapi/control-v1.yaml`](../api/openapi/control-v1.yaml) |
| Agent Manifest | `fenced.dev/v1` | [`internal/kernel/agentversion/manifest.go`](../internal/kernel/agentversion/manifest.go) |
| Runtime Protocol | `fenced.runtime.v1` | [`proto/fenced/runtime/v1/runtime.proto`](../proto/fenced/runtime/v1/runtime.proto) |
| Runtime Interface | `fenced.runtime.interface/v1` | [`api/openapi/runtime-interface-v1.yaml`](../api/openapi/runtime-interface-v1.yaml) |
| Gateway Protocol | `fenced.gateway.v1` | [`proto/fenced/gateway/v1/gateway.proto`](../proto/fenced/gateway/v1/gateway.proto) |
| Model Protocol | `fenced.model.v1` | [`proto/fenced/model/v1/model.proto`](../proto/fenced/model/v1/model.proto) |
| SLO contract | `fenced.slo/v1` | [`api/slo/v1.json`](../api/slo/v1.json) |

`v1alpha1` is the N-1 compatibility level for v1.0. Legacy manifests remain readable and can be promoted deterministically, while legacy gRPC service names remain available as wire-compatible aliases. The compatibility window will not close before **2027-02-17**. Breaking changes to stable contracts require a new version, and unknown fields continue to fail closed. The machine-readable policy is stored in [`api/compatibility/v1alpha1-to-v1.json`](../api/compatibility/v1alpha1-to-v1.json).

Promote a legacy manifest to v1:

```shell
go run ./cmd/fenced migrate \
  -manifest agent.v1alpha1.json \
  -out agent.v1.json
```

## Releases and supply-chain verification

The [release workflow](../.github/workflows/release.yml) defines command archives
for Linux, macOS, and Windows, Python and TypeScript SDK packages, a Linux
Wasmtime binary, SBOMs, checksums, and Sigstore provenance artifacts. See
[GitHub Releases](https://github.com/CloudEdgeCore/Fenced/releases) for the
assets actually published for a tag.

For a published tag, download its assets and check the checksums. Replace
`v1.3.0` if using a different version:

```shell
gh release download v1.3.0 --repo CloudEdgeCore/Fenced
sha256sum -c checksums.txt
```

## Repository layout

| Path | Contents |
| --- | --- |
| `cmd/` | CLI, control-plane, gateway, runtime provider, and operator entry points |
| `internal/kernel/` | Task/Run/Attempt, Admission, Scheduler, Policy, Budget, and Recovery |
| `internal/runtime/` | Runtime Control, Reference, Adapter, and OCI providers |
| `internal/gateway/` | Tool, Model, Memory, Capability, and Secret Broker implementations |
| `sdk/agent/` | Go Runtime Interface SDK |
| `sdk/python/` | Python Runtime Interface SDK |
| `sdk/typescript/` | TypeScript Control API and Runtime Interface SDK |
| `adapters/` | LangGraph and A2A adapters |
| `api/openapi/` | Stable and compatibility REST/HTTP contracts |
| `proto/fenced/` | Runtime, Gateway, and Model Protobuf contracts |
| `db/migrations/` | PostgreSQL migrations |
| `deploy/dev/` | Local dependencies and reference observability environment |
| `deploy/ci/` | OCI/gVisor isolation and environment fingerprint tests |
| `modelcheck/tla/` | TLA+ model of the kernel state machine |
