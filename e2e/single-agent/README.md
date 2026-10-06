# v1.1 Single-Agent Real-Loop Acceptance (Phase 1 / v1.1)

Executable acceptance evidence for the development plan's v1.1 gate —
"Real Agent Execution": a real Python agent, a real OpenAI-compatible model
execution layer behind the Model Gateway, MCP-mediated tools and memory,
checkpoint/lease-expiry recovery, and the capacity numbers.

## What each test proves

| Test | Acceptance item |
| --- | --- |
| `TestV11SingleAgentRealLoop` | One real Python agent fully governed: manifest → admission → scheduler → adapter runtime → model (OpenAI-compatible provider with streaming + exact usage) → tool (HTTPS webhook through MCP) → memory write/search → checkpoint → audit → result. Asserts usage settled exactly once, cost from the pinned price table, provider request ids recorded, and the provider credential never observable by the agent. |
| `TestV11ThousandTaskPipeline` | `FENCED_E2E_TASKS` (default 1000) complete tasks through the same real loop with ≥99% success, four concurrent workers sharing one agent endpoint (per-attempt MCP identity fencing), no duplicate settlements, no duplicate results; reports throughput. |
| `TestV11RecoveryFaultInjection` | `FENCED_E2E_FAULTS` (default 100) lease-expiry fault injections: the worker is killed mid-execution after a checkpoint confirmed the tool turn, the recovery controller requeues, a fresh worker restores from the checkpoint, and the confirmed tool side effect is never repeated. Recovery success ≥99%. |

The gVisor isolation leg (Phase 1.6) is validated by the existing
`runtime-linux-leg` CI job on real containerd + runsc hosts.

## Running

```powershell
docker compose -f deploy/dev/compose.yaml up -d --wait
$env:FENCED_TEST_DATABASE_URL = "postgres://fenced:fenced-dev-only@127.0.0.1:55432/fenced?sslmode=disable"
$env:FENCED_E2E_PYTHON = "python"
go test -tags=integration -count=1 -timeout 30m ./e2e/single-agent/
```

The suite launches the real agent from `examples/agents/python_remote` and
the SDK from `sdk/python`; no pip installs are required (the agent runtime
is standard-library only). Each test runs in its own PostgreSQL schema.
