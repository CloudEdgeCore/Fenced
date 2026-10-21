# AGENTS.md

Operating guide for Fenced — for AI agents and new contributors. It answers
three questions: where the code is, how to prove a change works, and what must
not be broken.

Fenced is a control plane for AI agent execution: it owns the durable
Task → Run → Attempt state machine in PostgreSQL, places work on runtime pools
under fenced leases, and meters tool and model calls against task budgets.
The agent code itself runs outside the kernel, behind the Runtime Interface
(`fenced.runtime.interface/v1`).

## Requirements

| Tool | Version | Needed for |
| --- | --- | --- |
| Go | 1.26.x (CI uses 1.26.6) | everything |
| Python | 3.11+ | SDK, adapters, conformance trial |
| Rust | 1.97.1 | Wasmtime provider only |
| Docker | any recent | PostgreSQL, NATS, isolation drills |

The Go, Python and TypeScript SDKs, the conformance suite, and the whole
control plane run without a model account or an API key.

## Repository map

| Path | What lives there |
| --- | --- |
| `internal/kernel/` | The kernel: state machines, admission, scheduler, recovery, gateways, budgets, audit, workflows, supervisor |
| `internal/kernel/model/provider/` | The model wire dialects (OpenAI-compatible, Anthropic Messages) shared by the gateway and the developer CLI |
| `internal/kernel/store/postgres/` | The persistence contract; **fencing predicates live here** |
| `internal/control/` | Operator REST API (`api/`) and identity (`auth/`) |
| `internal/control/console/` | The operator console: one static document that reads the control plane's own endpoints |
| `internal/control/api/` | One file per resource: `handler.go` (assembly, routes, shared helpers), `tasks.go`, `agent_versions.go`, `tools.go`, `memories.go`, `audit.go`, `quota.go`, `health.go`, `namespaces.go`, `services.go`, `workflow.go`, `runtime_pool.go` |
| `internal/gateway/` | Agent-facing gRPC: tool, model, memory, IPC, secret broker |
| `internal/runtime/` | Runtime providers: `reference/`, `adapter/`, `oci/`, `control/`, `leasekeeper/` |
| `internal/runtime/adapter/` | The adapter worker: `worker.go` (assignment loop), `worker_transport.go` (endpoint and TLS), `worker_execution.go` (waiting for the agent), `worker_checkpoint.go`, `worker_protocol.go` |
| `internal/runtime/bindings/` | The runtime binding table and the transport policy that guards it (`EndpointPolicy`, `IsLoopbackHost`) |
| `internal/runtime/broker/` | The sandboxed agent MCP brokers: model, memory, mailbox and workflow spawn |
| `internal/mcp/` | The brokered MCP surface a sandboxed agent talks to |
| `internal/platform/` | SPIFFE, outbox, projectors, artifact store, migrate, OTel, redaction |
| `internal/security/` | The negative test suite (no production code): secret leak, approval binding, tenant escape, fencing replay |
| `cmd/` | 18 binaries (`fenced-control`, `fenced-controller`, `fenced-gateway`, …) |
| `cmd/agent/` | The developer CLI: `ui.go` points at the console of the configured control plane |
| `runtime/wasm/` | The Rust Wasmtime provider |
| `sdk/agent` | The canonical Runtime Interface contract (Go, standard library only) |
| `sdk/python`, `sdk/typescript` | The other SDKs; they mirror `sdk/agent` |
| `sdk/conformance/` | The black-box certification suite and the shared wire vectors (`fixtures/runtime-interface-v1.json`) |
| `api/`, `proto/` | The machine-checked contracts (OpenAPI 3.1, protobuf) |
| `gen/go/` | **Generated** protobuf bindings — never edit by hand |
| `db/migrations/` | Forward-only SQL migrations (`000001` …) |
| `deploy/`, `examples/`, `e2e/`, `conformance/` | Deployment, reference workloads, acceptance suites |

## The verification ladder

Run the cheapest rung that can fail your change. Each rung is a `make` target.

| Rung | Command | Proves | Cost |
| --- | --- | --- | --- |
| 1 | `make fmt-check vet test` | the code compiles and the unit tests hold | seconds |
| 2 | `make conformance` | a Runtime Interface adapter still speaks the protocol | ~30 s |
| 3 | `make dev-up` then the commands in `docs/development.md` | the control plane boots, migrates and serves | minutes |
| 4 | `make takeover-check` | fencing really rejects a stale identity (real PostgreSQL) | ~1 min |
| 5 | `make test-integration` | the full acceptance suites (needs `FENCED_TEST_DATABASE_URL`) | tens of minutes |
| 6 | `make verify-ci` | everything CI runs that this machine can run | long |

`make verify` is rung 1 plus the Python SDK and the version assertion; it is
the gate to run before every commit.

## Daily tasks

```
make help              # every target with a one-line description
make build             # build the 18 commands
make test / test-race  # unit tests
make python-test       # Python SDK, adapters
make ts-test           # TypeScript SDK
make proto             # buf lint + regenerate + verify gen/go is unchanged
make lint / vuln       # staticcheck / govulncheck
make rust-lint rust-test
make conformance       # Runtime Interface smoke test
make dev-up / dev-down / dev-migrate / dev-reset
make takeover-check    # fencing evidence
make verify            # the local pre-commit gate
```

Override the toolchain when it is not on `PATH`:
`make GO=/opt/go/bin/go PYTHON=python3.12 verify`.

## How the pieces fit

```
agent framework ──HTTP/JSON──▶ Runtime Interface adapter ──gRPC──▶ Runtime Control
                                     │                                    │
                                     ▼                                    ▼
                             MCP broker (sandbox)              Task/Run/Attempt (PostgreSQL)
                                     │                                    │
                          tool/model/memory/IPC gateways ◀── budgets ───┘
                                     │
                                     ▼
                              audit ledger (hash-chained)
```

Invariants worth knowing before you touch the kernel:

- **Fencing is enforced in SQL, not in application code.** Every fenced write
  joins the attempt to the run and requires
  `r.current_fencing_token = a.fencing_token`. A worker that lost its lease
  cannot update durable state, no matter what it believes.
- **A lease expiry produces a replacement attempt with a higher token.** The
  old identity is rejected at the gateway, the syscall boundary and the store.
- **Budgets are reserved, settled incrementally and released.** Provider
  reported usage is authoritative; declared estimates only size the reservation.
- **The audit ledger is append-only and hash-chained**, written in the same
  transaction as the mutation it records.
- **Capability grants are split into two vocabularies at publish time.**
  `capabilities.tools` carries the tool names a runtime check compares against;
  `capabilities.toolVersions` carries the pinned (`name@version`) and floating
  (`name@*`) grants the publish-time freeze consumes. Authors write
  `capabilities.tools` in either form; the kernel normalizes the publication.

## Hard rules

1. **Never weaken a default-deny boundary to make a test pass.** Capability
   checks, tenant checks, the freeze, the budget ceiling and the audit chain
   are the product. A test that needs a wider grant must widen the *test
   fixture*, not the rule.
2. **Never edit `gen/go/`.** Run `make proto`; CI fails when the generated
   bindings drift.
3. **A public contract change needs a contract version.** The v1.2 freeze
   (`docs/contracts/v1.2-contract-freeze.md`) pins field numbers and syscall
   numbers. `v1alpha1` compatibility must not be removed before **2027-02-17**.
4. **Keep the documentation honest.** `docs/feature-status.md` distinguishes
   GA, Implemented, Limited and Not verified. If a change does not move a row
   to a stronger status, say so. Do not describe an untested path as certified.
5. **A capability row names its proof.** `docs/feature-status.md` is checked:
   every row names a test, a command or an evidence artifact, and a **GA**
   row must name a test that passes. A capability added without a proof is
   added without GA. The check is `make capability-verify`.
6. **A migration is forward-only and additive.** Add `db/migrations/NNNNNN_*.up.sql`;
   never edit an applied migration.
7. **Credentials never cross a boundary.** Provider keys are read from the
   configured environment variable per request, and secret handles are injected
   into the executor, never returned to the agent.
8. **A security policy has one implementation.** The transport policy of the
   Runtime Interface lives in `internal/runtime/bindings` (`EndpointPolicy`,
   `IsLoopbackHost`). A command line, a provider or a tool that needs the
   same rule calls it; a second copy of the rule is a second boundary that
   can drift.

## Known traps

These cost time in practice; they are properties of the system, not bugs you
introduced.

- `pkill -f 'fenced-gateway'` also matches the shell that contains the string
  and kills it. Kill by the listening port instead:
  `pid=$(ss -tlnp | grep ':9091' | sed -E 's/.*pid=([0-9]+).*/\1/')`.
- A budget dimension of `0` is invalid, not unlimited: `toolCalls: 0` is
  rejected with `BUDGET_TOOL_CALLS_INVALID`. Set a real ceiling.
- A container runtime class needs an explicit `placement.workspaceBytes`;
  otherwise admission rejects the task with `CONTAINER_WORKSPACE_REQUIRED`.
- The reference worker's deterministic model script **settles declared usage
  without calling the provider** (`finish_reason: deterministic-script`). Use
  the adapter path with a real agent to exercise a real provider.
- A capability grant for a tool is compared against the **bare tool name**.
  `name@version` also grants the tool, but the pinned version belongs to the
  freeze, not to the runtime check.
- The model gateway needs a model descriptor **and** a provider route for the
  same reference; a missing descriptor surfaces as `kernel object not found`.
- The developer CLI and the Model Gateway share one model implementation
  (`internal/kernel/model/provider`). Change a wire protocol there, never in
  `cmd/agent/stream.go`. The CLI reports the provider-reported token usage
  when the provider reports it, and falls back to a character estimate
  otherwise.
- The Runtime Interface has one written contract:
  `sdk/conformance/fixtures/runtime-interface-v1.json`. The Go SDK, the Python
  SDK and the OpenAPI specification are all checked against it, so a renamed
  field or a changed status code on one side fails the others. Add a vector
  there instead of asserting the shape in one implementation.
- The transport policy has one implementation, in
  `internal/runtime/bindings` (`EndpointPolicy.Validate`, `IsLoopbackHost`).
  The adapter command line delegates to it; do not re-implement the loopback
  or plaintext rules there. The development escape hatch
  (`-allow-plaintext-remote-runtime`) applies to the runtime binding table
  only: the explicit `-adapter-endpoint` always requires HTTPS or loopback.
- `fenced publish` expects `kind: AgentManifest`; the stored publication is an
  `AgentVersion`.

## Where the detail lives

- `docs/development.md` — local control plane, observability, release checks
- `docs/architecture/ARCHITECTURE.md` — the design in one page
- `docs/contracts/v1.2-contract-freeze.md` — the frozen public contracts
- `docs/feature-status.md` — the verified capability matrix
- `docs/user-guide.md` — the Chinese end-user and developer guide
- `docs/evidence/` — benchmark, soak and takeover evidence with raw logs
- `CHANGELOG.md` — release notes; keep the engineering tone
- `modelcheck/tla/` — the formal model of the core safety invariants
- `scripts/push-with-fallback.sh` — pushing to GitHub when the route to the
  git host is down or answered by an intercepting address: it probes the
  real git endpoint, waits for one that answers, pins it for the push and
  restores the hosts file afterwards
