# Fenced Feature Status & Capability Matrix

This matrix provides the verified implementation status for all subsystems, protocols, and APIs in Fenced as of **v1.4 (Anthropic dialect, written wire contract, structural consolidation)**.

## How to read a row

Every row names its proof in the **Proved by** column, and a check runs that
proof:

| Proof | What it means |
| :--- | :--- |
| `test:<package>.<TestName>` | The test must exist and must pass. This is the only proof that can carry a **GA** row. |
| `cmd:<command>` | The command must succeed. Used where the proof lives outside the Go tree (the Rust provider). |
| `evidence:<path>` | The artifact that records the run must exist. It records a result; the check does not re-run it. |
| `none` | No proof. A row that says `none` cannot claim GA. |

The rules:

1. **A GA row needs a passing test.** A capability that no test exercises is
   not certified, whatever the code does.
2. **A weaker status may name no proof**, but the description has to say what
   is missing. `Implemented (unverified)` means the code exists and is wired,
   and nothing exercises it end to end.
3. **The proof is part of the row.** A capability is added with the test that
   proves it, or it is added without GA.

The check runs in CI: `make capability-verify` locally,
`scripts/check-capability-proofs.py --strict` in CI.

---

## 1. Process System & Workload Management

| Feature / Capability | Status | Since | Protocol / Contract | Description | Proved by |
| :--- | :---: | :---: | :--- | :--- | :--- |
| **Task State Machine** | **GA** | v1.0 | `fenced.dev/v1` | `Task → Run → Attempt` lifecycle with non-preemptible execution semantics. | test:internal/kernel/domain.TestTaskTransitions, test:internal/kernel/store/postgres.TestTaskIdempotencyAndCAS |
| **Cooperative Cancellation** | **GA** | v1.0 | `proto/fenced/runtime/v1` | Heartbeat-driven cancellation signaling with verified TLA+ liveness convergence. | test:internal/kernel/store/postgres.TestRunningTaskCancellationConvergesThroughRuntime, test:internal/runtime/reference.TestWorkerStopsOnCancellationMidExecution |
| **Lease & Fencing Token** | **GA** | v1.0 | Kernel Store | Monotonically increasing fencing tokens preventing split-brain zombie writes. | test:internal/kernel/store/postgres.TestExpiredLeaseIssuesNewFenceAndRejectsOldOwner, test:internal/kernel/syscall.TestDispatcherRuntimeFence |
| **AgentService execution** | **Implemented (runtime-dependent)** | v1.2 | `proto/fenced/service/v1` | Each instance runs through a durable Task with normal admission, budgets, placement, leases, and Runtime execution. Every active replica needs a Worker slot; Host and Task execution timeouts still apply. | test:internal/kernel/store/postgres.TestServiceTaskSupervisorsConvergeOnOneInstanceAndTask |
| **Process Supervisor** | **Implemented** | v1.2 | `proto/fenced/service/v1` | Replica reconciliation, restart policy, backoff, and runtime lifecycle tracking. Running reflects Kernel execution state, not application readiness. | test:internal/kernel/supervisor.TestSupervisorReconciliationAndScaling, test:internal/kernel/supervisor.TestServiceCRUD |
| **Heartbeat Auto-Reap** | **Implemented** | v1.2 | `proto/fenced/service/v1` | Detects stale instance liveness and requests cancellation/replacement. Physical termination of a non-cooperative runtime requires an isolation-boundary termination hook. | test:internal/kernel/supervisor.TestHeartbeatAndHealthCheck, test:internal/kernel/supervisor.TestTaskSpawnerStaleHeartbeatReachesFailureThreshold |
| **Rolling Upgrade & Drain** | **Implemented** | v1.2 | `proto/fenced/service/v1` | Version and replica convergence with drain deadlines and cooperative task cancellation. A serving instance is only replaced after its replacement proved itself healthy (`rollout.minReadyTime`), the rollout state is durable and observable, and the operator API exposes `POST /v1/services/{id}/rollout`. The traffic follows the version: an address names the agent version, and the routing only offers the instances of that version, so the traffic of a task that was admitted against one version never reaches another one. | test:internal/kernel/supervisor.TestServiceRollingUpgradeAndRollback, test:internal/kernel/supervisor.TestRolloutWaitsForTheReplacementToProveItself |
| **Automatic Rollback** | **Implemented** | v1.2 | `proto/fenced/service/v1` | A target version that fails ends the rollout; with `rollout.autoRollback` the service returns to the version it ran before and the reason is recorded in the service status. Without it the service keeps the failed version and the operator decides. | test:internal/kernel/supervisor.TestFailedRolloutRollsBackAutomatically, test:internal/kernel/supervisor.TestFailedRolloutWithoutAutoRollbackKeepsTheTargetVersion |

---

## 2. Kernel Syscall ABI 1.0.0

| Subsystem | Syscall Range | Status | Syscalls | Description | Proved by |
| :--- | :---: | :---: | :--- | :--- | :--- |
| **Tool Subsystem** | 100–199 | **GA** | `101`, `102` | Capability checking, tool execution, and cryptographic audit receipts. | test:internal/kernel/syscall.TestToolSyscalls |
| **Model Subsystem** | 200–299 | **GA** | `201`–`204` | LLM invocation, streaming sessions, budget reservation, and incremental settlement. | test:internal/kernel/syscall.TestModelSyscalls |
| **Memory Subsystem** | 300–399 | **GA** | `301`, `302` | Persistent key-value storage and pgvector semantic memory search. | test:internal/kernel/syscall.TestMemorySyscalls |
| **IPC Subsystem** | 400–499 | **GA** | `401`–`403` | Mailbox send, poll/drain, and acknowledgement across agent boundaries. | test:internal/kernel/syscall.TestIPCSyscalls |
| **Runtime Subsystem** | 500–599 | **GA** | `501`–`503` | Durable state checkpointing, attempt completion, and cooperative timeslice yield. | test:internal/kernel/syscall.TestRuntimeLifecycleSyscalls |
| **Service Subsystem** | 600–699 | **GA** | `601`, `602` | Supervisor heartbeat submission and service instance discovery. | test:internal/kernel/syscall.TestServiceSyscalls |
| **Resource Subsystem** | 700–799 | **GA** | `701`–`703` | Namespace metadata inspection, quota verification, and real-time usage querying. | test:internal/kernel/syscall.TestResourceSyscallHandlersAndClient |
| **Effect Subsystem** | 800–899 | **GA** | `801`, `802` | Fenced external mutation execution and idempotent receipt retrieval. | test:internal/kernel/syscall.TestEffectSyscall_Execute_Success |

---

## 3. Communication & Durable Messaging

| Feature | Status | Protocol | Guarantee | Proved by |
| :--- | :---: | :--- | :--- | :--- |
| **Durable Mailbox** | **GA** | `proto/fenced/ipc/v1` | Messages persist across instance and attempt crashes. | test:internal/kernel/ipc.TestMailboxCrashRecoverySimulation, test:internal/kernel/store/postgres.TestIPCMailboxStoresAndDrainsMessages |
| **At-Least-Once Delivery** | **GA** | `proto/fenced/ipc/v1` | Network/crash resilience with unconsumed message replay. | test:internal/kernel/store/postgres.TestIPCMailboxSurvivesTheDrainingAttempt, test:internal/kernel/ipc.TestMessageExpiration |
| **Deduplication Engine** | **GA** | `proto/fenced/ipc/v1` | Receiver mailbox receipts guarantee exactly-once application. | test:internal/kernel/store/postgres.TestIPCAppendReplayIsIdempotent, test:internal/kernel/store/postgres.TestIPCAppendRejectsKeyReuseWithADifferentMessage |
| **Correlation & Reply-To** | **GA** | `proto/fenced/ipc/v1` | End-to-end distributed tracing and asynchronous RPC-style conversations. | test:internal/kernel/ipc.TestRequestReply, test:internal/kernel/ipc.TestSignalDeliveryAndParsing |
| **Tenant Boundary Routing** | **GA** | Kernel Router | Cross-tenant isolation with strict default-deny delivery rules. | test:internal/kernel/ipc.TestCrossTenantDenied, test:internal/kernel/store/postgres.TestIPCCrossTenantIsolation |

---

## 4. External Side-Effect Management

| Feature | Status | Protocol | Guarantee | Proved by |
| :--- | :---: | :--- | :--- | :--- |
| **Monotonic Fencing** | **GA** | `proto/fenced/effect/v1` | Stale attempts cannot mutate external state (`SYSCALL_EFENCE`). | test:internal/kernel/effect.TestEffect_Fencing_StaleAttemptRejected |
| **Idempotency Deduplication** | **GA** | `proto/fenced/effect/v1` | Existing keys return cached `EffectReceipt` without duplicate external dispatch. | test:internal/kernel/effect.TestEffect_Idempotent_Replay_ReturnsCachedReceipt |
| **Payload Integrity** | **GA** | `proto/fenced/effect/v1` | SHA-256 payload verification against duplicate keys (`SYSCALL_EINVAL`). | test:internal/kernel/effect.TestEffect_PayloadMismatch |
| **UNKNOWN Isolation** | **GA** | `proto/fenced/effect/v1` | Ambiguous timeouts transition to `UNKNOWN` (`SYSCALL_EUNKNOWN`); auto-replay strictly forbidden. | test:internal/kernel/effect.TestEffect_Unknown_NeverAutoReplays, test:internal/kernel/effect.TestEffect_ResolveUnknownEffect |

---

## 5. Runtimes, Isolation & Framework Ecosystem

| Runtime / Framework | Status | Supported Isolations | Conformance Certified | Proved by |
| :--- | :---: | :--- | :---: | :--- |
| **Wasmtime** | **GA** | WebAssembly sandboxing, memory caps, fuel metering | Yes | cmd:make rust-test |
| **OCI / Container** | **GA** | runsc (gVisor), Linux cgroups v2, network namespaces | Yes | test:internal/runtime/oci.TestRunscExecutorUsesFailClosedCtrArguments, test:internal/runtime/oci.TestWorkerRejectsMissingContainerLimits |
| **LangGraph** | **Implemented (unverified)** | Co-located adapter, Runtime Interface v1, Syscall ABI | **No.** The adapter implements the Runtime Interface v1 and compiles, but no test exercises it end to end: the row states the wiring, not the behavior. | none |
| **AutoGen** | **Implemented (unverified)** | Co-located adapter, Runtime Interface v1, Syscall ABI | **No.** The adapter implements the Runtime Interface v1 and compiles, but no test exercises it end to end: the row states the wiring, not the behavior. | none |
| **CrewAI** | **Implemented (unverified)** | Co-located adapter, Runtime Interface v1, Syscall ABI | **No.** The adapter implements the Runtime Interface v1 and compiles, but no test exercises it end to end: the row states the wiring, not the behavior. | none |
| **OpenAI Agents SDK** | **Implemented (unverified)** | Co-located adapter, Runtime Interface v1, Syscall ABI | **No.** The adapter implements the Runtime Interface v1 and compiles, but no test exercises it end to end: the row states the wiring, not the behavior. | none |
| **Custom / In-House** | **Implemented (unverified)** | Pure Python/Go/Rust adapter via standard Runtime Protocol | **No.** The adapter implements the Runtime Interface v1 and compiles, but no test exercises it end to end: the row states the wiring, not the behavior. | none |
| **Provider SDK (Go/Py)** | **GA** | Model, Tool, Memory, Browser, Storage, Runtime plugin protocols | Yes | test:sdk/provider.TestProviderRegistryAndManifests, test:sdk/provider.TestOpenAIProviderGenerateAndStream |
| **Third-Party Runtime SDK** | **GA** | 7-lifecycle SDK, Docker, Python, Remote HTTP reference runtimes | Yes | test:sdk/runtimesdk.TestRuntimeSDKPassesConformance |
| **Package Registry & Security** | **GA** | OCI + Metadata Index, 6-stage security pipeline, CLI suite | Yes | test:internal/kernel/agentpkg.TestSecurityPipelineHappyPath, test:internal/kernel/agentpkg.TestRegistryVerifyFailClosed |

---

## 6. Reliability & Soak Testing

| Quality Gate | Evidence / Environment | Invariant Enforced | Status | Proved by |
| :--- | :--- | :--- | :---: | :--- |
| **72-Hour Soak Engine** | Dedicated fixed-host environment (`soak_test.go`) | Zero lost tasks, zero lost IPC messages, monotonic fencing | **Manual Evidence: PASS**; Scheduled CI: Pending self-hosted runners ([Evidence](evidence/soak-process-system-72h-7d.md)) | test:internal/kernel/supervisor.TestProcessSystemSoakValidation, evidence:docs/evidence/soak-process-system-72h-7d.md |
| **7-Day Extended Soak** | Dedicated fixed-host environment | Memory/goroutine leak-free, connection pool stability | **Manual Evidence: PASS**; Scheduled CI: Pending self-hosted runners ([Evidence](evidence/soak-process-system-72h-7d.md)) | evidence:docs/evidence/soak-process-system-72h-7d.md |
| **Chaos Fault Injector** | In-tree chaos harness | Worker kills, lease expirations, database reconnects | **PASS** | test:internal/kernel/supervisor.TestProcessSystemSoakValidation |
| **TLA+ Liveness Proofs** | TLC Model Checker | Guaranteed convergence to terminal phase without deadlocks | **PASS** | evidence:modelcheck/tla/README.md |

---

## 7. Developer Core & Dynamic Orchestration (v1.3)

| Feature / Capability | Status | Since | Protocol / Contract | Description | Proved by |
| :--- | :---: | :---: | :--- | :--- | :--- |
| **Developer Core CLI (`agent`)** | **GA** | v1.3 | `cmd/agent` | Cascading `agent.yaml` configuration, interactive `agent config wizard`, multi-provider LLM matrix, chain-of-thought dual-channel streaming, project scaffolding, and demo scenarios. | test:cmd/agent.TestCLIStreamsThroughTheKernelExecutor, test:cmd/agent.TestApplyConfigSet |
| **Management Console** | **GA** | v1.4 | `web/`, `internal/control/console`, `GET /console` | Served by the control plane. A built application (React) that reads the control plane's own endpoints with the operator's bearer token: the task ledger with its live event stream, the supervised services with their rollouts, the approval queue, the audit chain, the runtime pools, the publications, the workflows and the registered tools. Multi-tenant: the operator holds one token per tenant, the console keeps them apart and shows the tenants side by side in the fleet view. It holds no state, loads no external asset, and every path it calls is checked against the published contract by a test. | test:internal/control/console.TestConsoleCallsOnlyDocumentedEndpoints, test:internal/control/console.TestConsoleHandlerServesTheDocumentAndItsAssets |
| **Dual-Protocol Model Layer** | **GA** | v1.3 | `cmd/agent/stream.go`, `internal/kernel/model/provider` | OpenAI-compatible (`/chat/completions`) and Anthropic Messages (`/v1/messages`) streaming. One dialect implementation serves both the developer CLI and the kernel Model Gateway, which selects the dialect per provider (`"protocol": "anthropic"`) and settles budget, usage and receipts identically for both. The reasoning channel of a provider is delivered separately from the answer. | test:internal/kernel/model/provider.TestProviderProtocolSelection, test:internal/kernel/model/provider.TestUnsupportedProtocolFailsLoudly |
| **Dynamic Task Spawn** | **GA** | v1.3 | `fenced.task.spawn` | Fenced child-task spawning with recursion, fan-out, and total-step guards. | test:internal/kernel/store/postgres.TestV13ConcurrentSpawnCreatesExactlyOneStep, test:internal/kernel/store/postgres.TestV13DynamicSpawnScale10K |
| **Workflow Budgets & Fair Sharding** | **GA** | v1.3 | Kernel Store / Orchestrator | Workflow-wide budgets and deadlines, dynamic group joins (`spawn:<parent>`), and lease-based fair sharding across orchestrator instances. | test:internal/kernel/store/postgres.TestV13WorkflowClaimsAreFairExclusiveAndRecoverable, test:internal/kernel/store/postgres.TestV13SpawnStopsAtWorkflowBudgetAndDeadline |

---

## 8. v1.4 (Anthropic dialect, written wire contract, consolidation)

| Feature / Capability | Status | Since | Protocol / Contract | Description | Proved by |
| :--- | :---: | :---: | :--- | :--- | :--- |
| **Anthropic Messages in the Model Gateway** | **GA** | v1.4 | `internal/kernel/model/provider` | The gateway speaks `/v1/messages` beside `/chat/completions`; a provider selects the dialect with `"protocol": "anthropic"`. The system prompt, `max_tokens`, `tool_use`/`tool_result` blocks, the typed stream events and the stop reasons are translated at the wire boundary; budget, usage settlement, request-id capture, retries and the circuit breaker are shared. | test:internal/kernel/model/provider.TestAnthropicEncodeRequestShape, test:internal/kernel/model/provider.TestAnthropicExecutorAgainstFakeEndpoint |
| **Written Runtime Interface contract** | **GA** | v1.4 | `sdk/conformance/fixtures/runtime-interface-v1.json` | Golden vectors of every endpoint, executed against the Go host, the Python host and the published OpenAPI schema. A renamed field, a changed status code or a new status value fails on every side. | test:sdk/conformance.TestRuntimeInterfaceVectors, test:sdk/conformance.TestRuntimeInterfaceVectorEnums |
| **Single transport policy** | **GA** | v1.4 | `internal/runtime/bindings` | Plaintext only on loopback, HTTPS with server verification elsewhere; one implementation, used by the adapter command line and the binding table. The development escape hatch applies to the binding table only. | test:internal/runtime/bindings.TestLoadRuntimeBindingsRejectsRemotePlaintextUnlessAcknowledged, test:internal/runtime/bindings.TestValidateEndpointOverrideGuard |
| **Pinned tool grants** | **GA** | v1.4 | `capabilities.tools`, `capabilities.toolVersions` | A grant of `name@version` or `name@*` grants the tool to the runtime check and pins the version for the freeze. The two vocabularies are separated at publication time; publications written before the split keep working. | test:internal/kernel/capability.TestAuthorizerAllowsPinnedToolGrant, test:internal/kernel/capability.TestToolFreezeKeepsPinnedAndFrozenSemantics |
| **Task-backed AgentService execution** | **Implemented (runtime-dependent)** | v1.4 | `proto/fenced/service/v1` | Each replica runs as a durable Task with the normal admission, budget, placement, lease and Runtime execution; the execution and drain state persist in migration `000037`. | test:internal/kernel/store/postgres.TestServiceReferenceWorkerCompletesThroughRealRuntimeProtocol |
| **Developer CLI over the kernel model layer** | **GA** | v1.4 | `cmd/agent/stream.go` | The CLI streams through the kernel dialect, keeps its reasoning and answer channels apart, and reports the provider-reported token usage when the provider reports it. | test:cmd/agent.TestCLIExecutorResolvesTheKernelProtocol, test:cmd/agent.TestStreamLLMProtocolsMock |
