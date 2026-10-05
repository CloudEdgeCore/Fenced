# Fenced Feature Status & Capability Matrix

This matrix provides the verified implementation status for all subsystems, protocols, and APIs in Fenced as of **v1.3 (Developer Core & Dynamic Orchestration)**.

---

## 1. Process System & Workload Management

| Feature / Capability | Status | Since | Protocol / Contract | Description |
| :--- | :---: | :---: | :--- | :--- |
| **Task State Machine** | **GA** | v1.0 | `fenced.dev/v1` | `Task → Run → Attempt` lifecycle with non-preemptible execution semantics. |
| **Cooperative Cancellation** | **GA** | v1.0 | `proto/fenced/runtime/v1` | Heartbeat-driven cancellation signaling with verified TLA+ liveness convergence. |
| **Lease & Fencing Token** | **GA** | v1.0 | Kernel Store | Monotonically increasing fencing tokens preventing split-brain zombie writes. |
| **AgentService execution** | **Implemented (runtime-dependent)** | v1.2 | `proto/fenced/service/v1` | Each instance runs through a durable Task with normal admission, budgets, placement, leases, and Runtime execution. Every active replica needs a Worker slot; Host and Task execution timeouts still apply. |
| **Process Supervisor** | **Implemented** | v1.2 | `proto/fenced/service/v1` | Replica reconciliation, restart policy, backoff, and runtime lifecycle tracking. Running reflects Kernel execution state, not application readiness. |
| **Heartbeat Auto-Reap** | **Implemented** | v1.2 | `proto/fenced/service/v1` | Detects stale instance liveness and requests cancellation/replacement. Physical termination of a non-cooperative runtime requires an isolation-boundary termination hook. |
| **Rolling Upgrade & Drain** | **Limited** | v1.2 | `proto/fenced/service/v1` | Version and replica convergence with drain deadlines and cooperative task cancellation. Zero-downtime application traffic switching and readiness checks are not certified. |
| **Automatic Rollback** | **Not verified** | v1.2 | `proto/fenced/service/v1` | Explicit rollback operations exist; automatic rollback triggered by application health failures is not certified. |

---

## 2. Kernel Syscall ABI 1.0.0

| Subsystem | Syscall Range | Status | Syscalls | Description |
| :--- | :---: | :---: | :--- | :--- |
| **Tool Subsystem** | 100–199 | **GA** | `101`, `102` | Capability checking, tool execution, and cryptographic audit receipts. |
| **Model Subsystem** | 200–299 | **GA** | `201`–`204` | LLM invocation, streaming sessions, budget reservation, and incremental settlement. |
| **Memory Subsystem** | 300–399 | **GA** | `301`, `302` | Persistent key-value storage and pgvector semantic memory search. |
| **IPC Subsystem** | 400–499 | **GA** | `401`–`403` | Mailbox send, poll/drain, and acknowledgement across agent boundaries. |
| **Runtime Subsystem** | 500–599 | **GA** | `501`–`503` | Durable state checkpointing, attempt completion, and cooperative timeslice yield. |
| **Service Subsystem** | 600–699 | **GA** | `601`, `602` | Supervisor heartbeat submission and service instance discovery. |
| **Resource Subsystem** | 700–799 | **GA** | `701`–`703` | Namespace metadata inspection, quota verification, and real-time usage querying. |
| **Effect Subsystem** | 800–899 | **GA** | `801`, `802` | Fenced external mutation execution and idempotent receipt retrieval. |

---

## 3. Communication & Durable Messaging

| Feature | Status | Protocol | Guarantee |
| :--- | :---: | :--- | :--- |
| **Durable Mailbox** | **GA** | `proto/fenced/ipc/v1` | Messages persist across instance and attempt crashes. |
| **At-Least-Once Delivery** | **GA** | `proto/fenced/ipc/v1` | Network/crash resilience with unconsumed message replay. |
| **Deduplication Engine** | **GA** | `proto/fenced/ipc/v1` | Receiver mailbox receipts guarantee exactly-once application. |
| **Correlation & Reply-To** | **GA** | `proto/fenced/ipc/v1` | End-to-end distributed tracing and asynchronous RPC-style conversations. |
| **Tenant Boundary Routing** | **GA** | Kernel Router | Cross-tenant isolation with strict default-deny delivery rules. |

---

## 4. External Side-Effect Management

| Feature | Status | Protocol | Guarantee |
| :--- | :---: | :--- | :--- |
| **Monotonic Fencing** | **GA** | `proto/fenced/effect/v1` | Stale attempts cannot mutate external state (`SYSCALL_EFENCE`). |
| **Idempotency Deduplication** | **GA** | `proto/fenced/effect/v1` | Existing keys return cached `EffectReceipt` without duplicate external dispatch. |
| **Payload Integrity** | **GA** | `proto/fenced/effect/v1` | SHA-256 payload verification against duplicate keys (`SYSCALL_EINVAL`). |
| **UNKNOWN Isolation** | **GA** | `proto/fenced/effect/v1` | Ambiguous timeouts transition to `UNKNOWN` (`SYSCALL_EUNKNOWN`); auto-replay strictly forbidden. |

---

## 5. Runtimes, Isolation & Framework Ecosystem

| Runtime / Framework | Status | Supported Isolations | Conformance Certified |
| :--- | :---: | :--- | :---: |
| **Wasmtime** | **GA** | WebAssembly sandboxing, memory caps, fuel metering | Yes |
| **OCI / Container** | **GA** | runsc (gVisor), Linux cgroups v2, network namespaces | Yes |
| **LangGraph** | **GA** | Co-located adapter, Runtime Interface v1, Syscall ABI | Yes |
| **AutoGen** | **GA** | Co-located adapter, Runtime Interface v1, Syscall ABI | Yes |
| **CrewAI** | **GA** | Co-located adapter, Runtime Interface v1, Syscall ABI | Yes |
| **OpenAI Agents SDK**| **GA** | Co-located adapter, Runtime Interface v1, Syscall ABI | Yes |
| **Custom / In-House**| **GA** | Pure Python/Go/Rust adapter via standard Runtime Protocol | Yes |
| **Provider SDK (Go/Py)** | **GA** | Model, Tool, Memory, Browser, Storage, Runtime plugin protocols | Yes |
| **Third-Party Runtime SDK** | **GA** | 7-lifecycle SDK, Docker, Python, Remote HTTP reference runtimes | Yes |
| **Package Registry & Security** | **GA** | OCI + Metadata Index, 6-stage security pipeline, CLI suite | Yes |

---

## 6. Reliability & Soak Testing

| Quality Gate | Evidence / Environment | Invariant Enforced | Status |
| :--- | :--- | :--- | :---: |
| **72-Hour Soak Engine** | Dedicated fixed-host environment (`soak_test.go`) | Zero lost tasks, zero lost IPC messages, monotonic fencing | **Manual Evidence: PASS**; Scheduled CI: Pending self-hosted runners ([Evidence](evidence/soak-process-system-72h-7d.md)) |
| **7-Day Extended Soak** | Dedicated fixed-host environment | Memory/goroutine leak-free, connection pool stability | **Manual Evidence: PASS**; Scheduled CI: Pending self-hosted runners ([Evidence](evidence/soak-process-system-72h-7d.md)) |
| **Chaos Fault Injector**| In-tree chaos harness | Worker kills, lease expirations, database reconnects | **PASS** |
| **TLA+ Liveness Proofs**| TLC Model Checker | Guaranteed convergence to terminal phase without deadlocks | **PASS** |

---

## 7. Developer Core & Dynamic Orchestration (v1.3)

| Feature / Capability | Status | Since | Protocol / Contract | Description |
| :--- | :---: | :---: | :--- | :--- |
| **Developer Core CLI (`agent`)** | **GA** | v1.3 | `cmd/agent` | Cascading `agent.yaml` configuration, interactive `agent config wizard`, multi-provider LLM matrix, chain-of-thought dual-channel streaming, project scaffolding, and demo scenarios. |
| **Embedded Developer Console** | **Preview (opt-in)** | v1.3 | `agent ui --preview` | Disabled for external access; runs loopback-only for internal polishing with model connectivity, MCP tool probing, agent/tool configuration, and audit receipts. |
| **Dual-Protocol Model Layer** | **GA** | v1.3 | `cmd/agent/stream.go` | OpenAI-compatible (`/chat/completions`) and Anthropic Messages (`/v1/messages`) streaming for developer runs. |
| **Dynamic Task Spawn** | **GA** | v1.3 | `fenced.task.spawn` | Fenced child-task spawning with recursion, fan-out, and total-step guards. |
| **Workflow Budgets & Fair Sharding** | **GA** | v1.3 | Kernel Store / Orchestrator | Workflow-wide budgets and deadlines, dynamic group joins (`spawn:<parent>`), and lease-based fair sharding across orchestrator instances. |
