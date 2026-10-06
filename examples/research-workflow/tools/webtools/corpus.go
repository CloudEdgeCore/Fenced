package webtools

// Corpus returns the embedded v1 research corpus: twelve public-knowledge
// style documents on agent runtime infrastructure (2024-2026). Content is
// original summary text written for this reference application, so the demo
// needs no network access and every E2E run is deterministic.
func Corpus() []Document {
	return []Document{
		{
			SourceID: "src-001", Title: "Agent Runtime Landscape 2026: From Frameworks to Control Planes",
			URL: "https://corpus.fenced.dev/landscape-2026", PublishedAt: "2026-01-14",
			Tags:    []string{"agent runtime", "control plane", "infrastructure", "landscape"},
			Content: "The agent runtime market has split into two layers. Application frameworks (planning loops, prompt templates, memory glue) moved up-stack, while a new control-plane layer consolidated concerns that used to be scattered inside frameworks: identity, capability admission, budget enforcement, scheduling, sandboxed execution and durable recovery. Control planes treat an agent task as a governed workload, not a script. The visible trend for the next three years is consolidation around kernel-like runtimes that expose gateways for models and tools, with frameworks becoming thin clients. Buyers now ask who audits a tool call, not which prompt template was used. Expect the framework layer to commoditize while control planes capture the compliance budget.",
		},
		{
			SourceID: "src-002", Title: "Sandboxing Agents: gVisor, Firecracker and Wasm Compared",
			URL: "https://corpus.fenced.dev/sandboxing", PublishedAt: "2025-11-02",
			Tags:    []string{"sandbox", "gvisor", "firecracker", "wasm", "isolation"},
			Content: "Three isolation technologies dominate agent sandboxing debates. gVisor intercepts syscalls in user space and boots in tens of milliseconds, making it practical for per-task reader agents that fetch untrusted web content. Firecracker microVMs give stronger hardware virtualization isolation at the cost of roughly one hundred twenty five milliseconds boot and higher memory floors, fitting long-lived code-execution pools. Wasm runtimes start in single-digit milliseconds with capability-based imports but break on native dependencies. Production platforms increasingly mix all three behind one scheduler abstraction: placement policies choose the cheapest class that satisfies the trust boundary of each tool call. The differentiator is not raw isolation strength but cold-start economics under bursty multi-agent fan-out.",
		},
		{
			SourceID: "src-003", Title: "Durable Execution for AI Workloads: Outbox Patterns at Scale",
			URL: "https://corpus.fenced.dev/durable-execution", PublishedAt: "2025-09-18",
			Tags:    []string{"durable execution", "outbox", "postgres", "recovery"},
			Content: "Agent workflows fail mid-flight: providers return 429s, workers crash between a tool side effect and its bookkeeping. Durable execution engines answer with write-ahead intent: state transitions are committed transactionally next to the business data, then projected to queues through an outbox. Postgres remains the dominant substrate because leasing and fencing tokens map cleanly onto conditional updates. Recovery controllers re-drive attempts whose leases expire, and fencing tokens make zombie writes harmless. Teams report that idempotency keys derived from attempt identity plus canonical arguments eliminate duplicate side effects even under at-least-once redelivery. The lesson: durability is a database design decision, not a queue feature.",
		},
		{
			SourceID: "src-004", Title: "Model Gateways: Why Credentials Never Belong in Agent Sandboxes",
			URL: "https://corpus.fenced.dev/model-gateways", PublishedAt: "2025-10-07",
			Tags:    []string{"model gateway", "credentials", "proxy", "budget"},
			Content: "A model gateway terminates provider credentials centrally and exposes invocation as a governed RPC: policy checks, budget reservation, usage settlement and audit happen before a token reaches the wire. Sandboxed agents see only logical model references; the platform resolves them to providers. Beyond security, centralization enables exact cost accounting per attempt, cross-provider failover, and semantic caching. Streaming complicates the contract - gateways must translate server-sent events into chunked RPCs while preserving finish reasons and usage frames. The 2026 pattern is gateway-side tool resolution: agents offer tool names, the gateway attaches schemas from a versioned registry, so sandboxes can never widen their own tool surface.",
		},
		{
			SourceID: "src-005", Title: "Multi-Agent Research Pipelines in Production: A Retrospective",
			URL: "https://corpus.fenced.dev/research-pipelines", PublishedAt: "2026-02-20",
			Tags:    []string{"multi-agent", "research pipeline", "fan-out", "critic"},
			Content: "Deep-research products converged on the same topology: a planner decomposes the question, search tasks fan out per sub-question, reader tasks extract claims per source, an analyst clusters evidence, and a critic decides whether another round is needed. Three lessons recur. First, pass large artifacts through a shared memory store keyed by workflow, never through prompts. Second, bound critic rounds explicitly - three rounds capture most of the recall improvement while capping cost curves. Third, citation coverage above ninety percent requires validating against extracted evidence records, not free-text references. Failures cluster in joins: waiting for dynamic fan-outs needs first-class group semantics or teams hand-roll polling loops that deadlock under load.",
		},
		{
			SourceID: "src-006", Title: "Capability Tokens for Agents: Least Privilege That Actually Holds",
			URL: "https://corpus.fenced.dev/capability-tokens", PublishedAt: "2025-08-29",
			Tags:    []string{"capability", "least privilege", "allowlist", "spawn"},
			Content: "Capability systems for agents matured from prompt-level disclaimers to enforced grants: immutable agent versions declare allowed models, tools, memory namespaces and spawn targets, and gateways deny by default. Wildcard namespaces such as team-slash-star balance ergonomics and blast radius. Dynamic spawning introduced a second axis - a parent may only create children whose versions appear in its allowlist, with depth and fan-out caps enforced inside the same transaction as the spawn itself. Auditors increasingly require proof that a denial happened, so denials are structured outcomes with stable codes rather than exceptions. The frontier is capability attenuation: children inheriting strictly narrower grants than their parents.",
		},
		{
			SourceID: "src-007", Title: "The Economics of Agent Fan-Out: Cost Curves and Budget Guards",
			URL: "https://corpus.fenced.dev/fan-out-economics", PublishedAt: "2026-03-05",
			Tags:    []string{"economics", "budget", "cost", "tokens", "fan-out"},
			Content: "Unbounded agent fan-out produces quadratic cost surprises. Mature platforms reserve budget at task creation, settle exactly after each model call, and expose committed-versus-settled drift as an SLO. Reservation matters because settlement lags: a thousand spawned readers can commit spend before the first response stream finishes. Workflow budgets need three dimensions - tasks, tokens and currency - plus hard walls on critic rounds and sources per question. Operators watch two drift metrics: reserved-but-never-settled tokens after crashes, and settled-but-unrecorded cost after provider retries. Both converge to zero when reservations are released in the same transaction that records the terminal attempt state.",
		},
		{
			SourceID: "src-008", Title: "Checkpointing Long-Horizon Agents Without Killing Throughput",
			URL: "https://corpus.fenced.dev/checkpointing", PublishedAt: "2025-12-11",
			Tags:    []string{"checkpoint", "recovery", "long-horizon", "state"},
			Content: "Long-horizon agents checkpoint logical state - conversation digests, cursor positions, pending tool calls - instead of heap snapshots. The runtime interface exposes checkpoint and restore as versioned documents with schema identifiers, letting upgrades skip incompatible states deliberately. Interval-based checkpoints trade event-log size against replay time; production systems checkpoint after every external side effect and at least once per minute otherwise. Restore must be idempotent: replays of identical restores are accepted silently, which keeps lease-expiry recovery paths simple. The measurable win is tail latency - recovered attempts resume within seconds instead of restarting multi-minute research chains from scratch.",
		},
		{
			SourceID: "src-009", Title: "SSRF in the Age of Agentic Browsing: Fetch Policies That Survive Review",
			URL: "https://corpus.fenced.dev/ssrf-fetch-policies", PublishedAt: "2025-07-22",
			Tags:    []string{"ssrf", "security", "fetch", "egress"},
			Content: "Agents that fetch URLs walk into the classic SSRF minefield with new energy. Reviewed designs share properties: scheme allowlists that exclude file and gopher, credential-free URLs, resolution-time rejection of loopback, link-local, private and shared address ranges, redirect caps, body size limits and per-host timeouts. DNS rebinding is answered by pinning the first resolved address for the lifetime of the connection. Audit logs record the requested URL, the resolved address and the policy verdict, because incident review asks what the agent tried, not just what succeeded. Allowlist-first deployments - where fetchable hosts come from a registry - pass security review fastest.",
		},
		{
			SourceID: "src-010", Title: "Scheduler Placement for Heterogeneous Agent Fleets",
			URL: "https://corpus.fenced.dev/scheduler-placement", PublishedAt: "2026-01-30",
			Tags:    []string{"scheduler", "placement", "runtime pool", "capacity"},
			Content: "Heterogeneous fleets need heterogeneous placement: reasoning-heavy planners want LLM concurrency slots, search agents want cheap egress-heavy workers, readers fetching untrusted content belong in hardened sandboxes. Pool-based scheduling expresses this as declared runtime classes per task plus registered capacity ledgers per pool. Placement fails closed when no pool satisfies the class constraint, which turns misconfiguration into a visible scheduling error instead of a security incident. Bin-packing on cpu, memory and llm-concurrency dimensions keeps utilization high; preemption remains rare because agent tasks are short. The open problem is locality-aware placement when memory stores are region-scoped.",
		},
		{
			SourceID: "src-011", Title: "Audit Trails That Reconstruct Decisions: Beyond Request Logs",
			URL: "https://corpus.fenced.dev/audit-trails", PublishedAt: "2025-10-25",
			Tags:    []string{"audit", "compliance", "traceability", "append-only"},
			Content: "Compliance-grade agent platforms reconstruct any decision from an append-only audit chain: which principal published which agent version, which attempt invoked which model with how many tokens at what cost, which tool call touched which resource and whether it was a fresh execution or an idempotent replay. Hash-chained entries let verifiers detect truncation without trusted hardware. Export formats matter as much as capture - auditors want self-contained bundles they can verify offline. The hardest requirement is correlation across gateways: model, tool and spawn events must share the attempt identity spine, or the reconstruction degrades into guesswork across siloed logs.",
		},
		{
			SourceID: "src-012", Title: "Remote Runtimes and the Coming Federation of Agent Capacity",
			URL: "https://corpus.fenced.dev/remote-runtimes", PublishedAt: "2026-04-01",
			Tags:    []string{"remote runtime", "federation", "mtls", "spiffe"},
			Content: "Capacity federates: control planes lease execution capacity from remote worker pools operated by other teams or vendors. The protocol surface stays small - poll assignments, heartbeat leases, stream events, report results - over mutually authenticated channels with workload identities like SPIFFE. Tenant binding travels with the workload identity so a compromised worker cannot impersonate another tenant's attempts. Transport requirements are non-negotiable in production: client certificates required, plaintext remote endpoints refused at configuration load. Expect hybrid topologies where sensitive readers stay on local gVisor pools while elastic overflow spills to federated capacity, selected by the same placement policy engine that already understands runtime classes.",
		},
	}
}
