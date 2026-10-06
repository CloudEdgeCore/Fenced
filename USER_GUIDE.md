# Fenced User Guide & Developer Manual

The comprehensive **Fenced User Guide & Developer Manual** is maintained in:

👉 **[docs/user-guide.md](docs/user-guide.md)**

---

### Quick Navigation

- [1. 系统概览与核心概念](docs/user-guide.md#1-系统概览与核心概念)
- [2. 环境准备与本地快速启动 (Docker Compose, Postgres, NATS)](docs/user-guide.md#2-环境准备与本地快速启动)
- [3. Agent 开发与生命周期管理 (fenced init, validate, run, logs)](docs/user-guide.md#3-agent-开发与生命周期管理)
- [4. Agent Package Registry 与 6 阶段安全门禁 (build, sign, push, verify, install)](docs/user-guide.md#4-agent-package-registry-与-6-阶段安全门禁)
- [5. 第三方 Runtime SDK 与一致性测试 (runtime init, runtime test, conformance)](docs/user-guide.md#5-第三方-runtime-sdk-与一致性测试)
- [6. Unified Provider SDK 插件生态 (Model, Tool, Memory, Browser, Storage, Runtime)](docs/user-guide.md#6-unified-provider-sdk-插件生态)
- [7. 常驻服务与 Supervisor 进程监管 (AgentService, rolling upgrade, drain)](docs/user-guide.md#7-常驻服务与-supervisor-进程监管)
- [8. 跨 Agent 通信 (Durable IPC, at-least-once, mailbox)](docs/user-guide.md#8-跨-agent-通信-durable-ipc)
- [9. 外部副作用引擎 (External Effect Engine, fencing tokens, idempotency)](docs/user-guide.md#9-外部副作用引擎-external-effect-engine)
- [10. 多 Agent 工作流 DAG 编排 (workflow create, workflow tree)](docs/user-guide.md#10-多-agent-工作流-dag-编排)
- [11. 多租户治理、安全与审计 (namespace, OPA Rego policy, /v1/audit/export)](docs/user-guide.md#11-多租户治理安全与审计)
- [12. CLI 命令族与配置速查表 (command cheat sheet & environment variables)](docs/user-guide.md#12-cli-命令族与配置速查表)

---

### Other Documentation

- [README.md](README.md) — System Overview & Architecture Highlights
- [Architecture Specification](docs/architecture/ARCHITECTURE.md) — Detailed Kernel Design
- [Feature Status Matrix](docs/feature-status.md) — Complete Feature & Stability Status
- [v1.2 Contract Freeze](docs/contracts/v1.2-contract-freeze.md) — Frozen Contracts & Syscall ABI 1.0.0
- [Ecosystem Guide](docs/ecosystem/README.md) — Ecosystem Integration Strategy
