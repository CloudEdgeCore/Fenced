# Fenced 用户使用与开发实战指南 (User Guide)

欢迎使用 **Fenced** —— 面向生产级 AI Agent 的安全发布、调度、执行、容灾、治理与审计的云原生操作系统内核与控制面平台。

本指南旨在为开发者、框架作者与运维工程师提供从零到一的完整实战指导，涵盖环境部署、Agent 开发、包注册表、Runtime SDK、Provider 扩展、常驻守护服务、跨 Agent IPC 通信以及外部副作用防护等全套功能。

---

## 目录 (Table of Contents)

1. [系统概览与核心概念](#1-系统概览与核心概念)
2. [环境准备与本地快速启动](#2-环境准备与本地快速启动)
3. [Agent 开发与生命周期管理](#3-agent-开发与生命周期管理)
4. [Agent Package Registry 与 6 阶段安全门禁](#4-agent-package-registry-与-6-阶段安全门禁)
5. [第三方 Runtime SDK 与一致性测试](#5-第三方-runtime-sdk-与一致性测试)
6. [Unified Provider SDK 插件生态](#6-unified-provider-sdk-插件生态)
7. [常驻服务与 Supervisor 进程监管](#7-常驻服务与-supervisor-进程监管)
8. [跨 Agent 通信 (Durable IPC)](#8-跨-agent-通信-durable-ipc)
9. [外部副作用引擎 (External Effect Engine)](#9-外部副作用引擎-external-effect-engine)
10. [多 Agent 工作流 DAG 编排](#10-多-agent-工作流-dag-编排)
11. [多租户治理、安全与审计](#11-多租户治理安全与审计)
12. [CLI 命令族与配置速查表](#12-cli-命令族与配置速查表)

---

## 1. 系统概览与核心概念

Fenced 不是一个简单的聊天窗口或无代码流程图工具，而是解决 AI Agent 真正进入企业级生产环境时面临的核心系统级工程挑战：**不可变版本控制、确定性执行隔离、精确成本预算结算、长时故障热恢复、密码学供应链签名与防重入副作用防护**。

### 1.1 系统架构图

```mermaid
flowchart TD
    subgraph ClientLayer["客户端与生态层 (Client & Ecosystem)"]
        CLI["Fenced CLI (agent / fenced)"]
        Registry["Agent Package Registry (OCI)"]
        SDK["Provider SDK & Runtime SDK"]
        Frameworks["LangGraph / AutoGen / CrewAI"]
    end

    subgraph ControlPlane["控制平面 (Control Plane)"]
        API["Control API Server (fenced-control)"]
        Scheduler["Scheduler & Placement Engine"]
        Supervisor["Supervisor & Recovery Controller"]
        Admission["Admission & Rego Policy Engine"]
    end

    subgraph StorageLayer["持久化与总线 (Durable State)"]
        PG[("PostgreSQL 16+ (pgvector / FTS)")]
        NATS[("NATS JetStream (Durable IPC)")]
    end

    subgraph Gateways["安全网关 (Security Gateways)"]
        GW_Tool["Tool Gateway"]
        GW_Model["Model Gateway"]
        GW_Memory["Memory Gateway"]
        GW_Effect["Effect Engine (Fencing Tokens)"]
    end

    subgraph RuntimeLayer["执行沙箱 (Execution Runtimes)"]
        Wasm["Wasmtime Sandbox"]
        OCI["OCI / gVisor Container"]
        CustomRT["Third-Party Runtimes"]
    end

    CLI --> API
    Registry --> API
    Frameworks --> CLI
    API --> Admission
    Admission --> PG
    Scheduler --> PG
    Scheduler --> RuntimeLayer
    Supervisor --> PG
    Supervisor --> RuntimeLayer
    RuntimeLayer --> Gateways
    Gateways --> PG
    RuntimeLayer --> NATS
```

### 1.2 核心概念与实体模型

| 实体概念 | 说明 | 对应文件/契约 |
| :--- | :--- | :--- |
| **Tenant & Namespace** | 多租户隔离的一级组织单位，所有的配额、预算和策略均绑定到租户。 | [`internal/kernel/tenant`](../internal/kernel/tenant) |
| **AgentManifest** | 声明式的 Agent 元数据描述文件（名称、版本、运行类、所需能力、硬性资源上限与逻辑检查点模式）。 | [`internal/kernel/agentversion/manifest.go`](../internal/kernel/agentversion/manifest.go) |
| **AgentVersion** | 发布到控制面后的只读、不可变版本对象，内容与哈希完全绑定。 | [`internal/kernel/agentversion`](../internal/kernel/agentversion) |
| **AgentPackage** | 符合 OCI 规范的数字签名包，携带 SBOM 清单、Spec 摘要与 Provenance 来源凭据。 | [`internal/kernel/agentpkg`](../internal/kernel/agentpkg) |
| **Task & Run & Attempt** | **单次批处理模型**：一个 Task 代表一次业务目标；包含若干次重试的 Run；每次真正派发到 Worker 执行的实体为 Attempt（非可抢占、租约栅栏保护）。 | [`internal/kernel/task`](../internal/kernel/task) |
| **Service & Instance** | **受监管副本模型**：每个服务实例绑定一个持久化 Task，复用准入、调度、租约和 Runtime 执行；Supervisor 管理副本、心跳与重启策略。 | [`proto/fenced/service/v1/service.proto`](../proto/fenced/service/v1/service.proto) |
| **Syscall ABI 1.0.0** | 内核标准系统调用规范，抽象了模型推理、工具执行、内存存取、IPC 通信、服务调用等 8 大子系统。 | [`proto/fenced/syscall/v1/syscall.proto`](../proto/fenced/syscall/v1/syscall.proto) |

---

## 2. 环境准备与本地快速启动

### 2.1 环境要求

- **操作系统**：Linux / macOS / Windows (amd64 或 arm64)
- **依赖工具**：
  - Docker 24.0+ 与 Docker Compose 2.20+
  - Go 1.26+（如果需要从源码构建；CI 使用 1.26.6）
  - Rust 1.97+（如果需要构建 Wasmtime 原生运行时组件）

### 2.2 启动本地基础组件栈

项目已在 [`deploy/dev/compose.yaml`](../deploy/dev/compose.yaml) 中编排了完整的基础设施环境，包括支持向量检索的 PostgreSQL 18、NATS JetStream、以及完整的 OpenTelemetry 可观测性套件。

在项目根目录下执行：

```bash
# 启动数据库与消息总线
docker compose -f deploy/dev/compose.yaml up -d postgres nats

# （可选）一并启动包含 Prometheus, Grafana, Tempo, Loki 的全套可观测套件
docker compose -f deploy/dev/compose.yaml --profile observability up -d
```

默认端口与凭证：
- **PostgreSQL**: `127.0.0.1:55432`，用户 `fenced`，密码 `fenced-dev-only`，数据库 `fenced`
- **NATS JetStream**: `127.0.0.1:54222`
- **Grafana 监控看板**: `http://127.0.0.1:3300` (无需密码)
- **Prometheus 指标**: `http://127.0.0.1:9093`

### 2.3 编译 Fenced 核心组件

```bash
# 编译统一开发者 CLI（agent：配置向导、模型连通性、脚手架、内部预览控制台）
go build -o bin/agent ./cmd/agent

# 编译稳定工作流 CLI（fenced：发布、运行、日志、工作流、服务）
go build -o bin/fenced ./cmd/fenced

# 编译控制面 API 服务
go build -o bin/fenced-control ./cmd/fenced-control

# 编译集群调度与恢复控制器
go build -o bin/fenced-controller ./cmd/fenced-controller

# 编译网关组件
go build -o bin/fenced-gateway ./cmd/fenced-gateway

# 编译数据库迁移工具
go build -o bin/fenced-migrate ./cmd/fenced-migrate
```

验证安装：
```bash
./bin/fenced version   # Fenced 1.3.0.0 (semver v1.3.0, GA)
./bin/agent version     # agent CLI 1.3.0 (product: Fenced 1.3.0.0, syscall ABI: 1.0.0)
```

### 2.4 初始化数据库架构

执行数据库全量迁移，建立多租户、任务状态机、Lease 租约、IPC 邮箱与安全策略表结构：

```bash
export DATABASE_URL="postgres://fenced:fenced-dev-only@127.0.0.1:55432/fenced?sslmode=disable"

# 执行数据库迁移（fenced migrate 子命令仅用于 manifest 版本提升）
./bin/fenced-migrate -database-url "$DATABASE_URL"
```

### 2.5 启动内核服务

在两个终端窗口中分别启动控制面服务与后台调度协调器：

**终端 1：启动控制面 API Server**
```bash
./bin/fenced-control \
  -database-url "$DATABASE_URL" \
  -listen "127.0.0.1:8080" \
  -dev-tenant dev
```

**终端 2：启动调度与容灾恢复控制器**
```bash
./bin/fenced-controller \
  -database-url "$DATABASE_URL" \
  -controller-id "controller-node-01" \
  -runtime-pools deploy/dev/runtime-pools.json \
  -tenant-policies deploy/dev/tenant-policies.json \
  -dev-mode
```

> 上述 `-dev-tenant` / `-dev-mode` 仅用于回环地址上的本地开发；生产模式要求 HTTPS、OIDC 与 SPIFFE mTLS，缺失时进程拒绝启动。

---

## 3. Agent 开发与生命周期管理

### 3.1 项目初始化 (`fenced init`)

Fenced 提供了开箱即用的模板脚手架，支持 Go、Python、LangGraph 与 Agent-to-Agent (A2A) 架构：

```bash
# 使用 Python 模板初始化新 Agent（-adapter 可选 go / python / langgraph / a2a）
./bin/fenced init -dir my-agent -name my-agent -adapter python

cd my-agent
ls -l
```

生成的项目目录结构包含：
- `agent.json`：Fenced 声明式规格配置
- `server.py`：业务逻辑入口，遵循 Fenced Runtime 协议（go 模板生成 `main.go`）

### 3.2 深入理解 `agent.json` 规格

```json
{
  "apiVersion": "fenced.dev/v1",
  "kind": "AgentManifest",
  "metadata": {
    "name": "research-assistant",
    "version": "1.0.0",
    "namespace": "default"
  },
  "spec": {
    "runtimeClassPolicy": {
      "allowed": ["oci", "wasm", "python-native"],
      "preferred": "python-native"
    },
    "runtimes": [
      {
        "class": "python-native",
        "interface": "fenced.runtime.interface/v1",
        "runtimeABI": "v1",
        "entrypoint": ["python3", "main.py"]
      }
    ],
    "capabilities": {
      "tools": ["web-search@v1", "calculator@v1"],
      "models": ["gpt-4o", "claude-3-5-sonnet"],
      "memory": ["project/*"],
      "secrets": ["OPENAI_API_KEY"]
    },
    "resources": {
      "cpuMillis": 500,
      "memoryMiB": 512,
      "workspaceBytes": 67108864
    },
    "budget": {
      "tokens": 100000,
      "costUsd": 5.0,
      "toolCalls": 50,
      "wallSeconds": 1800
    },
    "checkpoint": {
      "mode": "logical",
      "schemaVersion": "research-state/v1",
      "intervalSeconds": 60
    }
  }
}
```

> [!IMPORTANT]
> **能力默认拒绝 (Strict Default-Deny)**：`capabilities` 字段中的每一项授权必须显式声明。如果省略或者传入 `null`，内核准入控制器将在发布与提交阶段直接拒绝（HTTP 422 Unprocessable Entity）。

### 3.3 规范静态校验 (`fenced validate`)

在发布前使用静态校验器检查语法、资源边界与能力格式：

```bash
./bin/fenced validate -manifest agent.json
```

若通过，会输出类似 `manifest OK ref=default/research-assistant@1.0.0 digest=... runtimes=1`；校验失败时返回非零退出码并打印具体原因。

### 3.4 提交与运行单次任务 (`fenced run`)

向 Fenced 内核提交一个具体的执行任务：

```bash
./bin/fenced run \
  -endpoint "http://127.0.0.1:8080" \
  -agent "default/research-assistant@1.0.0" \
  -goal "分析近期 AI 操作系统架构设计要点并输出 Markdown 简报" \
  -namespace "default" \
  -spec "task-spec.json"
```

其中 `-spec` 可选，用于传入工作负载 JSON；省略时请求体 `spec` 为空对象。
命令将返回任务全局唯一标识符 `TaskID`（UUID）。

### 3.5 实时查看日志与事件流 (`fenced logs`)

Fenced 将所有生命周期事件、工具调用、思考过程与标准输出持久化为流式事件总线：

```bash
# 流式跟踪任务事件（SSE，直到服务端关闭连接）
./bin/fenced logs -endpoint "http://127.0.0.1:8080" -task "9a2f7c01-4b2e-4f1a-9c3d-7e5b8a1d2f30"
```

> `-task` 必须是任务 UUID。

---

## 4. Agent Package Registry 与 6 阶段安全门禁

在企业生产级部署中，未经安全审计和密码学防伪的 Agent 代码绝对不允许直接投入运行。Fenced 提供了兼容 OCI Registry 标准的包管理器与**强制性 6 阶段安全门禁流水线**。

```mermaid
flowchart TD
    S1["Stage 1: 注册表准入与元数据校验<br/>(Registry Admission & Schema Validation)"]
    S2["Stage 2: 密码学签名核验<br/>(Ed25519 Cryptographic Signature Verification)"]
    S3["Stage 3: 摘要与 SBOM 供应链比对<br/>(SHA-256 Spec, Layer & SBOM Digest Matching)"]
    S4["Stage 4: 内核 Syscall ABI 兼容性核验<br/>(Syscall ABI 1.0.0 Compatibility Check)"]
    S5["Stage 5: 严格 Default-Deny 租户权限匹配<br/>(Capability Grant vs Tenant Admission Policy)"]
    S6["Stage 6: 激活不可变 AgentVersion 记录<br/>(Immutable AgentVersion Publication)"]

    S1 --> S2 --> S3 --> S4 --> S5 --> S6
```

### 4.1 开发者实战流水线

#### 第 1 步：登录注册表
```bash
./bin/fenced login -registry "https://registry.fenced.dev" -token "developer-api-token"
```

#### 第 2 步：构建规范供应清单 (Build)
```bash
./bin/fenced package build \
  -manifest "agent.json" \
  -builder "alice@corp.internal" \
  -workflow "github-actions-release" \
  -git-commit "7c3b88a" \
  -out "package-manifest.json"
```

此命令将固定 Agent Spec 的 SHA-256 紧凑摘要，并生成包含 SBOM、Permissions、MemorySchema 等元数据的供应清单。

#### 第 3 步：Ed25519 密码学签名 (Sign)
使用发行者的 Ed25519 私钥为该包实施数字签名：

```bash
./bin/fenced package sign \
  -package "package-manifest.json" \
  -key-id "release-key-2026" \
  -private-key "YOUR_BASE64_ED25519_PRIVATE_KEY" \
  -out "package.signed.json"
```

#### 第 4 步：推送到 OCI 注册表 (Push)
```bash
./bin/fenced package push \
  -package "package.signed.json" \
  -registry "https://registry.fenced.dev"
```

#### 第 5 步：在线检索公开与企业包 (Search)
```bash
./bin/fenced package search -query "research" -capability "tools:web-search"
```

#### 第 6 步：完整性验证与安装 (Verify & Install)
在生产租户环境下安装已签名的包，强制经过 6 阶段安全门禁：

```bash
./bin/fenced package install \
  -package "package.signed.json" \
  -public-key "RELEASE_PUBLIC_KEY_BASE64" \
  -tenant "production-finance"
```

> [!CAUTION]
> 任何包若篡改了即便一个字节的配置，或依赖了租户未授权的模型/敏感密钥，都将在 Stage 2、3 或 5 立即阻断并记录审计事件，保证零绕过（Zero-Bypass）。

---

## 5. 第三方 Runtime SDK 与一致性测试

Fenced 允许第三方引擎（例如独立的 Docker 容器沙箱、自定义 Python 执行器、远程 HTTP 服务）接入作为统一运行时，只要适配器符合 `fenced.runtime.interface/v1` 协议。

### 5.1 快速脚手架 (`fenced runtime init`)

```bash
# 初始化一个远程 HTTP 运行时骨架
./bin/fenced runtime init my-runtime --template http

cd my-runtime
```

### 5.2 核心实现接口：7 大生命周期钩子

第三方开发者仅需在 Go / Python 中实现统一的 [`LifecycleHandler`](../sdk/runtimesdk/runtime.go)：

```go
package main

import (
	"context"
	"net/http"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/CloudEdgeCore/Fenced/sdk/runtimesdk"
)

type MyCustomRuntime struct{}

// 1. Health 探针与版本协商
func (r *MyCustomRuntime) Health(ctx context.Context) (agent.HealthResponse, error) {
	return agent.HealthResponse{
		Status:           "SERVING",
		ProtocolVersions: []string{agent.ProtocolVersion},
		Adapter:          "my-custom-runtime",
		MaxConcurrent:    50,
	}, nil
}

// 2. Start 幂等启动
func (r *MyCustomRuntime) Start(ctx context.Context, req agent.StartRequest) (agent.StartResponse, error) {
	// 初始化容器/进程并注入上下文
	return agent.StartResponse{ExecutionID: req.ExecutionID, Status: agent.StatusRunning}, nil
}

// 3. Event 基于游标事件拉取
func (r *MyCustomRuntime) Event(ctx context.Context, executionID string, after int64) (agent.EventList, error) {
	return agent.EventList{ExecutionID: executionID, Events: []agent.Event{}}, nil
}

// 4. Result 终态轮询
func (r *MyCustomRuntime) Result(ctx context.Context, executionID string) (agent.Result, error) {
	return agent.Result{ExecutionID: executionID, Status: agent.StatusSucceeded}, nil
}

// 5. Checkpoint 逻辑状态快照
func (r *MyCustomRuntime) Checkpoint(ctx context.Context, executionID string) (agent.CheckpointResponse, error) {
	return agent.CheckpointResponse{ExecutionID: executionID}, nil
}

// 6. Restore 跨节点热状态恢复
func (r *MyCustomRuntime) Restore(ctx context.Context, req agent.RestoreRequest) (agent.RestoreResponse, error) {
	return agent.RestoreResponse{ExecutionID: req.ExecutionID, Restored: true}, nil
}

// 7. Stop 优雅安全停机
func (r *MyCustomRuntime) Stop(ctx context.Context, executionID string) (agent.StopResponse, error) {
	return agent.StopResponse{ExecutionID: executionID, Status: agent.StatusCancelled}, nil
}

func main() {
	handler := &MyCustomRuntime{}
	// 使用 runtimesdk 一键暴露标准 HTTP 服务
	_ = runtimesdk.Serve(handler, ":8088")
}
```

### 5.3 运行官方一致性合规套件 (Conformance Suite)

启动你的 Runtime 服务后，运行内置的一致性认证命令：

```bash
./bin/fenced runtime test http://127.0.0.1:8088
```

或使用底层的详细验证套件：
```bash
./bin/fenced conformance -endpoint http://127.0.0.1:8088
```

合规套件将自动注入 12 项黑盒破坏性测试用例：
1. `health`：状态 SERVING 校验
2. `protocol-negotiation`：协议头 `Fenced-Runtime-Interface` 匹配
3. `start`：首次执行正常启动
4. `idempotency`：相同参数重入响应校验
5. `conflict`：冲突参数拒绝校验 (409 Conflict)
6. `event`：实时事件收集
7. `event-cursor`：游标递增与重放去重
8. `result`：有效 JSON 终态校验
9. `checkpoint`：模式版本合法性断言
10. `restore`：快照还原连贯性校验
11. `stop`：受控终止状态核验
12. `default-deny-capabilities`：缺省拒绝隐式权限 (422 Unprocessable Entity)

全部通过后输出：
```text
Fenced Runtime Interface Conformance: PASS
Endpoint: http://127.0.0.1:8088
Protocol: fenced.runtime.interface/v1
Checks Passed: 12/12
Status: CERTIFIED COMPATIBLE
```

---

## 6. Unified Provider SDK 插件生态

为实现新增大模型、工具、向量库或浏览器能力时**完全无需修改 Fenced 内核代码（Zero Kernel Modifications）**，Fenced 提供了 Go 与 Python 统一 Provider SDK（[`sdk/provider/`](../sdk/provider/)）。

### 6.1 支持的 6 大 Provider 类别

- **`ModelProvider`**：模型调用与 Token 记账
- **`ToolProvider`**：结构化工具参数校验与本地/远程执行
- **`MemoryProvider`**：向量余弦检索与短期上下文持久化
- **`BrowserProvider`**：无头浏览器驱动与页面截图 DOM 提取
- **`StorageProvider`**：CAS 内容寻址对象存取
- **`RuntimeProvider`**：执行环境创建与沙箱编排

### 6.2 官方参考 Provider 使用示例

#### 1. 模型提供商（OpenAIProvider）
```go
import "github.com/CloudEdgeCore/Fenced/sdk/provider"

openai, err := provider.NewOpenAIProvider(provider.OpenAIConfig{
    APIKey:  os.Getenv("OPENAI_API_KEY"),
    BaseURL: "https://api.openai.com/v1",
    Model:   "gpt-4o",
})

resp, err := openai.Generate(ctx, provider.GenerateRequest{
    Messages: []provider.Message{
        {Role: "user", Content: "你好，请自我介绍"},
    },
})
```

#### 2. 浏览器自动化提供商（BrowserProvider）
```go
browser := provider.NewBrowserProvider(provider.BrowserConfig{
    Headless: true,
})

_ = browser.Navigate(ctx, "https://fenced.dev")
screenshot, _ := browser.Screenshot(ctx)
```

#### 3. 向量持久化提供商（PostgresMemoryProvider）
```go
memory := provider.NewPostgresMemoryProvider(provider.PostgresConfig{
    ConnectionString: "postgres://fenced:fenced-dev-only@127.0.0.1:55432/fenced?sslmode=disable",
    Dimension:        1536,
})

// 语义余弦检索
results, err := memory.Search(ctx, queryVector, 5, 0.8)
```

#### 4. 动态注册中心（零内核侵入）
```go
reg := provider.NewRegistry()
_ = reg.Register(openai)
_ = reg.Register(browser)
_ = reg.Register(memory)

// 随时按需查找
modelProv, _ := reg.GetModelProvider("openai")
```

---

## 7. 常驻服务与 Supervisor 进程监管

**AgentService** 声明需要维持的 Agent 副本数及重启策略。Supervisor 为每个实例提交一个持久化 Task，实际执行沿用 `Task → Run → Attempt → Runtime` 链路，因此服务同样受到版本准入、租户策略、预算、调度容量和执行时限约束。重启创建新的执行任务，实例的 `taskId` 可用于追踪普通 Task 状态和事件。

```mermaid
stateDiagram-v2
    [*] --> Starting: Create Task / Schedule
    Starting --> Running: Runtime attempt and heartbeat
    Running --> Degraded: Heartbeat overdue
    Degraded --> Running: Heartbeat renewed
    Running --> Terminating: Stop / Drain
    Running --> Crashed: Attempt failed
    Crashed --> Starting: Supervisor Restart (Backoff)
    Terminating --> [*]: Clean Exit
```

### 7.1 服务注册与启动

先发布与命名空间匹配的 AgentVersion，并启动允许其运行类的 Worker 及 Runtime Interface 端点；相应运行池必须已向调度器注册。以下命令假定 `agent.json` 声明了默认命名空间下的 `customer-service-bot@1.0.0`，允许 `remote` 运行类，且版本预算不小于任务预算。`-spec` 必须指向 JSON 对象，包含普通 Task 所需的 `budget`、`placement` 等字段；这些内容仍由服务端准入校验。

```bash
./bin/fenced publish -manifest agent.json

cat > service-task-spec.json <<'JSON'
{
  "budget": {
    "tokens": 2000,
    "costUsd": 0.10,
    "toolCalls": 8,
    "wallSeconds": 120
  },
  "placement": {
    "runtimeClasses": ["remote"],
    "preferredClass": "remote",
    "region": "cn-east",
    "cpuMillis": 100,
    "memoryMiB": 128,
    "workspaceBytes": 1048576,
    "llmConcurrency": 1
  }
}
JSON

# 提交一个受监管副本；调度与实际启动异步进行
./bin/fenced service create \
  -name "customer-service-bot" \
  -agent "customer-service-bot@1.0.0" \
  -namespace "default" \
  -spec service-task-spec.json \
  -runtime-class "remote" \
  -replicas 1 \
  -restart-policy "Always"

# 将创建响应中的 id 填入 SERVICE_ID
SERVICE_ID=your-service-id
./bin/fenced service instances "$SERVICE_ID"

# 将实例响应中的 taskId 填入 TASK_ID，跟踪实际执行事件
TASK_ID=your-task-id
./bin/fenced logs -task "$TASK_ID"
```

默认命名空间的规范版本引用写作 `name@version`；其他命名空间写作 `namespace/name@version`，必须与 `-namespace` 相同。`-runtime-class` 可省略，此时按版本策略和工作负载 placement 调度。API 的 `spec.agentVersionRef` 固定服务使用的版本，`spec.workloadSpec` 保存上述 Task 配置。

升级控制面和 controller 前，先应用数据库迁移 `000037`。旧 Supervisor 的实例只记录状态，没有实际 Runtime Task；迁移会清除这些实例的虚假就绪状态，并结束没有底层进程的旧 drain 状态。为已有服务补齐已发布的 `spec.agentVersionRef` 和 `spec.workloadSpec` 后，Supervisor 才会创建受准入监管的执行任务。未配置完整的旧服务会保持未就绪，并在调谐时报告配置错误。

每个运行中的副本都需要实际 Worker 执行槽。当前 adapter Worker 同步执行一个 assignment；一个阻塞的常驻任务会占用该 Worker，因此增加副本数时必须准备足够的独立 Worker/运行池槽，Runtime Host 的 `maxConcurrent` 不会自动扩大 Worker 的派发并发。

Go/Python Runtime Host 默认执行超时为 **1 小时**。长运行实现应主动配置 `HostOptions.ExecutionTimeout` / `RuntimeHost(execution_timeout=...)`，并调整 AgentVersion 与 Task 的 `budget.wallSeconds` 和允许的租户上限。服务配置不会自动取消这些时限。`Running` 表示内核执行状态和租约存活，不表示业务端口就绪、请求可处理或应用探针通过。

### 7.2 滚动更新与平滑排空 (Rolling Upgrade & Drain)

Supervisor 依据目标版本、副本数和 drain deadline 收敛实例。Drain 达到期限后，停止操作取消实例绑定的 Task；Runtime Worker 在后续 heartbeat 接收取消请求并向底层 Runtime 发送 Stop。当前取消是合作式的，runtime 必须响应 context / stop event；需要强制终止时应配置隔离进程或容器的 `ForceTerminate` 回调。

版本更新及排空不能单独保证业务零停机：应用 readiness、入口流量切换、正在处理的会话和隔离边界的退出确认需要部署方接入并验证。当前服务能力不包含已验证的应用健康失败自动回滚。CLI 提供以下操作，带 flag 的命令应将 flag 放在 service ID 前：

生产环境需在 `FENCED_TOKEN` 中配置 Control API 接受的 OIDC ID token，并为命令添加 `-endpoint https://...`。`service stop` 调用 `POST /v1/services/{id}/stop`，将副本数设为零、禁用 AutoWake 并请求取消；实例仍需等待 Runtime 确认任务终止。

```bash
# 扩缩副本（Supervisor 滚动收敛到目标副本数）
./bin/fenced service scale -replicas 3 "$SERVICE_ID"

# 重启实例的执行任务
./bin/fenced service restart "$SERVICE_ID"

# 停止服务
./bin/fenced service stop "$SERVICE_ID"
```

---

## 8. 跨 Agent 通信 (Durable IPC)

在多 Agent 协同系统（如主管-工人架构、多专家投票法）中，进程间通信面临消息丢失、死锁与网络分区的风险。Fenced 提供了与内核事务绑定的 **Durable IPC Mailbox**。

### 8.1 核心特性
- **At-Least-Once 强持久化投递**：基于 NATS JetStream 与 PostgreSQL WAL 双重保障。
- **接收端去重收据 (Deduplication Receipts)**：每条消息携带全局幂等 MsgID，重复消息在内核层自动滤除。
- **租户隔离**：禁止跨未授权租户的信箱寻址。

### 8.2 Syscall 调用示例

通过内核 Syscall ABI 发送与拉取消息：

```go
// 1. 发送 IPC 消息
sendResp, err := syscallClient.IPCSend(ctx, &syscallv1.IPCSendRequest{
    TargetAgentRef: "analyst-agent@1.0.0",
    Subject:        "task.analysis.request",
    Payload:        []byte(`{"data_url": "s3://reports/2026-q3.parquet"}`),
    CorrelationId:  "corr-8849",
})

// 2. 轮询本地信箱
pollResp, err := syscallClient.IPCPoll(ctx, &syscallv1.IPCPollRequest{
    MaxMessages: 10,
    WaitTimeout: "5s",
})

for _, msg := range pollResp.Messages {
    // 业务处理...
    
    // 3. 确认消息消费
    _ = syscallClient.IPCAck(ctx, &syscallv1.IPCAckRequest{
        MessageId: msg.Id,
    })
}
```

---

## 9. 外部副作用引擎 (External Effect Engine)

LLM 生成代码或 Agent 决策在遇到网络抖动、重试机制时，最危险的行为是**对外部世界产生未保护的重复副作用**（例如：重复调用银行扣款接口、重复发送外部通知邮件、重复删除 S3 文件）。

Fenced 引入了 **External Effect Engine**，通过单调递增租约栅栏（Monotonic Fencing Tokens）与两阶段预备-提交机制实现真正的精确一次（Effectively-Once）隔离保护。

```mermaid
sequenceDiagram
    autonumber
    participant Agent as Agent Attempt (Fence=42)
    participant EffectEngine as Effect Engine
    participant External as 外部系统 (如 Stripe API)

    Agent->>EffectEngine: PrepareEffect(IdempotencyKey, Action="charge", Amount=100)
    Note over EffectEngine: 校验当前租约 Fencing Token 是否有效
    EffectEngine->>External: POST /charge (携带 IdempotencyKey)
    alt 外部系统调用成功 (HTTP 200)
        External-->>EffectEngine: 交易凭证 (Receipt)
        EffectEngine->>EffectEngine: 原子固化 EffectReceipt (PG WAL)
        EffectEngine-->>Agent: EffectSuccess(Receipt)
    else 外部系统无响应或网络超时 (Ambiguous Timeout)
        EffectEngine->>EffectEngine: 标记为 UNKNOWN 状态并实施栅栏封锁
        EffectEngine-->>Agent: AmbiguousOutcomeError(UNKNOWN)
        Note over Agent: 禁止重试！交由内核补偿审计或操作员对账
    end
```

### 9.1 使用规范
1. 所有会变更外部系统状态的操作必须通过 `EffectCall` 封装。
2. 状态机若捕获 `UNKNOWN` 状态，必须立即进入保护性挂起，内核将在后台根据补偿事务或人工对账收据推进，绝不盲目重放。

---

## 10. 多 Agent 工作流 DAG 编排

Fenced 提供了基于 DAG 依赖声明的工作流编排引擎。工作流规格以内容寻址（CAS）方式持久化，保证跨集群运行的一致性。

### 10.1 编写工作流规格 (`workflow.json`)

```json
{
  "name": "data-pipeline-workflow",
  "version": "1.0.0",
  "steps": [
    {
      "name": "fetch-data",
      "agentRef": "crawler-agent@1.0.0",
      "goal": "爬取目标网站公开行业数据",
      "dependencies": []
    },
    {
      "name": "clean-and-transform",
      "agentRef": "etl-agent@1.0.0",
      "goal": "清洗爬取产物并结构化为 Parquet",
      "dependencies": ["fetch-data"]
    },
    {
      "name": "generate-summary",
      "agentRef": "report-agent@1.0.0",
      "goal": "汇总数据并生成战略简报",
      "dependencies": ["clean-and-transform"]
    }
  ]
}
```

### 10.2 执行与可视化工作流

```bash
# 创建并运行工作流
./bin/fenced workflow create -file workflow.json -goal "data pipeline"

# 渲染工作流实时拓扑树与阶段进展
./bin/fenced workflow tree -id "4fa0bc12-6c1d-4c85-bf52-8f2a3d9e7100"
```

控制台将输出清晰的树状依赖拓扑：
```text
Workflow: data-pipeline-workflow (ID: 4fa0bc12-6c1d-4c85-bf52-8f2a3d9e7100)
Status: RUNNING (2/3 completed)

├── [✔] fetch-data (crawler-agent@1.0.0) -> SUCCEEDED (3.2s)
├── [✔] clean-and-transform (etl-agent@1.0.0) -> SUCCEEDED (8.1s)
└── [⟳] generate-summary (report-agent@1.0.0) -> RUNNING (attempt #1)
```

---

## 11. 多租户治理、安全与审计

### 11.1 命名空间与多租户隔离 (`fenced namespace`)

```bash
# 创建企业团队命名空间
./bin/fenced namespace create -name "fintech-team" -display-name "FinTech Team"

# 查看命名空间详情与资源使用水位
./bin/fenced namespace get -name "fintech-team"
```

### 11.2 Rego 准入控制策略

Fenced 原生集成 Open Policy Agent (OPA) 引擎。所有 Agent 提交与发布必须通过 [`policy/`](../policy) 下定义的安全策略。例如，禁止未授权团队的 Agent 请求生产数据库密钥：

```rego
package fenced.admission

default allow = false

allow {
    input.metadata.namespace == "finance"
    input.spec.capabilities.secrets[_] == "PROD_FINANCE_KEY"
}

# 拒绝超过单次 $50 预算的非管理类 Agent
deny[msg] {
    input.spec.budget.costUsd > 50.0
    not input.metadata.labels.tier == "enterprise-approved"
    msg := "Unapproved agent exceeds $50.0 hard spending limit"
}
```

### 11.3 密码学不可篡改审计导出 (Control API `/v1/audit/export`)

内核中的每一笔模型 Token 消耗、工具调用收据与权限决策均被记录进 Append-Only 审计日志。安全合规部门可通过 Control API 导出当前认证租户的完整审计链；控制面配置 `-audit-signing-key` 后导出包携带 Ed25519 签名：

```bash
# 导出审计链（配置签名密钥后为签名 WORM 归档）
curl -sS -H "Authorization: Bearer $FENCED_TOKEN" \
  "http://127.0.0.1:8080/v1/audit/export" \
  -o "audit-export.signed.json"

# 校验审计链完整性
curl -sS -H "Authorization: Bearer $FENCED_TOKEN" \
  "http://127.0.0.1:8080/v1/audit/verify"
```

---

## 12. CLI 命令族与配置速查表

### 12.1 核心 CLI 命令全景

| 根命令 | 子命令 | 主要用途 | 关键参数示例 |
| :--- | :--- | :--- | :--- |
| **`version`** | - | 查看产品版本（`-json` 含 Syscall ABI 与全部协议版本） | `./bin/fenced version -json` |
| **`init`** | - | 初始化 Agent 开发项目脚手架 | `-dir my-agent -adapter [go\|python\|langgraph\|a2a]` |
| **`validate`** | - | 静态验证 AgentManifest 语法与规范 | `-manifest agent.json` |
| **`login`** | - | 登录 OCI Agent Package Registry | `-registry https://... -token ...` |
| **`package`** | `build` | 构建带 Provenance 与摘要的发行清单 | `-manifest agent.json -out pkg.json` |
| | `sign` | 使用 Ed25519 私钥加密签名 | `-package pkg.json -key-id ... -private-key ...` |
| | `push` | 推送至包注册表 | `-package pkg.signed.json -registry ...` |
| | `search` | 在线搜索公共/企业包 | `-query "keyword" -capability ...` |
| | `verify` | 离线/在线校验包签名与摘要 | `-package pkg.signed.json -public-key ...` |
| | `install` | 经由 6 阶段安全门禁安装至租户 | `-package pkg.signed.json -tenant ...` |
| **`runtime`** | `init` | 初始化第三方 Runtime SDK 适配器模板 | `--template [docker\|python\|http\|go]` |
| | `test` | 对指定端点执行一致性套件测试 | `runtime test http://127.0.0.1:8088` |
| **`conformance`**| - | 运行标准一致性认证并生成评估报告 | `-endpoint http://... -json` |
| **`run`** | - | 提交单次任务 | `-agent name@version -goal ... [-spec spec.json]` |
| **`logs`** | - | 流式跟踪任务事件（SSE） | `-task <uuid>` |
| **`workflow`** | `create` | 创建并运行 DAG 工作流 | `-file workflow.json -goal ...` |
| | `tree` | 可视化工作流拓扑结构 | `-id <workflow_id>` |
| **`service`** | `create` | 创建受监管服务及持久化执行任务 | `-name ... -agent name@version -spec task-spec.json [-runtime-class class] [-replicas N] [-restart-policy policy]` |
| | `scale` / `restart` / `stop` | 扩缩、重启与停止服务 | `-replicas N <serviceId>`（scale）；其他命令使用 `<serviceId>` |
| | `instances` | 查看服务实例与健康状态 | `<serviceId>` |
| **`namespace`** | `create` / `list` / `get` | 组织与多租户命名空间配置 | `namespace create -name "dev"` |
| **`migrate`** | - | 提升旧版 AgentManifest 到 v1（非数据库迁移） | `-manifest agent.v1alpha1.json -out agent.v1.json` |
| **`metrics`** | - | 检查实时性能与 Prometheus 监控指标 | `-endpoint http://127.0.0.1:8080` |

### 12.2 核心环境变量清单

| 环境变量 | 作用与示例值 | 默认值 / 备注 |
| :--- | :--- | :--- |
| `DATABASE_URL` | PostgreSQL 连接串，必须支持 pgvector 扩展 | `postgres://fenced:fenced-dev-only@127.0.0.1:55432/fenced?sslmode=disable`；control / controller / outbox / migrate 读取 |
| `FENCED_CONTROL_URL` | Control Plane API 基础 URL | `http://127.0.0.1:8080`；`registry push` 默认读取（`publish` / `run` / `logs` 通过 `-endpoint` 传入） |
| `FENCED_TOKEN` | 经过身份认证的 JWT 令牌或 API 密钥 | 用于 CLI 与控制面通信时的 Bearer 鉴权 |
| `FENCED_TENANT_ID` | 默认交互租户标识符（Python SDK / OCI provider） | `default` |
| `FENCED_EMBEDDING_TOKEN` | 控制面/网关调用嵌入服务的 Bearer 令牌 | 生产模式未配置时拒绝启动 |
| `FENCED_AUDIT_SIGNING_KEY` | 审计导出签名私钥 | 未配置时仅开发模式允许未签名导出 |
| `OTEL_EXPORTER_OTLP_ENDPOINT`| OpenTelemetry 链路与指标采集接入点 | `127.0.0.1:4317` (gRPC) 或 `127.0.0.1:4318` (HTTP) |

> NATS 等组件连接串通过各自进程参数传入（例如 `fenced-outbox -nats-url ...`）。

> 开发者日常入口为 `agent` CLI（`agent config` / `test-llm` / `mcp` / `ui` / `init` / `demo`）；数据库迁移使用独立二进制 `fenced-migrate -database-url $DATABASE_URL`。

---

## 结语与生态共建

Fenced 致力于为整个 Agent 行业建立稳定、标准、中立的操作系统基座。欢迎查阅进阶技术文档以获取更多深度信息：
- [系统架构与深层规范：ARCHITECTURE.md](architecture/ARCHITECTURE.md)
- [版本功能全景矩阵：Feature Status](feature-status.md)
- [v1.2 契约冻结规范：v1.2 Contract Freeze](contracts/v1.2-contract-freeze.md)
- [第三方生态接入指南：Ecosystem Guide](ecosystem/README.md)
