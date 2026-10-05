# Naming: AgentOS → Fenced

> **中文摘要**：本项目原名 **AgentOS**，现更名为 **Fenced**。本文记录完整的重命名映射、兼容性影响，以及有意保留旧名的部分。本次为**全量重命名**：品牌、仓库、CLI、Go module 路径、proto package、协议标识符、SDK 包名、环境变量、目录与文件名均已改为 `Fenced` / `fenced`。**这是一次破坏性变更**，详见第 3 节。

---

## 1. Rename map

The rename is case-preserving across three forms: `AgentOS` → `Fenced` (brand),
`agentos` → `fenced` (identifiers), `AGENTOS` → `FENCED` (environment).

| Artifact | Former | Current |
| --- | --- | --- |
| Product / brand | `AgentOS` | `Fenced` |
| GitHub repository | `CloudEdgeCore/AgentOS` | `CloudEdgeCore/Fenced` |
| Go module path | `github.com/CloudEdgeCore/AgentOS` | `github.com/CloudEdgeCore/Fenced` |
| Primary CLI binaries | `agentos`, `agentos-control`, `agentos-gateway`, `agentos-controller`, `agentos-runtime-*`, `agentos-pkg`, `agentos-slo`, … | `fenced`, `fenced-control`, `fenced-gateway`, `fenced-controller`, `fenced-runtime-*`, `fenced-pkg`, `fenced-slo`, … |
| Command directories | `cmd/agentos*` | `cmd/fenced*` |
| Proto root directory | `proto/agentos/` | `proto/fenced/` |
| Protobuf package | `agentos.<group>.v1` | `fenced.<group>.v1` |
| Generated Go package | `gen/go/agentos/<group>/v1` | `gen/go/fenced/<group>/v1` |
| Runtime Interface protocol | `agentos.runtime.interface/v1` | `fenced.runtime.interface/v1` |
| HTTP adapter protocol | `agentos.adapter-http/v1` | `fenced.adapter-http/v1` |
| Reference/OCI runtime protocol | `agentos.reference/v1`, `agentos.oci/v1` | `fenced.reference/v1`, `fenced.oci/v1` |
| Kubernetes-style API group | `agentos.dev/v1`, `agentos.dev/v1alpha1` | `fenced.dev/v1`, `fenced.dev/v1alpha1` |
| Task spawn operation | `agentos.task.spawn` | `fenced.task.spawn` |
| Memory/IPC/Model ops | `agentos.memory.put`, `agentos.model.invoke`, `agentos.stdout`, … | `fenced.memory.put`, `fenced.model.invoke`, `fenced.stdout`, … |
| HTTP execution header | `X-Agentos-Execution` | `X-Fenced-Execution` |
| Environment variables | `AGENTOS_*` (e.g. `AGENTOS_TOKEN`, `AGENTOS_CONTROL_URL`, `AGENTOS_FENCING_TOKEN`) | `FENCED_*` (`FENCED_TOKEN`, `FENCED_CONTROL_URL`, `FENCED_FENCING_TOKEN`) |
| Python SDK package | `agentos_runtime` / distribution `agentos-runtime` | `fenced_runtime` / distribution `fenced-runtime` |
| Framework adapters | `adapters/<fw>/agentos_<fw>.py` | `adapters/<fw>/fenced_<fw>.py` |
| Rust WASM runtime crate | `agentos-runtime-wasm` | `fenced-runtime-wasm` |
| OPA policy | `internal/kernel/policy/agentos.rego` | `internal/kernel/policy/fenced.rego` |
| TLA+ model | `modelcheck/tla/AgentOS.{tla,cfg}` | `modelcheck/tla/Fenced.{tla,cfg}` |
| Helm chart | `deploy/helm/agentos` | `deploy/helm/fenced` |

The replacement was applied uniformly to 512 source, config, documentation, CI, and
evidence files. Generated protobuf code was **regenerated** with `buf generate`
rather than text-edited, because the embedded file descriptors are length-prefixed
and cannot be safely string-replaced.

## 2. What was intentionally NOT renamed

- **`cmd/agent` and `agent.yaml`** — the developer-facing CLI and its configuration
  file use the product-neutral word *agent*, not the product name. They are unchanged.
- **Third-party framework and product names** — LangGraph, AutoGen, CrewAI, OpenAI
  Agents SDK, gVisor, Wasmtime, PostgreSQL, NATS, SPIFFE, etc.
- **Historical release notes** — `docs/releases/*` retain their original wording for
  the versions they describe.
- **Hash-attested raw evidence artifacts** — the raw logs, JSON summaries, and
  reproduction scripts under `docs/evidence/benchmark/100k-stability-2026-09-15/` and
  `docs/evidence/benchmark/1m-2026-09-16/` are published together with a SHA-256
  manifest. Their bytes — including the former `agentos` identifiers inside them —
  are preserved unchanged so the manifest stays verifiable. The surrounding Markdown
  reports were renamed.

## 3. Compatibility consequences (breaking)

A compatibility layer accepts the most common legacy inputs automatically —
`AGENTOS_*` environment variables, manifests with `agentos.*` identifiers, the
legacy `X-Agentos-Execution` header, and the `agentos` command itself. The
gRPC/protobuf wire protocol is **not** aliased. See
[COMPATIBILITY.md](COMPATIBILITY.md) for the exact boundary and the migration
procedure.

This is a **wire-breaking and configuration-breaking change**. Before upgrading:

1. **gRPC / protobuf wire compatibility is broken.** Renaming the protobuf package
   from `agentos.*` to `fenced.*` changes every gRPC full method name
   (`/agentos.runtime.v1.Runtime/...` → `/fenced.runtime.v1.Runtime/...`) and every
   protobuf type URL. Clients and servers built before the rename cannot interoperate
   with binaries built after it. Regenerate and redeploy all clients.
2. **The v1.2 contract freeze is superseded.** The frozen identifiers were `agentos.*`;
   the rename constitutes a new contract generation. See
   [`contracts/v1.2-contract-freeze.md`](contracts/v1.2-contract-freeze.md).
3. **Persisted state may not be readable.** Any checkpoint, stored protocol string,
   API-group value (`agentos.dev/*`), or mailbox/effect record that embeds an
   `agentos.*` identifier is not recognized by the renamed binary. A migration or
   re-registration step is required for existing deployments.
4. **Environment variables and headers changed.** Rename `AGENTOS_*` → `FENCED_*` and
   `X-Agentos-Execution` → `X-Fenced-Execution` in deployment manifests and callers.
5. **CLI and SDK entry points changed.** `agentos ...` becomes `fenced ...`;
   `import agentos_runtime` becomes `import fenced_runtime`.
6. **Evidence prose was renamed; raw evidence bytes were preserved.** Markdown
   reports under `docs/evidence/` were renamed to the new identifiers. The hash-attested
   raw artifacts (logs, JSON summaries, reproduction scripts) keep their original bytes
   so their published SHA-256 manifest remains verifiable, and therefore still contain
   the former name.

## 4. Rollback

The rename was performed on the `rename-to-fenced` branch. To review or revert:

```shell
git diff main...rename-to-fenced --stat   # review the scope
git checkout main                          # return to the pre-rename state
```
