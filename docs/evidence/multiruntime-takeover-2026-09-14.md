# Multi-Runtime Takeover — 报告

> 仓库：`CloudEdgeCore/Fenced`
> 对应文档：《Fenced_当前状态与下一步建议_2026-08-29》交付包 **D1**（同一 Workflow 无需修改即跨运行时接管并 SUCCEEDED）
> 基线日期：2026-09-14

## 1. 运行环境

```text
Commit SHA:     c028508f2789234e9a23464745dc2336ef6917fb（HEAD，2026-09-14 13:57）
                其上的 af3de44 只改 .github/workflows/*.yml、c028508 只改
                e2e/single-agent/acceptance_test.go——均不参与被测包编译；
                examples/research-workflow 与 internal/ 的被测代码与 ca62d09 一致。
                测试编译自 HEAD 工作树（工作树另有未提交的 README.md 新段落，不参与编译）。
Go:             go1.26.6 windows/amd64
宿主:           Windows 10.0.26200 + Docker Desktop + WSL2（非基准硬件，本报告不做性能声明）
PostgreSQL:     pgvector/pgvector:pg18@sha256:2ba9ca5f2e7daa0f0e7723cba1ee9167bab54efd3640516a44ac1a928dd67e7a
                （fenced-dev-postgres-1, 127.0.0.1:55432, deploy/dev/compose.yaml）
测试入口:       go test -tags=integration -count=1 -run '^TestMultiRuntime' -v \
                  ./examples/research-workflow/tests/e2e/
Harness:        每场景独立 schema + 完整 in-process kernel 栈
                （admission / scheduler / workflow / recovery 四个 reconcile
                 ＋ 6 个 runtime adapter worker）
负载:           research-workflow 参考应用（8 个 agent 角色），确定性语料——
                无真实模型、无真实网络，全部经受管 MCP/gateway 路径
```

## 2. 四个场景（全部 PASS，总耗时 309.709s）

| # | 场景 | 注入 | 断言 | 耗时 |
|---|------|------|------|------|
| 1 | `TestMultiRuntimeRolePlacement` | 无（异构基线） | 8 个角色的每个 step 都落在其 manifest 声明的 runtime class（planner/analyst/critic/writer/validator→reasoning，search/collector→network，reader→sandbox），且每个角色都真实派发过 | 50.50s |
| 2 | `TestMultiRuntimeMigration` | 提交前 cordon `sandbox-pool` | **同一份 workflow 文档零修改**，每个 reader step 都落在幸存池 `sandbox-pool-2`；角色放置断言仍全过 | 47.95s |
| 3 | `TestMultiRuntimeCapacityExhaustion` | 耗尽 `sandbox-pool` 容量 | placement 走完 ranked candidates，reader 全部落到容量可用的 `sandbox-pool-2` | 49.89s |
| 4 | `TestMultiRuntimeWorkerRecoveryReplacement` | 等待 reader attempt 在 `research-worker-02` 上真实 PLACED 后 **kill 该 worker** 并 cordon 其池 | 租约过期 → fencing token 递增 → 调度器重新放置到幸存池 `research-worker-05`（`sandbox-pool-2`）→ workflow 仍 SUCCEEDED，replacement attempt 有记录，无重复副作用 | 161.17s |

## 3. 接管路径的观测链（场景 4 原始日志摘录）

```text
[research-e2e] worker research-worker-02: transition adapter attempt to
  ATTEMPT_PHASE_STARTING: rpc error: code = Canceled desc = context canceled
  （← 运行中 kill， attempt 失联）

    multiruntime_test.go:271: multi-runtime re-placement complete:
      readers migrated to sandbox-pool-2
  （← lease 过期 + fencing 递增 + 调度器重放，全量 reader 落到幸存池）
```

完整日志：本地 `tmp/multiruntime-evidence.log`（122 行，gitignored，不入库）。

## 4. 这份证据证明了什么 / 没证明什么

**证明**：在 4 运行时类（reasoning / network / sandbox ×2 池）的异构池上——

1. 角色级放置按 manifest 的 runtime class 语义执行（协议与调度器行为一致）；
2. 池 cordon 后，**同一份 workflow 文档**零修改地在幸存池上继续完成；
3. 容量耗尽时 placement 按候选序列走，而不是失败或排队挂死；
4. 运行中 worker crash ＋ lease 过期 ＋ 池 cordon 三者叠加时，内核走
   fencing 递增 → 调度器重放置 → 新 attempt 在幸存池完成，workflow 终态
   SUCCEEDED，无重复副作用。

**没证明**：真实远程运行时的 mTLS/SPIFFE 接管（D2）、性能（本机非基准硬件、
负载为确定性语料）、重复 3 次的稳定性（本报告只主张正确性，与
`docs/evidence/benchmark/100k.md` 的性能口径分开）。

## 5. 与"不可抢占"边界的关系

场景 4 的"接管"走的是 Runtime Protocol 的强制路径——**lease 过期 ＋ fencing
递增 ＋ 重新调度**，不是抢占：被杀 worker 的新 attempt 从 checkpoint/重试
恢复，原 attempt 的任何后续写入都会被 fencing token 拒绝
（`internal/security/negative_integration_test.go` 的
`TestFencingReplayRejectedAfterTakeover`）。场景 4 的观察链与该语义一致：
**跨运行时恢复是重启式的（fencing 递增 ＋ 重新调度），不是指令级迁移。**
