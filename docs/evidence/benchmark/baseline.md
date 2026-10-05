# Benchmark Baseline

> 仓库：`CloudEdgeCore/Fenced`  
> 目标：建立可重复、可比较的性能基线，并记录每次运行的完整环境，使后续版本可以精确对标。

## 1. 运行环境（每次运行必须完整记录）

```text
Commit SHA:     <填入>
Go version:     <填入>
OS / kernel:    <填入>
CPU:            <型号 / vCPU 数>
RAM:            <GB>
Disk:           <NVMe/SATA, 容量>
PostgreSQL:     <版本, 配置 (max_connections, shared_buffers, work_mem)>
NATS:           <版本, 部署位置>
网络:           <同机 / 同可用区, RTT ms>
```

推荐最小硬件（低于此规格的结果不做跨版本对标）：

```text
CPU: 8+ vCPU (x86_64)
RAM: 32 GB+
Disk: NVMe SSD, 500 GB+
PostgreSQL: 16+, network RTT < 1ms
NATS: 同机或同可用区
```

## 2. 测试场景

| 场景 | Tasks | Scheduler | Controller | Runtime | 入口 |
|------|-------|-----------|------------|---------|------|
| A: 10K | 10,000 | 8 | 16 | 10 | `TestControlPlanePipelineCapacityBaseline`（`FENCED_CAPACITY_TASKS=10000`） |
| B: 100K | 100,000 | 8 | 16 | 10 | `TestControlPlanePipelineCapacityBaseline`（`FENCED_CAPACITY_TASKS=100000`，nightly） |
| C: 1M | 1,000,000 | 8 | 16 | 10 | `TestControlPlanePipelineCapacityBaseline`（`FENCED_CAPACITY_TASKS=1000000`，nightly） |
| 补充: workflow 10K | 10,000 动态任务 | — | — | — | `TestV13DynamicSpawnScale10K` / `TestV13Orchestrates10KDynamicTasks` |
| 补充: research 100K | 100,000 | — | — | — | `TestScale100kTasks`（`FENCED_RESEARCH_SCALE_100K=1`） |

## 3. 运行命令

```bash
# 需要 live PostgreSQL
export FENCED_TEST_DATABASE_URL=postgres://user:pass@host:5432/fenced_test?sslmode=disable
export FENCED_CAPACITY_TASKS=10000   # 10K / 100000 / 1000000

go test -tags=integration -count=1 -timeout 240m \
  -run 'TestControlPlanePipelineCapacityBaseline' -v \
  ./internal/kernel/store/postgres/
```

## 4. 必须记录的输出

```text
Task Create QPS
Schedule QPS
Step Dispatch QPS
Workflow Completion QPS
P50 / P95 / P99 延迟
CPU 使用率
Memory 使用
DB Connections (peak)
DB P95 / P99
NATS Throughput (如有)
```

## 5. 验收

- 同一环境连续运行 3 次：核心吞吐差异 ≤ 10%，P95 差异 ≤ 15%
- 首次运行结果写入本文件作为 **Baseline v1**
- 每次结果记录实际环境（§1），跨版本对标只在相同硬件/配置下进行

## 6. 运行历史

### Baseline v1 — 2026-09-02（本地 Docker PG）

环境（本机）：

```text
Commit SHA:     dfd3478 (refactor/workflow-claim-lease)
OS:             Windows 10.0.26200 (Git Bash), Docker Desktop + WSL2
CPU / RAM:      本机（非基准硬件，仅作功能验证与趋势参考）
PostgreSQL:     16 (pgvector/pgvector:pg16 容器, 127.0.0.1:5432)
NATS:           未部署（本场景不涉及）
```

场景 A（10K Tasks）结果：

```text
任务数:           10,000
总耗时:           4m 23.7s
端到端吞吐:        38 tasks/s
Lost / Duplicate / Deferred: 0 / 0 / 0

submit    (enqueue):   407 tasks/s  p50=244ms   p95=255ms   p99=262ms
admit     (queue→admit): 231 tasks/s  p50=35.4s   p95=42.8s   p99=43.3s
schedule  (admit→run):  85 tasks/s  p50=1m16.7s  p95=1m55.4s  p99=1m57.6s
complete  (run→done):  128 tasks/s  p50=1m40.9s  p95=1m56.9s  p99=1m58.9s
end-to-end:                     p50=3m33.2s  p95=3m56.7s  p99=3m58.9s
```

> ⚠️ 本机为 Windows + Docker 容器 PG，单控制器串行 drain，且非基准硬件。
> 数据仅作为**功能正确性验证**（Lost=0 / Duplicate=0 已证明），
> 不作为吞吐基线；正式 baseline 需在 §1 硬件上由 nightly 产出。

**100K 本机尝试（2026-09-02，同环境）**：admit 阶段 ~21 min 完成 100K（deferred=0），
schedule 阶段推进至 **75,300/100,000 RUNNING（deferred=0, maxRetry=0, 零错误）**，
随后被 go test `-timeout 180m` 终止——本机吞吐不足以在 3h 内完成 100K。
**结论：管线在 75K 规模下无错误无积压异常；完整 100K PASS 属于 nightly CI**
（`capacity-baseline` job, `FENCED_CAPACITY_TASKS=100000`, ubuntu-latest ~90min）。

### Baseline v1 — 正式基线（待 nightly 硬件产出）

> 100K/1M 由 nightly（GitHub Actions ubuntu-latest）持续产出，数据以 nightly artifact 为准。
> **注意**：`capacity-baseline-1m` job 因自托管 runner 池未配置而持续 skip，
> 2026-09-16 的 1M 结果是在等效固定硬件上带外（out of band）产出的，非 nightly 产出。

| 日期 | Commit | 场景 | 环境 | Create QPS | Schedule QPS | P95 | 结论 |
|------|--------|------|------|-----------|-------------|-----|------|
| (待填) | | 10K | | | | | |
| (待填) | | 100K | | | | | |
| 2026-09-16 | `45598a9` | 1M | EC2 m7i.2xlarge, 8 vCPU / 30.81 GiB / gp3 300G | 1764–1817 | 50–53 | 9h3m55s–9h19m58s | 3/3 PASS；吞吐 spread 3.45%、P95 spread 2.95%，两项达标 — [证据](1m-2026-09-16.md) |

场景 C（1M Tasks）三次运行关键数字：

```text
任务数:            1,000,000
总耗时:            9h9m33.531s / 9h25m42.190s / 9h9m42.495s
端到端吞吐:         30 / 29 / 30 tasks/s
Lost / Duplicate / Stuck: 0 / 0 / 0

enqueue:   1764 / 1717 / 1817 tasks/s   p95=72ms / 72ms / 67ms
admit:       85 /   85 /   88 tasks/s   p95=3h13m9.038s / 3h12m38.615s / 3h7m5.477s
schedule:    53 /   50 /   52 tasks/s   p95=5h7m49.13s / 5h25m24.557s / 5h17m7.429s
complete:   574 /  570 /  581 tasks/s   p95=5h8m59.236s / 5h26m2.034s / 5h14m49.791s
end-to-end:                             p95=9h3m55.014s / 9h19m58.048s / 9h3m58.337s
```

> ⚠️ 该结果不是生产容量数字：~30 tasks/s 描述的是这台机器 + 未调优的 2 GiB
> `shared_buffers` + 单 admission/scheduler 控制器串行 drain，不是受支持硬件的吞吐上限。
> 未做同机 100K → 1M 对比，因此不构成规模劣化的测量。磁盘利用率峰值 98.7%（gp3 基线
> 125 MiB/s）按突发记录（仅 1.71% 采样 ≥95%），但未在更快卷上复跑，
> "1M 下 gp3 不是瓶颈"未被证实。详见 [1m-2026-09-16.md](1m-2026-09-16.md) §6。
