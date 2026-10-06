# Database Consistency Model

> 仓库：`CloudEdgeCore/Fenced`  
> 对应版本：2026-09-02 审计，涵盖 Task Claim、Lease、Attempt Fencing、Capacity Reservation 等核心路径。

## 1. Isolation Assumption

所有事务使用 **PostgreSQL Read Committed**（`pgx.TxOptions{}` 默认）。`SERIALIZABLE` 明确被拒绝——实测约 94% 冲突率，不可接受（`store.go:661-666`）。

Read Committed 下每个语句看到的是该语句执行时已提交的最新数据。`SELECT ... FOR UPDATE` 会阻塞直到前置事务释放行锁，但不会读到未提交的中间状态。

## 2. 锁定 / CAS 策略

| 级别 | 机制 | 适用路径 |
|------|------|----------|
| 行锁 | `SELECT ... FOR UPDATE` | Task Claim（`tasks` 行）、`requireTaskClaim`（`task_controller_claims` 行）、`lockRuntimeOwner`（run/attempt/lease 行） |
| 跳过锁 | `SKIP LOCKED` | Task Claim SELECT（多个 scheduler 不互相阻塞） |
| 乐观锁 | `UPDATE ... WHERE resource_version = $N` | Task 状态迁移、Workflow 状态迁移、Step 状态迁移、Claim 续期 |
| 原子条件更新 | `UPDATE ... WHERE ... AND reserved + $1 <= total` | Capacity Reservation |
| `ON CONFLICT DO UPDATE` | `INSERT ... ON CONFLICT (tenant_id, task_id, controller_kind) DO UPDATE SET ... WHERE expires_at <= now` | Claim 的"取或偷"语义 |

## 3. 每条路径审计

### 3.1 Tier 1：Task Claim（`control.go:57-70`）

- **谁可以写**：任何未被超时 claim 锁定的 controller。Claim 逻辑使用 `SELECT ... FOR UPDATE OF t SKIP LOCKED` 锁定 tasks 行，然后 `INSERT ... ON CONFLICT DO UPDATE ... WHERE expires_at <= now`。
- **并发竞态**：`ON CONFLICT ... WHERE expires_at <= now` 保证只有过期 claim 能被偷。`fencing_token = task_controller_claims.fencing_token + 1` 每次抢锁递增。
- **旧 Worker 覆盖**：`requireTaskClaim`（`control.go:544`）校验 `owner_id` 和 `fencing_token` 精确匹配，`SELECT ... FOR UPDATE` 锁定 claim 行。旧 token 的写操作返回 `ErrFenced`。
- **幂等**：否——claim 是单次使用。重试会得到新 fencing token。

### 3.2 Tier 1：Lease Renewal（`runtime.go` 中的锁机制，`workflow.go:616` orchestrator）

- **调度 Runtime lease**：`ClaimTasks` 使用 `INSERT ... ON CONFLICT ... WHERE expires_at <= now`，与 Task Claim 相同的取/偷语义。
- **Orchestrator claim**：`ClaimWorkflows` 使用 `UPDATE workflows SET ... WHERE ... AND (orchestrator_claim IS NULL OR ... OR orchestrator_claim_until <= $6)`——CAS 加上过期检查。
- **续期**：`RenewWorkflowClaim` 使用 `UPDATE ... WHERE orchestrator_claim = $4 AND orchestrator_claim_until > $5`——校验 owner 和未过期。
- **旧 Worker 覆盖**：续期受 owner 检查保护。初始 claim 受 `orchestrator_claim_until` 过期检查保护。

### 3.3 Tier 1：Attempt Fencing（`runtime.go:717` `RecoverExpiredAttempt`）

- **谁可以写**：持有正确 `fencingToken` 的 recovery controller（`run.current_fencing_token == attempt.fencing_token`）。
- **锁定顺序**：`lockRuntimeOwner`（`runtime.go:1056`）使用 `FOR UPDATE` 按序锁定 run → attempt → task → lease 行，防止死锁。
- **旧 Worker 覆盖**：`requireCurrent`（`store.go:710`）检查 `run.CurrentFencingToken` 和 `attempt.FencingToken` 均匹配。恢复成功后 `run.current_fencing_token` 递增，旧 token 的后续操作被阻塞。
- **幂等**：是。恢复后旧 lease 被标记 `released_at IS NOT NULL`，重试不会找到活跃 lease。

### 3.4 Tier 1：Capacity Reservation（`capacity.go:22` `reserveRuntimeCapacity`）

- **谁可以写**：已通过 `requireTaskClaim` 的 scheduler（`ScheduleTask`）。
- **并发竞态**：原子单语句 `UPDATE runtime_pool_capacities SET ... WHERE reserved + $1 <= total`。无 `SELECT FOR UPDATE` 需要，因为 WHERE 条件由 PostgreSQL 原子评估。
- **超卖保护**：WHERE 子句 `reserved_cpu_millis + $1 <= total_cpu_millis` 等保证不超卖。
- **幂等**：`reserved_cpu += $1` 操作本身不是幂等的，但重试仅发生在可重试事务错误（40001/40P01）后，此错误会回滚整个事务，因此增量被撤销。提交后无重试。
- **释放**：`releaseRuntimeCapacity`（`capacity.go:74`）使用 `FOR UPDATE` 锁定 reservation 行然后递减，`WHERE status = 'ACTIVE'` 确保恰好一次释放。

### 3.5 Tier 2：Workflow Status Transition（`workflow.go:241` `TransitionWorkflow`）

- **⚠️ 发现：没有 orchestrator_claim 检查**。`TransitionWorkflow`、`TransitionWorkflowStep`、`SpawnWorkflowStep` 在写入时均不检查 `orchestrator_claim` 或 `orchestrator_claim_until`。唯一的防御是 `resource_version` CAS。
- **影响**：过期 claim 的旧 orchestrator 如果其 `resource_version` 快照仍然是当前的，仍然可以写入 workflow/step 状态。系统通过 CAS 收敛，但"只有 claim 持有者才能写入"的不变量未被强制执行。
- **严重性**：中。不会导致 double dispatch（step CAS 防止），但会使 workflow 状态机收敛变慢，并在极端情况下可能导致不期望的转换（如旧 orchestrator 在到期后写入 `SUCCEEDED` 而新 orchestrator 同时写入 `FAILED`——CAS 会阻止其中一个，但谁会赢取决于时序）。
- **建议修复**：在 `TransitionWorkflow` 和 `TransitionWorkflowStep` 中增加 `orchestrator_claim = $owner AND orchestrator_claim_until > now()` 检查。

### 3.6 Tier 2：Final Result（`complete.go` / `runtime.go` 的 `CompleteAttempt` / `CompleteRun`）

- **谁可以写**：`CompleteAttempt` 需要 `lockRuntimeOwner`（fencing token）。`CompleteRun` 需要 attempt 已经是 COMPLETED（`runtime.go:606`）。
- **Exactly-once**：`task.resource_version` 和 `run.resource_version` 的 CAS 确保恰好一次。`CompleteAttempt` 和 `CompleteRun` 同时竞速时，CAS 序列化——一个成功，另一个失败。
- **幂等**：是。第一次提交后，第二次的 CAS 失败，或返回之前的结果。

### 3.7 Tier 2：Checkpoint / Outbox / Inbox

- **Checkpoint**：`UPDATE tasks SET ... WHERE resource_version = $N`（标准 CAS）。
- **Outbox**：使用 `INSERT INTO outbox`。没有竞态问题（追加写入）。
- **Inbox**：使用 `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`——天然幂等。

## 4. 事务边界

- 所有事务内 **禁止外部 HTTP / RPC**。扫描确认无违反。
- 所有事务错误正确处理为 `rollback(ctx, tx)`（`defer rollback` 模式）。
- 重试通过 `RetryRetryable`（`store/retry.go`）使用指数退避最多 4 次（`1<<attempt * 5ms`）。

## 5. 修复 / 改进项

### 5.1 高优先级（Tier 1 验证后确认）

- [x] Task Claim 使用 `SKIP LOCKED` + `ON CONFLICT DO UPDATE ... WHERE expires_at <= now` + `fencing_token` 递增 —— 正确
- [x] Capacity Reservation 使用原子 WHERE 条件 —— 不超过
- [x] Attempt Fencing 使用 `requireCurrent` + `FOR UPDATE` 锁顺序 —— 正确
- [x] Lease Renewal 使用 owner 检查 + 过期 guard —— 正确

### 5.2 中优先级（Tier 2 修复）

- [ ] **`TransitionWorkflow` 增加 `orchestrator_claim` 检查**：在 `UPDATE workflows SET ... WHERE resource_version = $N` 中增加 `AND orchestrator_claim = $owner AND orchestrator_claim_until > now()`。这将使 orchestrator claim 成为实际写 fence，而不仅仅是分配提示。
- [ ] **`TransitionWorkflowStep` 增加 `orchestrator_claim` 检查**：同上。
- [ ] **`ClaimWorkflows` 增加 `FOR UPDATE`**：SELECT 候选阶段使用 `FOR UPDATE OF w SKIP LOCKED`，与 Task Claim 一致，消除 SELECT 阶段的不确定性。

### 5.3 低优先级

- [ ] 修正 `ScheduleTask` 注释中 "SERIALIZABLE" 的引用（`control.go:327-328`），改为 "Read Committed"。