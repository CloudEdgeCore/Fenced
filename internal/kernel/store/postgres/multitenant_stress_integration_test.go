//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/admission"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/recovery"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const stressSpec = `{
	"priority":70,
	"deadline":"2099-08-14T12:00:00Z",
	"budget":{"tokens":100,"costUsd":1,"toolCalls":10,"wallSeconds":60},
	"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","dataResidency":"cn","artifactRegion":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
}`

const (
	stressNormalPerTenant = 24
	stressCancelPerTenant = 8
	stressJitterPerTenant = 8
	stressTasksPerTenant  = stressNormalPerTenant + stressCancelPerTenant + stressJitterPerTenant
)

// TestMultiTenantStressQuotaIsolationAndRecovery runs the v0.8 multi-tenant
// stress model on a real PostgreSQL with the default controller loop: eight
// tenants with different quota configurations submit mixed workloads
// (normal, cancel-after-admission, worker-jitter-with-recovery-retry) in
// parallel; a second wave then proves quota isolation (a tenant exactly at
// its reserved-and-settled limit is blocked while unlimited tenants keep
// flowing). Assertions are exactness counts against the observed admitted
// set: no duplicate admission, no unauthorized admission, no dropped outbox
// events, no fencing errors, no leftovers.
func TestMultiTenantStressQuotaIsolationAndRecovery(t *testing.T) {
	pool, store := prepare(t, time.Now)
	ctx := context.Background()

	tenants := []string{"tenant-0", "tenant-1", "tenant-2", "tenant-3", "tenant-4", "tenant-5", "tenant-6", "tenant-7"}
	tenantPolicies := policy.TenantPolicies{}
	for _, tenant := range tenants {
		tenantPolicies[tenant] = policy.TenantPolicy{MaxPriority: 100}
		publishVersion(t, ctx, store, tenant, "agent", "1", `{"runtimeClassPolicy":{"allowed":["oci"]}}`)
	}
	policyEngine, err := policy.New(tenantPolicies)
	if err != nil {
		t.Fatalf("prepare policy engine: %v", err)
	}
	// Different quota configurations: tenant-0 gets a tight token quota
	// (10 x 100-ceiling admissions), tenant-1 a looser one (20), the rest
	// are unlimited.
	if _, err := store.SetTenantQuota(ctx, kernelstore.SetTenantQuotaInput{
		TenantID: "tenant-0", WindowSeconds: 86400,
		Limits: kernelstore.TaskBudget{Tokens: 1000},
	}); err != nil {
		t.Fatalf("set quota tenant-0: %v", err)
	}
	if _, err := store.SetTenantQuota(ctx, kernelstore.SetTenantQuotaInput{
		TenantID: "tenant-1", WindowSeconds: 86400,
		Limits: kernelstore.TaskBudget{Tokens: 2000},
	}); err != nil {
		t.Fatalf("set quota tenant-1: %v", err)
	}

	engine := admission.New(admission.Limits{
		RuntimeClasses: []string{"oci"}, MaxTokens: 1_000_000, MaxCostMicroUSD: money.MustFromUSD(1_000),
		MaxToolCalls: 100_000, MaxWallSeconds: 86_400, MaxCPU: 64_000,
		MaxMemory: 262_144, MaxLLMConcurrency: 128,
	})
	admissionController := admission.NewController(store, engine, policyEngine, "stress/admission", 50, time.Minute)
	admission.WithTenantQuotas(store)(admissionController)
	pools := staticPools{{
		ID: "stress-pool", TenantIDs: tenants, RuntimeClass: "oci", RuntimeInstanceID: "stress-worker-1",
		Region: "cn-east", DataResidency: "cn", Ready: true,
		AvailableCPU: int64(wantStressCapacity() * 100), AvailableMemory: int64(wantStressCapacity() * 128), AvailableLLMSlots: wantStressCapacity(),
	}}
	if err := store.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatalf("register stress fixture pools: %v", err)
	}
	schedulerController := scheduler.NewController(store, pools, "stress/scheduler", 50, time.Minute, 30*time.Minute)
	recoveryController := recovery.NewController(store, 50, time.Minute)

	var tasks []taskRef
	for _, tenant := range tenants {
		for i := 0; i < stressTasksPerTenant; i++ {
			kind := "normal"
			if i >= stressNormalPerTenant && i < stressNormalPerTenant+stressCancelPerTenant {
				kind = "cancel"
			} else if i >= stressNormalPerTenant+stressCancelPerTenant {
				kind = "jitter"
			}
			created, err := store.CreateTask(ctx, kernelstore.CreateTaskInput{
				ID: uuid.New(), TenantID: tenant, Namespace: "default", AgentVersionRef: "agent@1",
				Goal: "stress " + kind, Spec: []byte(stressSpec),
				IdempotencyKey: fmt.Sprintf("stress-%s-%d-%s", tenant, i, uuid.NewString()[:6]),
			})
			if err != nil {
				t.Fatalf("create task %s/%d: %v", tenant, i, err)
			}
			tasks = append(tasks, taskRef{id: created.Task.ID, tenant: tenant, kind: kind})
		}
	}

	// Wave 1: admit to quiescence. With v0.8 reservation semantics the
	// admission count per tenant is deterministic (limit / ceiling): 10 for
	// tenant-0 (1000/100), 20 for tenant-1 (2000/100), all 40 for the
	// unlimited tenants. WHICH tasks are admitted is decided by the
	// reservation race, so the scenario acts on the observed admitted set.
	wantAdmittedWave1 := 10 + 20 + 40*(len(tenants)-2)
	drainPhase(t, ctx, pool, admissionController.Reconcile, "ADMITTED", wantAdmittedWave1)
	admittedRefs := map[string][]taskRef{}
	rejectedByTenant := map[string]int{}
	for _, task := range tasks {
		current, err := store.GetTask(ctx, task.tenant, task.id)
		if err != nil {
			t.Fatalf("get task after wave 1: %v", err)
		}
		switch current.Phase {
		case domain.TaskAdmitted:
			admittedRefs[task.tenant] = append(admittedRefs[task.tenant], task)
		case domain.TaskRejected:
			if current.AdmissionReasonCode != "TENANT_QUOTA_EXCEEDED" {
				t.Fatalf("task %s rejected with %q, want TENANT_QUOTA_EXCEEDED", task.id, current.AdmissionReasonCode)
			}
			rejectedByTenant[task.tenant]++
		default:
			t.Fatalf("task %s in unexpected phase %s after wave 1", task.id, current.Phase)
		}
	}
	expectedAdmitted := map[string]int{"tenant-0": 10, "tenant-1": 20}
	for _, tenant := range tenants {
		want := stressTasksPerTenant
		if v, ok := expectedAdmitted[tenant]; ok {
			want = v
		}
		if len(admittedRefs[tenant]) != want {
			t.Fatalf("wave 1 admitted %s = %d, want %d (reservation semantics)", tenant, len(admittedRefs[tenant]), want)
		}
		if rejectedByTenant[tenant] != stressTasksPerTenant-want {
			t.Fatalf("wave 1 rejected %s = %d, want %d", tenant, rejectedByTenant[tenant], stressTasksPerTenant-want)
		}
	}

	// Cancel subset: cancel admitted cancel-kind tasks before scheduling.
	// Cancelling returns their reserved ceiling (v0.8 release-on-terminal).
	for _, task := range admittedRefsByKind(admittedRefs, "cancel") {
		current, err := store.GetTask(ctx, task.tenant, task.id)
		if err != nil {
			t.Fatalf("get task for cancel: %v", err)
		}
		if _, err := store.RequestTaskCancellation(ctx, task.tenant, task.id, current.ResourceVersion); err != nil {
			t.Fatalf("cancel task %s: %v", task.id, err)
		}
	}

	// Jitter subset: simulate worker crashes by acquiring attempts with a
	// sub-second lease TTL and letting them lapse.
	for _, task := range admittedRefsByKind(admittedRefs, "jitter") {
		current, err := store.GetTask(ctx, task.tenant, task.id)
		if err != nil {
			t.Fatalf("get task for jitter: %v", err)
		}
		run, err := store.CreateRun(ctx, kernelstore.CreateRunInput{
			ID: uuid.New(), TenantID: task.tenant, TaskID: task.id, ExpectedTaskVersion: current.ResourceVersion,
		})
		if err != nil {
			t.Fatalf("create run for jitter task: %v", err)
		}
		if _, err := store.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
			TenantID: task.tenant, AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
			ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "oci",
			RuntimeInstanceID: "stress-worker-1", TTL: 100 * time.Millisecond,
		}); err != nil {
			t.Fatalf("acquire jitter attempt: %v", err)
		}
	}
	// Let the jitter leases lapse.
	time.Sleep(250 * time.Millisecond)

	// Schedule the admitted normal tasks, then recover every lapsed attempt
	// (the recovery batch is bounded, so loop until no candidates remain).
	schedulerDrainTarget := len(admittedRefsByKind(admittedRefs, "normal")) + len(admittedRefsByKind(admittedRefs, "jitter"))
	drainPhase(t, ctx, pool, schedulerController.Reconcile, "RUNNING", schedulerDrainTarget)
	for round := 0; round < 10; round++ {
		processed, err := recoveryController.Reconcile(ctx)
		if err != nil {
			t.Fatalf("recovery reconcile: %v", err)
		}
		if processed == 0 {
			break
		}
	}

	// Complete every running task (normal + recovered jitter) through the
	// full attempt lifecycle.
	completeRuns(t, ctx, pool, store, schedulerDrainTarget)

	// Settle the full ceiling of every admitted tenant-0 task so the window
	// is exactly at its limit regardless of which mix was admitted, then
	// prove wave 2 is blocked for tenant-0 only. Completion already released
	// the reservations, so the gate now sees consumed = 10 * 100 = 1000.
	for _, task := range admittedRefs["tenant-0"] {
		if _, err := store.SettleTaskUsage(ctx, kernelstore.SettleTaskUsageInput{
			TenantID: task.tenant, TaskID: task.id, IdempotencyKey: "stress-settle-" + task.id.String(),
			Usage: kernelstore.TaskBudget{Tokens: 100},
		}); err != nil {
			t.Fatalf("settle tenant-0 usage: %v", err)
		}
	}
	var windowTokens, windowReserved int64
	if err := pool.QueryRow(ctx, `SELECT consumed_tokens, reserved_tokens FROM tenant_consumption_windows
		WHERE tenant_id = 'tenant-0'`).Scan(&windowTokens, &windowReserved); err != nil {
		t.Fatalf("read tenant-0 window: %v", err)
	}
	if windowTokens != 10*100 || windowReserved != 0 {
		t.Fatalf("tenant-0 window: consumed=%d reserved=%d, want %d/0", windowTokens, windowReserved, 10*100)
	}

	// Wave 2: new submissions. tenant-0 is exactly at its limit and must be
	// rejected with TENANT_QUOTA_EXCEEDED; unlimited tenant-2 keeps flowing.
	wave2 := map[string][]uuid.UUID{}
	for _, tenant := range []string{"tenant-0", "tenant-2"} {
		for i := 0; i < 5; i++ {
			created, err := store.CreateTask(ctx, kernelstore.CreateTaskInput{
				ID: uuid.New(), TenantID: tenant, Namespace: "default", AgentVersionRef: "agent@1",
				Goal: "stress wave2", Spec: []byte(stressSpec),
				IdempotencyKey: fmt.Sprintf("stress-wave2-%s-%d", tenant, i),
			})
			if err != nil {
				t.Fatalf("create wave2 task: %v", err)
			}
			wave2[tenant] = append(wave2[tenant], created.Task.ID)
		}
	}
	if _, err := admissionController.Reconcile(ctx); err != nil {
		t.Fatalf("wave 2 admission: %v", err)
	}
	if _, err := admissionController.Reconcile(ctx); err != nil {
		t.Fatalf("wave 2 admission retry: %v", err)
	}

	// ---- assertions: exactness counts, no thresholds ----
	assertStressOutcomes(t, ctx, pool, store, tenants, admittedRefs, rejectedByTenant, wave2)

	var activeLeases int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM runtime_leases WHERE released_at IS NULL`).Scan(&activeLeases); err != nil {
		t.Fatalf("count active leases: %v", err)
	}
	if activeLeases != 0 {
		t.Fatalf("active leases = %d, want 0 (every lease released)", activeLeases)
	}
	var claims int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_controller_claims`).Scan(&claims); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if claims != 0 {
		t.Fatalf("lingering claims = %d, want 0", claims)
	}
}

func wantStressCapacity() int {
	return stressTasksPerTenant * 8
}

// admittedRefsByKind returns the admitted task refs of one kind, ordered by
// tenant then creation.
func admittedRefsByKind(refs map[string][]taskRef, kind string) []taskRef {
	var result []taskRef
	for _, tenant := range sortedRefKeys(refs) {
		for _, task := range refs[tenant] {
			if task.kind == kind {
				result = append(result, task)
			}
		}
	}
	return result
}

func sortedRefKeys(m map[string][]taskRef) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func assertStressOutcomes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, store *postgresstore.Store, tenants []string, admittedRefs map[string][]taskRef, rejectedByTenant map[string]int, wave2 map[string][]uuid.UUID) {
	t.Helper()

	// Wave 2: quota isolation — tenant-0 is exactly at its limit (consumed
	// 1000) and every wave-2 submission must be rejected with the recorded
	// reason; unlimited tenant-2 is fully admitted.
	var rejected0, admitted2 int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks
		WHERE tenant_id = 'tenant-0' AND phase = 'REJECTED' AND admission_reason_code = 'TENANT_QUOTA_EXCEEDED'
		  AND idempotency_key LIKE 'stress-wave2-%'`).Scan(&rejected0); err != nil {
		t.Fatalf("count tenant-0 rejected: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks
		WHERE tenant_id = 'tenant-2' AND phase = 'ADMITTED' AND idempotency_key LIKE 'stress-wave2-%'`).Scan(&admitted2); err != nil {
		t.Fatalf("count tenant-2 admitted: %v", err)
	}
	if rejected0 != len(wave2["tenant-0"]) {
		t.Fatalf("tenant-0 wave-2 rejections = %d, want %d (quota isolation)", rejected0, len(wave2["tenant-0"]))
	}
	if admitted2 != len(wave2["tenant-2"]) {
		t.Fatalf("tenant-2 wave-2 admissions = %d, want %d (unlimited tenant must flow)", admitted2, len(wave2["tenant-2"]))
	}

	// Cancellations: exactly the admitted cancel-kind tasks were cancelled,
	// and the outbox carries one TaskCancelled event per cancellation.
	cancelledRefs := admittedRefsByKind(admittedRefs, "cancel")
	var cancelled, cancelledEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE phase = 'CANCELLED'`).Scan(&cancelled); err != nil {
		t.Fatalf("count cancelled: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE aggregate_type = 'Task' AND event_type = 'TaskCancelled'`).Scan(&cancelledEvents); err != nil {
		t.Fatalf("count cancel events: %v", err)
	}
	if cancelled != len(cancelledRefs) || cancelledEvents != cancelled {
		t.Fatalf("cancelled=%d events=%d, want %d/%d", cancelled, cancelledEvents, len(cancelledRefs), len(cancelledRefs))
	}

	// Recovery: every admitted jitter task was retried and succeeded (worker
	// crash recovered through the fencing path, never lost).
	wantSucceeded := len(admittedRefsByKind(admittedRefs, "normal")) + len(admittedRefsByKind(admittedRefs, "jitter"))
	var succeeded, succeededEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE phase = 'SUCCEEDED'`).Scan(&succeeded); err != nil {
		t.Fatalf("count succeeded: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE aggregate_type = 'Task' AND event_type = 'TaskSucceeded'`).Scan(&succeededEvents); err != nil {
		t.Fatalf("count success events: %v", err)
	}
	if succeeded != wantSucceeded || succeededEvents != wantSucceeded {
		t.Fatalf("succeeded=%d events=%d, want %d/%d", succeeded, succeededEvents, wantSucceeded, wantSucceeded)
	}

	// Wave-1 rejection counts match the reservation semantics per tenant.
	expectedRejected := map[string]int{"tenant-0": 30, "tenant-1": 20}
	for _, tenant := range tenants {
		want := 0
		if v, ok := expectedRejected[tenant]; ok {
			want = v
		}
		if rejectedByTenant[tenant] != want {
			t.Fatalf("tenant %s wave-1 rejections = %d, want %d", tenant, rejectedByTenant[tenant], want)
		}
	}

	// Outbox exactness: one TaskAdmitted per admitted task, no duplicates.
	// The outbox is then drained by a dispatcher: every event is published
	// exactly once with zero fencing rejections and zero drops.
	wantAdmitted := 0
	for _, refs := range admittedRefs {
		wantAdmitted += len(refs)
	}
	wantAdmitted += len(wave2["tenant-2"])
	var admittedEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE aggregate_type = 'Task' AND event_type = 'TaskAdmitted'`).Scan(&admittedEvents); err != nil {
		t.Fatalf("count admit events: %v", err)
	}
	if admittedEvents != wantAdmitted {
		t.Fatalf("admitted events=%d, want %d (exact-once)", admittedEvents, wantAdmitted)
	}
	var totalEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&totalEvents); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	var published, fenced int
	for {
		events, err := store.ClaimOutbox(ctx, kernelstore.ClaimOutboxInput{
			DispatcherID: "stress-dispatcher", Limit: 100, LockTTL: time.Minute,
		})
		if err != nil {
			t.Fatalf("claim outbox: %v", err)
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			if err := store.MarkOutboxPublished(ctx, event.ID, "stress-dispatcher", event.LockFencingToken, time.Now()); err != nil {
				fenced++
			} else {
				published++
			}
		}
	}
	var unpublished int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
		t.Fatalf("count unpublished: %v", err)
	}
	if published != totalEvents || unpublished != 0 || fenced != 0 {
		t.Fatalf("published=%d total=%d unpublished=%d fenced=%d, want %d/%d/0/0",
			published, totalEvents, unpublished, fenced, totalEvents, totalEvents)
	}

	// No leftovers except the wave-2 tasks that were admitted at the very
	// end of the scenario (they are legitimately ADMITTED, waiting for a
	// scheduler that the test stops before starting).
	var queued, admitted, running int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE phase = 'QUEUED'),
		count(*) FILTER (WHERE phase = 'ADMITTED'),
		count(*) FILTER (WHERE phase = 'RUNNING') FROM tasks`).Scan(&queued, &admitted, &running); err != nil {
		t.Fatalf("count leftovers: %v", err)
	}
	if queued != 0 || running != 0 {
		t.Fatalf("leftover queued=%d running=%d, want 0/0", queued, running)
	}
	if admitted != len(wave2["tenant-2"]) {
		t.Fatalf("leftover admitted=%d, want %d (only the wave-2 tenant-2 tasks)", admitted, len(wave2["tenant-2"]))
	}
}

type taskRef struct {
	id     uuid.UUID
	tenant string
	kind   string // normal | cancel | jitter
}
