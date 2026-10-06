//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/scheduler"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/google/uuid"
)

func TestRuntimePoolRegistryUsesDynamicTenantGrants(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	pools := []scheduler.RuntimePool{{
		ID: "dynamic-a", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-a", Region: "cn-east", Ready: true, Status: "ACTIVE",
		AvailableCPU: 1000, AvailableMemory: 1024, AvailableLLMSlots: 2,
	}}
	if err := repository.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatal(err)
	}
	allowed, err := repository.ListRuntimePools(ctx, "tenant-a")
	if err != nil || len(allowed) != 1 || allowed[0].ID != "dynamic-a" {
		t.Fatalf("allowed=%+v err=%v", allowed, err)
	}
	denied, err := repository.ListRuntimePools(ctx, "tenant-b")
	if err != nil || len(denied) != 0 {
		t.Fatalf("cross-tenant pools=%+v err=%v", denied, err)
	}
	pools[0].TenantIDs = []string{"tenant-b"}
	pools[0].Status = "CORDONED"
	if err := repository.RegisterRuntimePools(ctx, pools); err != nil {
		t.Fatal(err)
	}
	allowed, _ = repository.ListRuntimePools(ctx, "tenant-a")
	granted, _ := repository.ListRuntimePools(ctx, "tenant-b")
	if len(allowed) != 0 || len(granted) != 1 || granted[0].Status != "CORDONED" {
		t.Fatalf("grant replacement allowed=%+v granted=%+v", allowed, granted)
	}
}

// scheduleOnPool claims and schedules one admitted task on the named pool.
func scheduleOnPool(t *testing.T, ctx context.Context, repository *postgresstore.Store, key, poolID string, requestedCPU int64) error {
	t.Helper()
	task := createAdmittedTask(t, ctx, repository, key)
	claim, err := repository.ClaimTasks(ctx, kernelstore.ClaimTasksInput{
		Kind: kernelstore.ControllerScheduling, Phase: domain.TaskAdmitted,
		OwnerID: "scheduler-" + key, Limit: 1, TTL: time.Minute,
	})
	if err != nil || len(claim) != 1 {
		t.Fatalf("claim %s: %d %v", key, len(claim), err)
	}
	_, err = repository.ScheduleTask(ctx, kernelstore.ScheduleTaskInput{
		TaskID: task.ID, TenantID: "tenant-a", OwnerID: "scheduler-" + key,
		ClaimFencingToken: claim[0].FencingToken, ExpectedTaskVersion: task.ResourceVersion,
		RunID: uuid.New(), AttemptID: uuid.New(), LeaseID: uuid.New(),
		RuntimePoolID: poolID, RuntimeClass: "oci", RuntimeInstanceID: "worker-cap", LeaseTTL: time.Minute,
		RequestedCPU: requestedCPU, RequestedMemory: 64, RequestedLLMSlots: 1,
	})
	return err
}

// TestSchedulerSeesEffectiveCapacityAndCannotWriteTotals proves placement
// input is the reserved-adjusted effective capacity, and that scheduling
// with a stale (larger) snapshot can never inflate the operator-owned
// capacity ledger totals.
func TestSchedulerSeesEffectiveCapacityAndCannotWriteTotals(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	if err := repository.RegisterRuntimePools(ctx, []scheduler.RuntimePool{{
		ID: "capacity-pool", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-cap", Region: "cn-east", Ready: true, Status: "ACTIVE",
		AvailableCPU: 1000, AvailableMemory: 1024, AvailableLLMSlots: 4,
	}}); err != nil {
		t.Fatalf("register pool: %v", err)
	}

	before, err := repository.ListRuntimePools(ctx, "tenant-a")
	if err != nil || len(before) != 1 || before[0].AvailableCPU != 1000 || before[0].CapacityLedgerMissing {
		t.Fatalf("fresh pool listing=%+v err=%v, want full effective capacity", before, err)
	}

	if err := scheduleOnPool(t, ctx, repository, "effective-cap", "capacity-pool", 400); err != nil {
		t.Fatalf("schedule task: %v", err)
	}

	after, err := repository.ListRuntimePools(ctx, "tenant-a")
	if err != nil || len(after) != 1 {
		t.Fatalf("pool listing after reservation: %+v err=%v", after, err)
	}
	if after[0].AvailableCPU != 600 {
		t.Fatalf("effective CPU = %d, want 600 (1000 total - 400 reserved)", after[0].AvailableCPU)
	}
	var totalCPU, reservedCPU int64
	if err := pool.QueryRow(ctx, `SELECT total_cpu_millis, reserved_cpu_millis
		FROM runtime_pool_capacities WHERE pool_id='capacity-pool'`).Scan(&totalCPU, &reservedCPU); err != nil {
		t.Fatalf("read capacity ledger: %v", err)
	}
	// ScheduleTask was invoked with a stale snapshot of 1<<30 CPU millis;
	// the operator-owned totals must remain untouched.
	if totalCPU != 1000 || reservedCPU != 400 {
		t.Fatalf("ledger total=%d reserved=%d, want totals pinned at 1000 and reserved 400", totalCPU, reservedCPU)
	}
}

// TestPoolShrinkBelowActiveReservationsIsRejected proves the registry — the
// only writer of totals — refuses to scale a pool below what live tasks
// already reserved, while expansion takes effect immediately.
func TestPoolShrinkBelowActiveReservationsIsRejected(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	base := scheduler.RuntimePool{
		ID: "shrink-pool", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-shrink", Region: "cn-east", Ready: true, Status: "ACTIVE",
		AvailableCPU: 1000, AvailableMemory: 1024, AvailableLLMSlots: 4,
	}
	if err := repository.RegisterRuntimePools(ctx, []scheduler.RuntimePool{base}); err != nil {
		t.Fatalf("register pool: %v", err)
	}
	if err := scheduleOnPool(t, ctx, repository, "shrink-hold", "shrink-pool", 500); err != nil {
		t.Fatalf("schedule holding task: %v", err)
	}

	shrunk := base
	shrunk.AvailableCPU = 400
	err := repository.RegisterRuntimePools(ctx, []scheduler.RuntimePool{shrunk})
	if !errors.Is(err, kernelstore.ErrCapacityExhausted) {
		t.Fatalf("shrink below active reservation err=%v, want ErrCapacityExhausted", err)
	}

	expanded := base
	expanded.AvailableCPU = 2000
	if err := repository.RegisterRuntimePools(ctx, []scheduler.RuntimePool{expanded}); err != nil {
		t.Fatalf("expand pool: %v", err)
	}
	pools, err := repository.ListRuntimePools(ctx, "tenant-a")
	if err != nil || len(pools) != 1 || pools[0].AvailableCPU != 1500 {
		t.Fatalf("after expansion listing=%+v err=%v, want 2000-500 effective", pools, err)
	}
}

// TestScheduleFailsClosedWithoutRegisteredCapacityLedger proves placement
// on a pool with no operator-registered capacity row fails closed instead
// of bootstrapping totals from the caller's snapshot.
func TestScheduleFailsClosedWithoutRegisteredCapacityLedger(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	// Insert a registry row without going through RegisterRuntimePools so
	// the capacity ledger stays unseeded (torn registration shape).
	if _, err := pool.Exec(ctx, `INSERT INTO runtime_pools (
		id,runtime_class,runtime_instance_id,region,data_residency,ready,status,failure_domain,
		available_cpu_millis,available_memory_mib,available_llm_slots,artifact_regions,cost_weight,created_at,updated_at)
		VALUES ('torn','oci','worker-torn','cn-east','',true,'ACTIVE','',1000,1024,4,'[]',0,now(),now())`); err != nil {
		t.Fatalf("seed torn pool: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO runtime_pool_tenant_grants(pool_id,tenant_id,created_at)
		VALUES ('torn','tenant-a',now())`); err != nil {
		t.Fatalf("seed torn grant: %v", err)
	}

	listed, err := repository.ListRuntimePools(ctx, "tenant-a")
	if err != nil || len(listed) != 1 || !listed[0].CapacityLedgerMissing {
		t.Fatalf("torn pool listing=%+v err=%v, want CapacityLedgerMissing", listed, err)
	}

	err = scheduleOnPool(t, ctx, repository, "torn-pool", "torn", 100)
	if !errors.Is(err, kernelstore.ErrPoolCapacityNotInitialized) {
		t.Fatalf("schedule on unregistered ledger err=%v, want ErrPoolCapacityNotInitialized", err)
	}
}

// TestRuntimePoolOperatorGrantIsSeparateFromUsageGrant proves the two pool
// authorities are independent: a tenant usage grant keeps the pool visible
// and schedulable but cannot cordon it, while a separately granted operator
// subject can — with the deciding subject recorded in the audit chain.
func TestRuntimePoolOperatorGrantIsSeparateFromUsageGrant(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	if err := repository.RegisterRuntimePools(ctx, []scheduler.RuntimePool{{
		ID: "shared-pool", TenantIDs: []string{"tenant-a", "tenant-b"}, Operators: []string{"ops@fleet"},
		RuntimeClass: "oci", RuntimeInstanceID: "worker-shared", Region: "cn-east", Ready: true, Status: "ACTIVE",
		AvailableCPU: 4000, AvailableMemory: 8192, AvailableLLMSlots: 8,
	}}); err != nil {
		t.Fatalf("register shared pool: %v", err)
	}

	// Both tenants see the pool (usage authority).
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		listed, err := repository.ListRuntimePools(ctx, tenant)
		if err != nil || len(listed) != 1 {
			t.Fatalf("usage grant lost for %s: %+v err=%v", tenant, listed, err)
		}
	}

	// A plain usage tenant cannot operate the pool; the message names the
	// missing operator grant, not a missing pool.
	_, err := repository.UpdateRuntimePoolStatus(ctx, kernelstore.UpdateRuntimePoolStatusInput{
		TenantID: "tenant-a", PoolID: "shared-pool", Status: "CORDONED",
		OperatorSubject: "user:tenant-a-admin", ExpectedVersion: 1,
	})
	if !errors.Is(err, kernelstore.ErrPoolOperatorDenied) {
		t.Fatalf("usage-tenant cordon err=%v, want ErrPoolOperatorDenied", err)
	}

	// The granted operator can, and the audit chain records the subject.
	state, err := repository.UpdateRuntimePoolStatus(ctx, kernelstore.UpdateRuntimePoolStatusInput{
		TenantID: "tenant-a", PoolID: "shared-pool", Status: "CORDONED",
		OperatorSubject: "ops@fleet", ExpectedVersion: 1,
	})
	if err != nil || state.Status != "CORDONED" || state.ResourceVersion != 2 {
		t.Fatalf("operator cordon = %+v err=%v", state, err)
	}
	var audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_events
		WHERE tenant_id = 'tenant-a' AND event_type = 'runtime_pool.status'
		AND details->>'operatorSubject' = 'ops@fleet' AND details->>'poolId' = 'shared-pool'`).Scan(&audits); err != nil {
		t.Fatalf("count audit: %v", err)
	}
	if audits != 1 {
		t.Fatalf("operator audit rows = %d, want 1", audits)
	}

	// The cordon affected both tenants: neither sees a schedulable pool,
	// and tenant-b equally cannot reactivate it.
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		listed, err := repository.ListRuntimePools(ctx, tenant)
		if err != nil || len(listed) != 1 || listed[0].Status != "CORDONED" {
			t.Fatalf("cordon not visible to %s: %+v err=%v", tenant, listed, err)
		}
	}
	_, err = repository.UpdateRuntimePoolStatus(ctx, kernelstore.UpdateRuntimePoolStatusInput{
		TenantID: "tenant-b", PoolID: "shared-pool", Status: "ACTIVE",
		OperatorSubject: "user:tenant-b-admin", ExpectedVersion: 2,
	})
	if !errors.Is(err, kernelstore.ErrPoolOperatorDenied) {
		t.Fatalf("peer-tenant reactivate err=%v, want ErrPoolOperatorDenied", err)
	}
}
