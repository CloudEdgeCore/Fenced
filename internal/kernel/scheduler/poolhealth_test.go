package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
)

// fakePoolHealth is a scriptable store.PoolHealthStore.
type fakePoolHealth struct {
	health map[string]bool
	err    error
}

func (f *fakePoolHealth) PoolInstanceHealth(_ context.Context, instanceIDs []string, _ time.Time, _ time.Duration) (map[string]bool, error) {
	if f.err != nil {
		return nil, f.err
	}
	result := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		result[id] = f.health[id]
	}
	return result, nil
}

func healthyPool(id, instance string) RuntimePool {
	return RuntimePool{
		ID: id, TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci",
		RuntimeInstanceID: instance, Region: "cn-east", Ready: true,
		AvailableCPU: 2000, AvailableMemory: 4096, AvailableLLMSlots: 4,
	}
}

// TestLeaseAwarePoolSourceOverlaysLeaseHealth proves the overlay semantics:
// a pool is ready only when the static config says ready AND the instance is
// alive; a missing health entry is fail-closed; health errors propagate.
func TestLeaseAwarePoolSourceOverlaysLeaseHealth(t *testing.T) {
	static := StaticPoolSource{
		healthyPool("alive", "worker-1"),
		healthyPool("stale", "worker-2"),
		{ID: "disabled", TenantIDs: []string{"tenant-a"}, RuntimeClass: "oci", RuntimeInstanceID: "worker-3", Region: "cn-east", Ready: false},
	}
	health := &fakePoolHealth{health: map[string]bool{"worker-1": true, "worker-2": false, "worker-3": false}}
	source := NewLeaseAwarePoolSource(static, health, time.Minute)

	pools, err := source.ListRuntimePools(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("list pools: %v", err)
	}
	if len(pools) != 3 {
		t.Fatalf("pools = %d, want 3", len(pools))
	}
	byID := map[string]bool{}
	for _, pool := range pools {
		byID[pool.ID] = pool.Ready
	}
	if !byID["alive"] {
		t.Fatal("alive pool was marked not ready")
	}
	if byID["stale"] {
		t.Fatal("stale pool stayed ready")
	}
	if byID["disabled"] {
		t.Fatal("disabled pool became ready")
	}

	// Fail-closed: an instance missing from the health result is unhealthy.
	missing := &fakePoolHealth{health: map[string]bool{"worker-1": true}}
	source = NewLeaseAwarePoolSource(static, missing, time.Minute)
	pools, err = source.ListRuntimePools(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("list pools with missing health: %v", err)
	}
	for _, pool := range pools {
		if pool.ID == "stale" && pool.Ready {
			t.Fatal("missing health entry kept the pool ready")
		}
	}

	// Health store failures propagate to the caller.
	failing := &fakePoolHealth{err: errors.New("lease health unavailable")}
	source = NewLeaseAwarePoolSource(static, failing, time.Minute)
	if _, err := source.ListRuntimePools(context.Background(), "tenant-a"); err == nil {
		t.Fatal("health error was swallowed")
	}
}

// TestPlacementRejectsPoolWhoseWorkerIsStale proves the end-to-end effect:
// with a stale lease on the only pool, Select reports ErrNoPlacement with a
// POOL_NOT_READY rejection instead of scheduling onto a dead worker.
func TestPlacementRejectsPoolWhoseWorkerIsStale(t *testing.T) {
	spec := workload.Spec{Placement: workload.Placement{
		RuntimeClasses: []string{"oci"}, Region: "cn-east", CPU: 100, Memory: 128, LLMConcurrency: 1,
	}}
	source := NewLeaseAwarePoolSource(
		StaticPoolSource{healthyPool("only", "worker-1")},
		&fakePoolHealth{health: map[string]bool{"worker-1": false}},
		time.Minute,
	)
	pools, err := source.ListRuntimePools(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("list pools: %v", err)
	}
	result, err := Select(spec, pools)
	if !errors.Is(err, ErrNoPlacement) {
		t.Fatalf("expected ErrNoPlacement, got %v", err)
	}
	if len(result.Rejected) != 1 || result.Rejected[0].PoolID != "only" {
		t.Fatalf("unexpected rejections: %+v", result.Rejected)
	}
	found := false
	for _, reason := range result.Rejected[0].Reasons {
		if reason == "POOL_NOT_READY" {
			found = true
		}
	}
	if !found {
		t.Fatalf("stale pool was not rejected as not ready: %+v", result.Rejected)
	}
}
