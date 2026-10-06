//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestPoolInstanceHealthFromLeaseHeartbeats proves the liveness derivation:
// an idle instance is healthy, a fresh lease keeps it healthy, an expired or
// unrenewed lease marks it unhealthy, and releasing the lease (completion or
// recovery) heals the instance again.
func TestPoolInstanceHealthFromLeaseHeartbeats(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	freshness := 3 * time.Minute

	// An idle worker with no leases is healthy.
	health, err := repository.PoolInstanceHealth(ctx, []string{"worker-idle"}, clock.Now(), freshness)
	if err != nil {
		t.Fatalf("health of idle worker: %v", err)
	}
	if !health["worker-idle"] {
		t.Fatal("idle worker marked unhealthy")
	}

	// A worker holding a fresh lease is healthy.
	_, run := createAdmittedRun(t, ctx, repository, "pool-health")
	owned, err := repository.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: "tenant-a", AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-busy", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire attempt: %v", err)
	}
	health, err = repository.PoolInstanceHealth(ctx, []string{"worker-busy"}, clock.Now(), freshness)
	if err != nil || !health["worker-busy"] {
		t.Fatalf("busy worker with fresh lease: health=%v err=%v", health, err)
	}

	// Renewals within the TTL keep the worker alive across lease windows.
	clock.Advance(30 * time.Second)
	renewed, err := repository.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: "tenant-a", AttemptID: owned.Attempt.ID, FencingToken: owned.Attempt.FencingToken,
		ExpectedLeaseVersion: owned.Lease.ResourceVersion, TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	clock.Advance(30 * time.Second)
	if _, err := repository.HeartbeatLease(ctx, kernelstore.HeartbeatLeaseInput{
		TenantID: "tenant-a", AttemptID: owned.Attempt.ID, FencingToken: owned.Attempt.FencingToken,
		ExpectedLeaseVersion: renewed.ResourceVersion, TTL: time.Minute,
	}); err != nil {
		t.Fatalf("renew lease again: %v", err)
	}
	health, err = repository.PoolInstanceHealth(ctx, []string{"worker-busy"}, clock.Now(), freshness)
	if err != nil || !health["worker-busy"] {
		t.Fatalf("worker after renewals: health=%v err=%v", health, err)
	}

	// The worker stops heartbeating: once the lease expires it is dead.
	clock.Advance(2 * time.Minute)
	health, err = repository.PoolInstanceHealth(ctx, []string{"worker-busy"}, clock.Now(), freshness)
	if err != nil {
		t.Fatalf("health after expiry: %v", err)
	}
	if health["worker-busy"] {
		t.Fatal("worker with an expired lease marked healthy")
	}

	// Recovery takeover (the same path recovery uses) releases the stale
	// lease and fences the old owner: the instance is healed.
	takenOver, err := repository.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: "tenant-a", AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: owned.Run.ResourceVersion, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-busy", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("reacquire expired run: %v", err)
	}
	if takenOver.Attempt.FencingToken <= owned.Attempt.FencingToken {
		t.Fatalf("takeover did not raise the fence: old=%d new=%d", owned.Attempt.FencingToken, takenOver.Attempt.FencingToken)
	}
	health, err = repository.PoolInstanceHealth(ctx, []string{"worker-busy"}, clock.Now(), freshness)
	if err != nil || !health["worker-busy"] {
		t.Fatalf("worker after takeover: health=%v err=%v", health, err)
	}

	// An empty instance list yields an empty result.
	health, err = repository.PoolInstanceHealth(ctx, nil, clock.Now(), freshness)
	if err != nil || len(health) != 0 {
		t.Fatalf("empty instance list: health=%v err=%v", health, err)
	}
}

// TestPoolInstanceHealthHeartbeatFreshnessWindow proves the heartbeat clause:
// with a freshness window shorter than the lease TTL, an un-renewed lease is
// stale even while still unexpired.
func TestPoolInstanceHealthHeartbeatFreshnessWindow(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	_, run := createAdmittedRun(t, ctx, repository, "pool-health-fresh")
	owned, err := repository.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: "tenant-a", AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-slow", TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire attempt: %v", err)
	}
	_ = owned

	// Within the freshness window the lease is fresh even though no renewal
	// happened yet.
	health, err := repository.PoolInstanceHealth(ctx, []string{"worker-slow"}, clock.Now(), 30*time.Second)
	if err != nil || !health["worker-slow"] {
		t.Fatalf("fresh lease: health=%v err=%v", health, err)
	}
	// Past the freshness window, still unexpired, the lease is stale.
	clock.Advance(time.Minute)
	health, err = repository.PoolInstanceHealth(ctx, []string{"worker-slow"}, clock.Now(), 30*time.Second)
	if err != nil {
		t.Fatalf("stale-by-heartbeat health: %v", err)
	}
	if health["worker-slow"] {
		t.Fatal("unrenewed lease within TTL marked healthy past the freshness window")
	}
}

// TestPoolInstanceHealthIgnoresReleasedLeases proves that a lease released by
// normal completion never marks the instance unhealthy.
func TestPoolInstanceHealthIgnoresReleasedLeases(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	_, run := createAdmittedRun(t, ctx, repository, "pool-health-done")
	owned, err := repository.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: "tenant-a", AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "oci",
		RuntimeInstanceID: "worker-done", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire attempt: %v", err)
	}
	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: owned.Attempt.ID, FencingToken: owned.Attempt.FencingToken,
		ExpectedAttemptVersion: owned.Attempt.ResourceVersion, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: starting.ID, FencingToken: starting.FencingToken,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatalf("run attempt: %v", err)
	}
	completed, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: running.ID, FencingToken: running.FencingToken,
		ExpectedAttemptVersion: running.ResourceVersion, To: domain.AttemptCompleted,
	})
	if err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	// Commit the result long after the lease would have lapsed: the release
	// must make the instance healthy regardless of lease age.
	clock.Advance(10 * time.Minute)
	if _, _, err := repository.CompleteRun(ctx, kernelstore.CompleteRunInput{
		TenantID: "tenant-a", RunID: run.ID, AttemptID: completed.ID, FencingToken: completed.FencingToken,
		ExpectedRunVersion: owned.Run.ResourceVersion, ResultRef: "cas://sha256/result-done",
	}); err != nil {
		t.Fatalf("commit run: %v", err)
	}
	health, err := repository.PoolInstanceHealth(ctx, []string{"worker-done"}, clock.Now(), time.Minute)
	if err != nil || !health["worker-done"] {
		t.Fatalf("completed worker: health=%v err=%v", health, err)
	}
}
