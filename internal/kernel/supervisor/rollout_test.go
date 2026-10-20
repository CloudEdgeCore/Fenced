package supervisor

import (
	"context"
	"strings"
	"testing"
	"time"
)

// rolloutFixture builds a supervised service with two replicas on version v1
// and a clock the test controls.
type rolloutFixture struct {
	ctx       context.Context
	store     *MemoryStore
	spawner   *MockSpawner
	sup       *Supervisor
	serviceID string
	now       time.Time
}

func newRolloutFixture(t *testing.T, serviceID string, rollout RolloutConfig) *rolloutFixture {
	t.Helper()
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()
	// The clock reads the fixture, so advancing the fixture advances the
	// kernel's notion of time.
	fixture := &rolloutFixture{
		ctx:     ctx,
		store:   store,
		spawner: spawner,
		now:     time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC),
	}
	sup := NewSupervisor(store, WithSpawner(spawner), WithClock(func() time.Time { return fixture.now }))
	fixture.sup = sup

	svc := &Service{
		ID:        serviceID,
		TenantID:  "tenant-r",
		Namespace: "default",
		Name:      "rollout-service",
		AgentID:   "agent-rollout",
		Spec: ServiceSpec{
			Replicas:      2,
			AgentVersion:  "v1",
			RestartPolicy: RestartAlways,
			DrainTimeout:  5 * time.Second,
			Rollout:       rollout,
		},
	}
	created, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}
	if created.Status.ReadyReplicas != 2 {
		t.Fatalf("expected 2 ready replicas, got %d", created.Status.ReadyReplicas)
	}
	fixture.serviceID = serviceID
	return fixture
}

// advance moves the clock, lets the healthy instances report in, and
// reconciles the service once. A live instance heartbeats; an instance that
// stops reporting is how the kernel learns that it is gone.
func (f *rolloutFixture) advance(t *testing.T, step time.Duration) *Service {
	t.Helper()
	f.now = f.now.Add(step)
	f.heartbeat(t)
	svc, err := f.store.GetService(f.ctx, "tenant-r", f.serviceID)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}
	if err := f.sup.ReconcileService(f.ctx, svc); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	current, err := f.store.GetService(f.ctx, "tenant-r", f.serviceID)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}
	return current
}

// heartbeat reports in for every instance that is still serving.
func (f *rolloutFixture) heartbeat(t *testing.T) {
	t.Helper()
	instances, err := f.store.ListInstances(f.ctx, "tenant-r", f.serviceID)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	for _, inst := range instances {
		if !inst.IsActive() {
			continue
		}
		if err := f.sup.RecordHeartbeat(f.ctx, "tenant-r", f.serviceID, inst.ID); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
	}
}

// instancesByVersion groups the instances of the service by their version.
func (f *rolloutFixture) instancesByVersion(t *testing.T) map[string][]*Instance {
	t.Helper()
	instances, err := f.store.ListInstances(f.ctx, "tenant-r", f.serviceID)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	grouped := map[string][]*Instance{}
	for _, inst := range instances {
		grouped[inst.AgentVersion] = append(grouped[inst.AgentVersion], inst)
	}
	return grouped
}

// A rollout must not trade a serving instance for one that has never run.
// The replacement has to prove itself first, and only then may the old
// instance be drained.
func TestRolloutWaitsForTheReplacementToProveItself(t *testing.T) {
	f := newRolloutFixture(t, "svc-gate", RolloutConfig{
		MaxSurge:       1,
		MaxUnavailable: 1,
		MinReadyTime:   30 * time.Second,
	})

	if _, err := f.sup.RolloutUpgrade(f.ctx, "tenant-r", "svc-gate", "v2"); err != nil {
		t.Fatalf("RolloutUpgrade failed: %v", err)
	}

	// The replacement exists, but it has not proven itself yet.
	grouped := f.instancesByVersion(t)
	if len(grouped["v2"]) != 1 {
		t.Fatalf("expected one v2 instance, got %d", len(grouped["v2"]))
	}
	for _, inst := range grouped["v1"] {
		if inst.Phase != InstanceRunning {
			t.Fatalf("the serving instance was drained before its replacement proved itself: %s", inst.Phase)
		}
	}

	// Less than the minimum ready time: the old instance keeps serving.
	f.advance(t, 10*time.Second)
	grouped = f.instancesByVersion(t)
	for _, inst := range grouped["v1"] {
		if inst.Phase != InstanceRunning {
			t.Fatalf("the serving instance was drained before its replacement proved itself: %s", inst.Phase)
		}
	}

	// The replacement has proven itself: now the old instance may go.
	f.advance(t, 40*time.Second)
	grouped = f.instancesByVersion(t)
	draining := 0
	for _, inst := range grouped["v1"] {
		if inst.Phase == InstanceDraining || inst.Phase == InstanceStopped {
			draining++
		}
	}
	if draining == 0 {
		t.Fatalf("expected the outdated instance to be drained after the replacement proved itself: %+v", grouped["v1"])
	}
}

// A version that fails during a rollout ends the rollout. With the automatic
// rollback the service returns to the version it ran before.
func TestFailedRolloutRollsBackAutomatically(t *testing.T) {
	f := newRolloutFixture(t, "svc-autrollback", RolloutConfig{
		MaxSurge:       1,
		MaxUnavailable: 1,
		AutoRollback:   true,
	})

	if _, err := f.sup.RolloutUpgrade(f.ctx, "tenant-r", "svc-autrollback", "v2"); err != nil {
		t.Fatalf("RolloutUpgrade failed: %v", err)
	}

	// The new version fails.
	grouped := f.instancesByVersion(t)
	for _, inst := range grouped["v2"] {
		inst.Phase = InstanceFailed
		inst.ExitReason = "task FAILED: the agent crashed on startup"
		if err := f.store.UpdateInstance(f.ctx, inst); err != nil {
			t.Fatalf("mark the instance failed: %v", err)
		}
	}

	current := f.advance(t, 6*time.Second)
	if current.Spec.AgentVersion != "v1" {
		t.Fatalf("expected the service to return to v1, got %q", current.Spec.AgentVersion)
	}
	if current.Status.RolloutPhase != RolloutPhaseRollingBack && current.Status.RolloutPhase != RolloutPhaseRolledBack {
		t.Fatalf("expected the rollout to roll back, got %q", current.Status.RolloutPhase)
	}
	if !strings.Contains(current.Status.RolloutFailureReason, "the agent crashed on startup") {
		t.Fatalf("the failure reason was not recorded: %q", current.Status.RolloutFailureReason)
	}
	if !strings.Contains(strings.ToLower(current.Status.Message), "back to v1") {
		t.Fatalf("the status message does not explain the rollback: %q", current.Status.Message)
	}

	// The service converges on the previous version.
	for i := 0; i < 6; i++ {
		current = f.advance(t, 6*time.Second)
		if current.Status.RolloutPhase == RolloutPhaseRolledBack {
			break
		}
	}
	if current.Status.RolloutPhase != RolloutPhaseRolledBack {
		t.Fatalf("expected the rollout to end rolled back, got %q", current.Status.RolloutPhase)
	}
	if current.Status.ReadyReplicas != 2 {
		t.Fatalf("expected the service to serve 2 replicas again, got %d", current.Status.ReadyReplicas)
	}
	if current.Status.ActiveVersion != "v1" {
		t.Fatalf("expected the active version to be v1, got %q", current.Status.ActiveVersion)
	}
}

// Without the automatic rollback the service keeps the failed version and
// says so: the operator decides what happens next.
func TestFailedRolloutWithoutAutoRollbackKeepsTheTargetVersion(t *testing.T) {
	f := newRolloutFixture(t, "svc-nofallback", RolloutConfig{MaxSurge: 1, MaxUnavailable: 1})

	if _, err := f.sup.RolloutUpgrade(f.ctx, "tenant-r", "svc-nofallback", "v2"); err != nil {
		t.Fatalf("RolloutUpgrade failed: %v", err)
	}
	grouped := f.instancesByVersion(t)
	for _, inst := range grouped["v2"] {
		inst.Phase = InstanceFailed
		inst.ExitReason = "task FAILED: the agent crashed on startup"
		if err := f.store.UpdateInstance(f.ctx, inst); err != nil {
			t.Fatalf("mark the instance failed: %v", err)
		}
	}

	current := f.advance(t, 6*time.Second)
	if current.Status.RolloutPhase != RolloutPhaseFailed {
		t.Fatalf("expected the rollout to fail, got %q", current.Status.RolloutPhase)
	}
	if current.Spec.AgentVersion != "v2" {
		t.Fatalf("expected the service to keep v2, got %q", current.Spec.AgentVersion)
	}
	if current.Status.RolloutFailureReason == "" {
		t.Fatal("the failure reason was not recorded")
	}
}

// The explicit rollback is a movement between versions like any rollout.
func TestExplicitRollbackIsRecordedAndCloses(t *testing.T) {
	f := newRolloutFixture(t, "svc-explicit", RolloutConfig{MaxSurge: 1, MaxUnavailable: 1})

	if _, err := f.sup.RolloutUpgrade(f.ctx, "tenant-r", "svc-explicit", "v2"); err != nil {
		t.Fatalf("RolloutUpgrade failed: %v", err)
	}
	current := f.advance(t, 6*time.Second)
	if current.Status.RolloutPhase != RolloutPhaseProgressing {
		t.Fatalf("expected the rollout to progress, got %q", current.Status.RolloutPhase)
	}

	rolledBack, err := f.sup.RollbackService(f.ctx, "tenant-r", "svc-explicit")
	if err != nil {
		t.Fatalf("RollbackService failed: %v", err)
	}
	if rolledBack.Status.RolloutPhase != RolloutPhaseRollingBack {
		t.Fatalf("expected the rollback to be recorded, got %q", rolledBack.Status.RolloutPhase)
	}
	if rolledBack.Spec.AgentVersion != "v1" {
		t.Fatalf("expected the service to return to v1, got %q", rolledBack.Spec.AgentVersion)
	}

	for i := 0; i < 8; i++ {
		current = f.advance(t, 6*time.Second)
		if current.Status.RolloutPhase == RolloutPhaseRolledBack {
			break
		}
	}
	if current.Status.RolloutPhase != RolloutPhaseRolledBack {
		t.Fatalf("expected the rollback to close, got %q", current.Status.RolloutPhase)
	}
	if current.Status.ActiveVersion != "v1" || current.Status.ReadyReplicas != 2 {
		t.Fatalf("expected the service to serve v1 with 2 replicas, got %+v", current.Status)
	}
}

// One movement at a time: a second rollout while one is running is refused.
func TestSecondRolloutDuringARunningRolloutIsRefused(t *testing.T) {
	f := newRolloutFixture(t, "svc-concurrent", RolloutConfig{MaxSurge: 1, MaxUnavailable: 1})

	if _, err := f.sup.RolloutUpgrade(f.ctx, "tenant-r", "svc-concurrent", "v2"); err != nil {
		t.Fatalf("RolloutUpgrade failed: %v", err)
	}
	if _, err := f.sup.RolloutUpgrade(f.ctx, "tenant-r", "svc-concurrent", "v3"); err == nil {
		t.Fatal("expected the second rollout to be refused")
	} else if !strings.Contains(err.Error(), "still in progress") {
		t.Fatalf("unexpected error: %v", err)
	}
}
