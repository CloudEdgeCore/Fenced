//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
)

// The rollout state has to survive a controller restart: it lives in the
// durable service status, not in the memory of the process that started it.
func TestPostgresServiceRolloutStateIsDurable(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	engine := supervisor.NewSupervisor(store, supervisor.WithSpawner(supervisor.NewMockSpawner()))
	svc := &supervisor.Service{
		ID:        "svc-pg-rollout",
		TenantID:  "tenant-pg",
		Namespace: "default",
		Name:      "pg-rollout",
		AgentID:   "agent-pg",
		Spec: supervisor.ServiceSpec{
			Replicas:      2,
			AgentVersion:  "v1",
			RestartPolicy: supervisor.RestartAlways,
			Health:        supervisor.DefaultHealthConfig(),
			Rollout: supervisor.RolloutConfig{
				MaxSurge:       1,
				MaxUnavailable: 1,
				AutoRollback:   true,
			},
		},
	}
	if _, err := engine.CreateService(ctx, svc); err != nil {
		t.Fatalf("create service: %v", err)
	}

	upgraded, err := engine.RolloutUpgrade(ctx, "tenant-pg", svc.ID, "v2")
	if err != nil {
		t.Fatalf("rollout: %v", err)
	}
	if upgraded.Status.RolloutPhase != supervisor.RolloutPhaseProgressing {
		t.Fatalf("expected a running rollout, got %q", upgraded.Status.RolloutPhase)
	}

	// The state is durable: a fresh read of the store sees the rollout.
	reloaded, err := store.GetService(ctx, "tenant-pg", svc.ID)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}
	if reloaded.Status.RolloutPhase != supervisor.RolloutPhaseProgressing {
		t.Fatalf("the rollout did not survive the store: %q", reloaded.Status.RolloutPhase)
	}
	if reloaded.Status.RolloutFromVersion != "v1" || reloaded.Status.RolloutTargetVersion != "v2" {
		t.Fatalf("the rollout does not name its versions: %+v", reloaded.Status)
	}

	// The proof of readiness is durable as well: it decides whether a
	// serving instance may be replaced.
	current, err := store.GetService(ctx, "tenant-pg", svc.ID)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}
	if err := engine.ReconcileService(ctx, current); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	instances, err := store.ListInstances(ctx, "tenant-pg", svc.ID)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if len(instances) == 0 {
		t.Fatal("expected instances")
	}
	proved := 0
	for _, inst := range instances {
		if inst.ReadyAt != nil {
			proved++
		}
	}
	if proved != len(instances) {
		t.Fatalf("the readiness proof did not survive the store: %d of %d instances", proved, len(instances))
	}

	// A failing version ends the rollout and returns the service to the
	// version it ran before.
	failing, err := store.ListInstances(ctx, "tenant-pg", svc.ID)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	for _, inst := range failing {
		if inst.AgentVersion != "v2" {
			continue
		}
		inst.Phase = supervisor.InstanceFailed
		inst.ExitReason = "task FAILED: the agent crashed on startup"
		if err := store.UpdateInstance(ctx, inst); err != nil {
			t.Fatalf("mark the instance failed: %v", err)
		}
	}
	current, err = store.GetService(ctx, "tenant-pg", svc.ID)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}
	if err := engine.ReconcileService(ctx, current); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	settled, err := store.GetService(ctx, "tenant-pg", svc.ID)
	if err != nil {
		t.Fatalf("read service: %v", err)
	}
	if settled.Spec.AgentVersion != "v1" {
		t.Fatalf("expected the service to return to v1, got %q", settled.Spec.AgentVersion)
	}
	if settled.Status.RolloutPhase != supervisor.RolloutPhaseRollingBack && settled.Status.RolloutPhase != supervisor.RolloutPhaseRolledBack {
		t.Fatalf("expected the rollout to roll back, got %q", settled.Status.RolloutPhase)
	}
	if settled.Status.RolloutFailureReason == "" {
		t.Fatal("the failure reason was not recorded")
	}
	_ = time.Now
}
