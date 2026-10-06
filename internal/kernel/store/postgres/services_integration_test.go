//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
)

func TestPostgresServiceAndInstancePersistence(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	svc := &supervisor.Service{
		ID:        "svc-pg-1",
		TenantID:  "tenant-pg",
		Namespace: "default",
		Name:      "pg-service",
		AgentID:   "agent-pg",
		Spec: supervisor.ServiceSpec{
			Replicas:      2,
			RestartPolicy: supervisor.RestartAlways,
			Backoff:       supervisor.DefaultBackoffConfig(),
			Health:        supervisor.DefaultHealthConfig(),
		},
		Status: supervisor.ServiceStatus{
			Phase:            supervisor.ServiceActive,
			DesiredReplicas:  2,
			CurrentReplicas:  2,
			ReadyReplicas:    2,
			LastTransitionAt: clock.Now(),
		},
	}

	// 1. Create service
	if err := store.CreateService(ctx, svc); err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	// 2. Duplicate service name in same namespace should fail
	dupSvc := &supervisor.Service{
		ID:        "svc-pg-2",
		TenantID:  "tenant-pg",
		Namespace: "default",
		Name:      "pg-service",
		AgentID:   "agent-pg-2",
		Spec:      svc.Spec,
		Status:    svc.Status,
	}
	if err := store.CreateService(ctx, dupSvc); err == nil {
		t.Fatalf("expected duplicate service creation to fail")
	}

	// 3. Get service
	got, err := store.GetService(ctx, "tenant-pg", "svc-pg-1")
	if err != nil {
		t.Fatalf("GetService failed: %v", err)
	}
	if got.Name != "pg-service" || got.Spec.Replicas != 2 {
		t.Fatalf("unexpected fetched service: %+v", got)
	}

	// 4. Get service by name
	byName, err := store.GetServiceByName(ctx, "tenant-pg", "default", "pg-service")
	if err != nil {
		t.Fatalf("GetServiceByName failed: %v", err)
	}
	if byName.ID != "svc-pg-1" {
		t.Fatalf("unexpected service ID from GetServiceByName: %s", byName.ID)
	}

	// 5. List services
	list, err := store.ListServices(ctx, "tenant-pg", "default")
	if err != nil {
		t.Fatalf("ListServices failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 service, got %d", len(list))
	}

	// 6. Create instances
	inst1 := &supervisor.Instance{
		ID:            "inst-1",
		ServiceID:     "svc-pg-1",
		TenantID:      "tenant-pg",
		Namespace:     "default",
		AgentID:       "agent-pg",
		Address:       ipc.NewInstanceAddress("tenant-pg", "default", "agent-pg", "inst-1"),
		Phase:         supervisor.InstanceRunning,
		LastHeartbeat: clock.Now(),
		CreatedAt:     clock.Now(),
		UpdatedAt:     clock.Now(),
	}
	inst2 := &supervisor.Instance{
		ID:            "inst-2",
		ServiceID:     "svc-pg-1",
		TenantID:      "tenant-pg",
		Namespace:     "default",
		AgentID:       "agent-pg",
		Address:       ipc.NewInstanceAddress("tenant-pg", "default", "agent-pg", "inst-2"),
		Phase:         supervisor.InstanceRunning,
		LastHeartbeat: clock.Now(),
		CreatedAt:     clock.Now(),
		UpdatedAt:     clock.Now(),
	}
	if err := store.CreateInstance(ctx, inst1); err != nil {
		t.Fatalf("CreateInstance 1 failed: %v", err)
	}
	if err := store.CreateInstance(ctx, inst2); err != nil {
		t.Fatalf("CreateInstance 2 failed: %v", err)
	}

	// 7. Get instance
	gotInst, err := store.GetInstance(ctx, "tenant-pg", "svc-pg-1", "inst-1")
	if err != nil {
		t.Fatalf("GetInstance failed: %v", err)
	}
	if gotInst.Phase != supervisor.InstanceRunning || gotInst.Address.InstanceID != "inst-1" {
		t.Fatalf("unexpected instance: %+v", gotInst)
	}

	// 8. Update instance (heartbeat + state change)
	gotInst.Phase = supervisor.InstanceDegraded
	gotInst.ConsecutiveFailures = 1
	if err := store.UpdateInstance(ctx, gotInst); err != nil {
		t.Fatalf("UpdateInstance failed: %v", err)
	}
	updatedInst, _ := store.GetInstance(ctx, "tenant-pg", "svc-pg-1", "inst-1")
	if updatedInst.Phase != supervisor.InstanceDegraded || updatedInst.ConsecutiveFailures != 1 {
		t.Fatalf("unexpected updated instance: %+v", updatedInst)
	}

	// 9. List instances
	instances, err := store.ListInstances(ctx, "tenant-pg", "svc-pg-1")
	if err != nil {
		t.Fatalf("ListInstances failed: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}

	// 10. Delete instance
	if err := store.DeleteInstance(ctx, "tenant-pg", "svc-pg-1", "inst-2"); err != nil {
		t.Fatalf("DeleteInstance failed: %v", err)
	}
	_, err = store.GetInstance(ctx, "tenant-pg", "svc-pg-1", "inst-2")
	if err != supervisor.ErrInstanceNotFound {
		t.Fatalf("expected ErrInstanceNotFound, got %v", err)
	}

	// 11. Delete service cascade
	if err := store.DeleteService(ctx, "tenant-pg", "svc-pg-1"); err != nil {
		t.Fatalf("DeleteService failed: %v", err)
	}
	_, err = store.GetService(ctx, "tenant-pg", "svc-pg-1")
	if err != supervisor.ErrServiceNotFound {
		t.Fatalf("expected ErrServiceNotFound, got %v", err)
	}
	// Instance 1 should also be cascade deleted
	_, err = store.GetInstance(ctx, "tenant-pg", "svc-pg-1", "inst-1")
	if err != supervisor.ErrInstanceNotFound {
		t.Fatalf("expected ErrInstanceNotFound for cascade deleted instance, got %v", err)
	}
}
