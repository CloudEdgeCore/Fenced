//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
)

// The version aware routing resolves a logical target over the durable
// instances: the instances of an agent, with the version they run.
func TestPostgresServiceInstancesByAgent(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	svc := &supervisor.Service{
		ID:        "svc-pg-agent",
		TenantID:  "tenant-pg",
		Namespace: "default",
		Name:      "pg-agent",
		AgentID:   "worker",
		Spec: supervisor.ServiceSpec{
			Replicas:      2,
			AgentVersion:  "worker@1.0.0",
			RestartPolicy: supervisor.RestartAlways,
		},
		Status: supervisor.ServiceStatus{Phase: supervisor.ServiceActive},
	}
	if err := store.CreateService(ctx, svc); err != nil {
		t.Fatalf("create service: %v", err)
	}
	now := clock.Now()
	for _, version := range []string{"worker@1.0.0", "worker@2.0.0"} {
		instance := &supervisor.Instance{
			ID:            "svc-pg-agent-" + version,
			ServiceID:     svc.ID,
			TenantID:      svc.TenantID,
			Namespace:     svc.Namespace,
			AgentID:       svc.AgentID,
			AgentVersion:  version,
			Address:       ipc.NewInstanceAddressForVersion(svc.TenantID, svc.Namespace, svc.AgentID, version, "svc-pg-agent-"+version),
			Phase:         supervisor.InstanceRunning,
			LastHeartbeat: now,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := store.CreateInstance(ctx, instance); err != nil {
			t.Fatalf("create instance: %v", err)
		}
	}

	instances, err := store.ListServiceInstancesByAgent(ctx, "tenant-pg", "default", "worker")
	if err != nil {
		t.Fatalf("list the instances of the agent: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("expected two instances, got %d", len(instances))
	}
	versions := map[string]bool{}
	for _, instance := range instances {
		versions[instance.AgentVersion] = true
		if instance.AgentID != "worker" {
			t.Fatalf("the instance belongs to another agent: %+v", instance)
		}
	}
	if !versions["worker@1.0.0"] || !versions["worker@2.0.0"] {
		t.Fatalf("the versions are not distinguishable: %v", versions)
	}

	// Another agent has no instances.
	others, err := store.ListServiceInstancesByAgent(ctx, "tenant-pg", "default", "planner")
	if err != nil {
		t.Fatalf("list the instances of another agent: %v", err)
	}
	if len(others) != 0 {
		t.Fatalf("expected no instances, got %d", len(others))
	}

	// Another tenant sees nothing.
	foreign, err := store.ListServiceInstancesByAgent(ctx, "tenant-other", "default", "worker")
	if err != nil {
		t.Fatalf("list the instances of another tenant: %v", err)
	}
	if len(foreign) != 0 {
		t.Fatalf("another tenant saw %d instances", len(foreign))
	}
}
