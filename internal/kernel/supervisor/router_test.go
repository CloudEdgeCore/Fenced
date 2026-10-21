package supervisor

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
)

// A service that runs two versions at once is what a rollout looks like. The
// routing has to follow the version: a message that was addressed to one
// version must not land in the mailbox of another one.
func TestRouterRoutesByVersion(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	sup := NewSupervisor(store)

	svc := &Service{
		ID:        "svc-versioned",
		TenantID:  "tenant-a",
		Namespace: "default",
		Name:      "versioned-svc",
		AgentID:   "agent-versioned",
		Spec: ServiceSpec{
			Replicas:      2,
			AgentVersion:  "worker@1.0.0",
			RestartPolicy: RestartAlways,
		},
	}
	if _, err := sup.CreateService(ctx, svc); err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	// The service now runs one instance of the old version and one of the
	// new one - the state of a service in the middle of a rollout.
	instances, err := store.ListInstances(ctx, "tenant-a", "svc-versioned")
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if len(instances) < 1 {
		t.Fatal("expected instances")
	}
	instances[0].AgentVersion = "worker@1.0.0"
	if err := store.UpdateInstance(ctx, instances[0]); err != nil {
		t.Fatalf("update instance: %v", err)
	}
	newInstance := &Instance{
		ID:            "svc-versioned-new",
		ServiceID:     "svc-versioned",
		TenantID:      "tenant-a",
		Namespace:     "default",
		AgentID:       "agent-versioned",
		AgentVersion:  "worker@2.0.0",
		Address:       ipc.NewInstanceAddressForVersion("tenant-a", "default", "agent-versioned", "worker@2.0.0", "svc-versioned-new"),
		Phase:         InstanceRunning,
		LastHeartbeat: time.Now().UTC(),
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
	if err := store.CreateInstance(ctx, newInstance); err != nil {
		t.Fatalf("create instance: %v", err)
	}

	router := NewRouter(sup, store)

	// An address that names the new version reaches the new instance.
	for attempt := 0; attempt < 4; attempt++ {
		resolved, err := router.ResolveAddress(ctx, ipc.NewVersionedAddress("tenant-a", "default", "agent-versioned", "worker@2.0.0"))
		if err != nil {
			t.Fatalf("resolve the new version: %v", err)
		}
		if resolved.AgentVersionRef != "worker@2.0.0" {
			t.Fatalf("expected the new version, got %q", resolved.AgentVersionRef)
		}
		if resolved.InstanceID != "svc-versioned-new" {
			t.Fatalf("expected the new instance, got %q", resolved.InstanceID)
		}
	}

	// An address that names the old version reaches the old instance.
	resolved, err := router.ResolveAddress(ctx, ipc.NewVersionedAddress("tenant-a", "default", "agent-versioned", "worker@1.0.0"))
	if err != nil {
		t.Fatalf("resolve the old version: %v", err)
	}
	if resolved.AgentVersionRef != "worker@1.0.0" {
		t.Fatalf("expected the old version, got %q", resolved.AgentVersionRef)
	}

	// A version nobody runs is not reachable, and the error says so.
	_, err = router.ResolveAddress(ctx, ipc.NewVersionedAddress("tenant-a", "default", "agent-versioned", "worker@3.0.0"))
	if err == nil {
		t.Fatal("expected the missing version to be refused")
	}
	if !strings.Contains(err.Error(), "worker@3.0.0") {
		t.Fatalf("the error does not name the version: %v", err)
	}

	// An address without a version reaches whichever instance is healthy: the
	// caller did not say which one it meant.
	resolved, err = router.ResolveAddress(ctx, ipc.NewAddress("tenant-a", "default", "agent-versioned"))
	if err != nil {
		t.Fatalf("resolve without a version: %v", err)
	}
	if !resolved.IsInstance() {
		t.Fatalf("expected a concrete instance, got %+v", resolved)
	}
}

// An address that names an instance of another version is refused: the
// instance is healthy, but it is not the one that was addressed.
func TestRouterRefusesAnInstanceOfAnotherVersion(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	sup := NewSupervisor(store)

	svc := &Service{
		ID:        "svc-mismatch",
		TenantID:  "tenant-a",
		Namespace: "default",
		Name:      "mismatch-svc",
		AgentID:   "agent-mismatch",
		Spec: ServiceSpec{
			Replicas:      1,
			AgentVersion:  "worker@1.0.0",
			RestartPolicy: RestartAlways,
		},
	}
	if _, err := sup.CreateService(ctx, svc); err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}
	instances, err := store.ListInstances(ctx, "tenant-a", "svc-mismatch")
	if err != nil || len(instances) == 0 {
		t.Fatalf("list instances: %v", err)
	}
	instances[0].AgentVersion = "worker@1.0.0"
	if err := store.UpdateInstance(ctx, instances[0]); err != nil {
		t.Fatalf("update instance: %v", err)
	}

	router := NewRouter(sup, store)
	// The address names the instance and a version it does not run.
	mismatch := ipc.NewInstanceAddressForVersion("tenant-a", "default", "agent-mismatch", "worker@2.0.0", instances[0].ID)
	_, err = router.ResolveAddress(ctx, mismatch)
	if err == nil {
		t.Fatal("expected the version mismatch to be refused")
	}
	if !strings.Contains(err.Error(), "worker@1.0.0") {
		t.Fatalf("the error does not name the version the instance runs: %v", err)
	}
}
