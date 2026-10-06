package namespace

import (
	"context"
	"errors"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func TestEnforcerTaskAdmission(t *testing.T) {
	ctx := context.Background()
	nsStore := NewMemoryStore()
	enforcer := NewEnforcer(nsStore)

	tenant := "tenant-enf"
	nsName := "batch-jobs"

	// Create namespace with quota: MaxTasks = 2, MaxTokens = 1000
	err := nsStore.CreateNamespace(ctx, &Namespace{
		TenantID: tenant,
		Name:     nsName,
		Quota: ResourceQuota{
			MaxTasks:  2,
			MaxTokens: 1000,
		},
	})
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// 1. Task within limits passes
	budget := store.TaskBudget{Tokens: 500}
	if err := enforcer.CheckTaskAdmission(ctx, tenant, nsName, budget); err != nil {
		t.Fatalf("expected task admission to pass, got: %v", err)
	}

	// 2. Budget tokens exceeding ceiling is rejected
	oversizedBudget := store.TaskBudget{Tokens: 1500}
	err = enforcer.CheckTaskAdmission(ctx, tenant, nsName, oversizedBudget)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded for token limit, got: %v", err)
	}

	// 3. Saturate active tasks count
	_ = nsStore.RecordUsageDelta(ctx, tenant, nsName, ResourceUsageDelta{ActiveTasksDelta: 2})
	err = enforcer.CheckTaskAdmission(ctx, tenant, nsName, budget)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded for task count, got: %v", err)
	}
}

func TestEnforcerServiceAdmission(t *testing.T) {
	ctx := context.Background()
	nsStore := NewMemoryStore()
	enforcer := NewEnforcer(nsStore)

	tenant := "tenant-enf"
	nsName := "web-services"

	err := nsStore.CreateNamespace(ctx, &Namespace{
		TenantID: tenant,
		Name:     nsName,
		Quota: ResourceQuota{
			MaxServices:           1,
			MaxReplicasPerService: 3,
		},
	})
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	// 1. Valid service creation passes
	if err := enforcer.CheckServiceAdmission(ctx, tenant, nsName, true, 2); err != nil {
		t.Fatalf("expected service admission to pass, got: %v", err)
	}

	// 2. Replicas exceeding limit rejected
	err = enforcer.CheckServiceAdmission(ctx, tenant, nsName, true, 5)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded for replicas, got: %v", err)
	}

	// 3. Max services reached
	_ = nsStore.RecordUsageDelta(ctx, tenant, nsName, ResourceUsageDelta{ActiveServicesDelta: 1})
	err = enforcer.CheckServiceAdmission(ctx, tenant, nsName, true, 1)
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("expected ErrQuotaExceeded for max services, got: %v", err)
	}

	// Scaling existing service (isNewService = false) still allowed if replicas within limit
	if err := enforcer.CheckServiceAdmission(ctx, tenant, nsName, false, 3); err != nil {
		t.Fatalf("expected scale existing service to pass, got: %v", err)
	}
}

func TestEnforcerIPCPermissions(t *testing.T) {
	ctx := context.Background()
	nsStore := NewMemoryStore()
	enforcer := NewEnforcer(nsStore)

	tenant := "tenant-enf"

	// Create ns-isolated (no cross-namespace allowed)
	_ = nsStore.CreateNamespace(ctx, &Namespace{
		TenantID: tenant,
		Name:     "isolated",
		Quota:    ResourceQuota{AllowCrossNamespaceIPC: false},
	})

	// Create ns-open (cross-namespace allowed)
	_ = nsStore.CreateNamespace(ctx, &Namespace{
		TenantID: tenant,
		Name:     "open-a",
		Quota:    ResourceQuota{AllowCrossNamespaceIPC: true},
	})
	_ = nsStore.CreateNamespace(ctx, &Namespace{
		TenantID: tenant,
		Name:     "open-b",
		Quota:    ResourceQuota{AllowCrossNamespaceIPC: true},
	})

	// 1. Same namespace is always permitted
	if err := enforcer.CheckIPCPermission(ctx, tenant, "isolated", tenant, "isolated"); err != nil {
		t.Fatalf("expected same namespace IPC to pass, got: %v", err)
	}

	// 2. Cross-tenant is forbidden
	if err := enforcer.CheckIPCPermission(ctx, tenant, "open-a", "other-tenant", "open-a"); !errors.Is(err, ErrCrossNamespaceForbidden) {
		t.Fatalf("expected ErrCrossNamespaceForbidden for cross-tenant, got: %v", err)
	}

	// 3. Sender isolated to receiver open should be denied
	err := enforcer.CheckIPCPermission(ctx, tenant, "isolated", tenant, "open-a")
	if !errors.Is(err, ErrCrossNamespaceForbidden) {
		t.Fatalf("expected ErrCrossNamespaceForbidden for sender isolated, got: %v", err)
	}

	// 4. Sender open to receiver isolated should be denied
	err = enforcer.CheckIPCPermission(ctx, tenant, "open-a", tenant, "isolated")
	if !errors.Is(err, ErrCrossNamespaceForbidden) {
		t.Fatalf("expected ErrCrossNamespaceForbidden for receiver isolated, got: %v", err)
	}

	// 5. Open to Open should succeed
	if err := enforcer.CheckIPCPermission(ctx, tenant, "open-a", tenant, "open-b"); err != nil {
		t.Fatalf("expected open-to-open IPC to pass, got: %v", err)
	}
}

func TestEnforcerTerminatingNamespace(t *testing.T) {
	ctx := context.Background()
	nsStore := NewMemoryStore()
	enforcer := NewEnforcer(nsStore)

	tenant := "tenant-enf"
	nsName := "decommissioning"

	err := nsStore.CreateNamespace(ctx, &Namespace{
		TenantID: tenant,
		Name:     nsName,
		Phase:    NamespacePhaseTerminating,
	})
	if err != nil {
		t.Fatalf("failed to create terminating namespace: %v", err)
	}

	err = enforcer.CheckTaskAdmission(ctx, tenant, nsName, store.TaskBudget{})
	if !errors.Is(err, ErrNamespaceTerminating) {
		t.Fatalf("expected ErrNamespaceTerminating, got: %v", err)
	}

	err = enforcer.CheckServiceAdmission(ctx, tenant, nsName, true, 1)
	if !errors.Is(err, ErrNamespaceTerminating) {
		t.Fatalf("expected ErrNamespaceTerminating, got: %v", err)
	}
}
