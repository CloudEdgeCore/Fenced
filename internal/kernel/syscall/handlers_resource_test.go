package syscall

import (
	"context"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/google/uuid"
)

func TestResourceSyscallHandlersAndClient(t *testing.T) {
	ctx := context.Background()
	nsStore := namespace.NewMemoryStore()

	dispatcher := NewDispatcher(WithAllowedTenant("tenant-test"))
	resHandler := NewResourceSyscallHandler(nsStore)
	dispatcher.Register(SysNamespaceGet, resHandler)
	dispatcher.Register(SysResourceQuotaGet, resHandler)
	dispatcher.Register(SysResourceUsageGet, resHandler)

	client := NewClient(dispatcher)

	tenant := "tenant-test"
	identity := AttemptIdentity{
		TenantID:     tenant,
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	// 1. Get default namespace
	info, err := client.GetNamespace(ctx, identity, NamespaceGetPayload{Namespace: "default"})
	if err != nil {
		t.Fatalf("failed to get default namespace: %v", err)
	}
	if info.Name != "default" || info.Phase != namespace.NamespacePhaseActive {
		t.Fatalf("unexpected namespace info: %+v", info)
	}

	// 2. Create custom namespace and retrieve it
	customNS := &namespace.Namespace{
		TenantID:    tenant,
		Name:        "ml-ops",
		DisplayName: "Machine Learning Operations",
		Description: "Namespace for training and batch inference",
		Phase:       namespace.NamespacePhaseActive,
		Quota: namespace.ResourceQuota{
			MaxServices:            3,
			MaxTasks:               10,
			MaxTokens:              500000,
			AllowCrossNamespaceIPC: true,
		},
	}
	if err := nsStore.CreateNamespace(ctx, customNS); err != nil {
		t.Fatalf("failed to create custom namespace: %v", err)
	}

	customInfo, err := client.GetNamespace(ctx, identity, NamespaceGetPayload{Namespace: "ml-ops"})
	if err != nil {
		t.Fatalf("failed to get custom namespace info: %v", err)
	}
	if customInfo.DisplayName != "Machine Learning Operations" || customInfo.Quota.MaxServices != 3 {
		t.Fatalf("unexpected custom info: %+v", customInfo)
	}

	// 3. Get Resource Quota
	quotaInfo, err := client.GetResourceQuota(ctx, identity, ResourceQuotaPayload{Namespace: "ml-ops"})
	if err != nil {
		t.Fatalf("failed to get quota info: %v", err)
	}
	if quotaInfo.Quota.MaxTokens != 500000 || !quotaInfo.Quota.AllowCrossNamespaceIPC {
		t.Fatalf("unexpected quota info: %+v", quotaInfo)
	}

	// 4. Record usage and get Resource Usage
	_ = nsStore.RecordUsageDelta(ctx, tenant, "ml-ops", namespace.ResourceUsageDelta{
		ActiveServicesDelta: 2,
		ActiveTasksDelta:    4,
		ConsumedTokensDelta: 125000,
	})

	usageInfo, err := client.GetResourceUsage(ctx, identity, ResourceUsagePayload{Namespace: "ml-ops"})
	if err != nil {
		t.Fatalf("failed to get usage info: %v", err)
	}
	if usageInfo.Usage.ActiveServices != 2 || usageInfo.Usage.ActiveTasks != 4 || usageInfo.Usage.ConsumedTokens != 125000 {
		t.Fatalf("unexpected usage info: %+v", usageInfo)
	}

	// 5. Query non-existent namespace
	_, err = client.GetNamespace(ctx, identity, NamespaceGetPayload{Namespace: "does-not-exist"})
	if err == nil {
		t.Fatal("expected error querying non-existent namespace, got nil")
	}
}

func TestResourceSyscallUnconfigured(t *testing.T) {
	ctx := context.Background()
	dispatcher := NewDispatcher(WithAllowedTenant("tenant-test"))
	// Handler with nil store
	resHandler := NewResourceSyscallHandler(nil)
	dispatcher.Register(SysNamespaceGet, resHandler)

	client := NewClient(dispatcher)
	identity := AttemptIdentity{
		TenantID:     "tenant-test",
		AttemptID:    uuid.New(),
		FencingToken: 1,
	}

	_, err := client.GetNamespace(ctx, identity, NamespaceGetPayload{Namespace: "default"})
	if err == nil {
		t.Fatal("expected error with unconfigured store, got nil")
	}
}
