package supervisor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
)

func TestServiceCRUD(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	svc := &Service{
		ID:        "svc-test-1",
		TenantID:  "tenant-a",
		Namespace: "default",
		Name:      "test-service",
		AgentID:   "agent-alpha",
		Spec: ServiceSpec{
			Replicas:      2,
			RestartPolicy: RestartAlways,
		},
	}

	// Create
	if err := store.CreateService(ctx, svc); err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	// Duplicate ID should fail
	if err := store.CreateService(ctx, svc); err == nil {
		t.Fatalf("expected error creating duplicate service ID, got nil")
	}

	// Duplicate Name in same namespace should fail
	svcDupName := &Service{
		ID:        "svc-test-2",
		TenantID:  "tenant-a",
		Namespace: "default",
		Name:      "test-service",
		AgentID:   "agent-beta",
		Spec: ServiceSpec{
			Replicas: 1,
		},
	}
	if err := store.CreateService(ctx, svcDupName); err == nil {
		t.Fatalf("expected error creating duplicate service name, got nil")
	}

	// Get
	got, err := store.GetService(ctx, "tenant-a", "svc-test-1")
	if err != nil {
		t.Fatalf("GetService failed: %v", err)
	}
	if got.Name != "test-service" || got.Spec.Replicas != 2 {
		t.Fatalf("unexpected service: %+v", got)
	}

	// Get by Name
	byName, err := store.GetServiceByName(ctx, "tenant-a", "default", "test-service")
	if err != nil {
		t.Fatalf("GetServiceByName failed: %v", err)
	}
	if byName.ID != "svc-test-1" {
		t.Fatalf("unexpected ID from GetServiceByName: %s", byName.ID)
	}

	// List
	list, err := store.ListServices(ctx, "tenant-a", "default")
	if err != nil {
		t.Fatalf("ListServices failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 service, got %d", len(list))
	}

	// Update
	got.Spec.Replicas = 4
	if err := store.UpdateService(ctx, got); err != nil {
		t.Fatalf("UpdateService failed: %v", err)
	}
	updated, _ := store.GetService(ctx, "tenant-a", "svc-test-1")
	if updated.Spec.Replicas != 4 {
		t.Fatalf("expected 4 replicas, got %d", updated.Spec.Replicas)
	}

	// Delete
	if err := store.DeleteService(ctx, "tenant-a", "svc-test-1"); err != nil {
		t.Fatalf("DeleteService failed: %v", err)
	}
	_, err = store.GetService(ctx, "tenant-a", "svc-test-1")
	if err != ErrServiceNotFound {
		t.Fatalf("expected ErrServiceNotFound, got %v", err)
	}
}

func TestSupervisorReconciliationAndScaling(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()

	curTime := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curTime }

	sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))

	svc := &Service{
		ID:        "svc-scale",
		TenantID:  "tenant-a",
		Namespace: "prod",
		Name:      "scaler",
		AgentID:   "agent-scale",
		Spec: ServiceSpec{
			Replicas:      3,
			RestartPolicy: RestartAlways,
		},
	}

	created, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	// Check that 3 instances were spawned and status is Active
	if created.Status.Phase != ServiceActive {
		t.Fatalf("expected phase Active, got %s", created.Status.Phase)
	}
	if created.Status.ReadyReplicas != 3 {
		t.Fatalf("expected 3 ready replicas, got %d", created.Status.ReadyReplicas)
	}
	if spawner.SpawnCount() != 3 {
		t.Fatalf("expected 3 spawned instances, got %d", spawner.SpawnCount())
	}

	// Scale down to 1
	scaled, err := sup.ScaleService(ctx, "tenant-a", "svc-scale", 1)
	if err != nil {
		t.Fatalf("ScaleService down failed: %v", err)
	}
	if scaled.Status.ReadyReplicas != 1 {
		t.Fatalf("expected 1 ready replica, got %d", scaled.Status.ReadyReplicas)
	}
	if spawner.StopCount() != 2 {
		t.Fatalf("expected 2 stopped instances, got %d", spawner.StopCount())
	}

	// Scale down to 0
	stopped, err := sup.ScaleService(ctx, "tenant-a", "svc-scale", 0)
	if err != nil {
		t.Fatalf("ScaleService to 0 failed: %v", err)
	}
	if stopped.Status.Phase != ServiceSuspended {
		t.Fatalf("expected phase Suspended, got %s", stopped.Status.Phase)
	}
	if stopped.Status.ReadyReplicas != 0 {
		t.Fatalf("expected 0 ready replicas, got %d", stopped.Status.ReadyReplicas)
	}

	// Scale back up to 2
	scaledUp, err := sup.ScaleService(ctx, "tenant-a", "svc-scale", 2)
	if err != nil {
		t.Fatalf("ScaleService up failed: %v", err)
	}
	if scaledUp.Status.Phase != ServiceActive {
		t.Fatalf("expected phase Active, got %s", scaledUp.Status.Phase)
	}
	if scaledUp.Status.ReadyReplicas != 2 {
		t.Fatalf("expected 2 ready replicas, got %d", scaledUp.Status.ReadyReplicas)
	}
}

func TestHeartbeatAndHealthCheck(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()

	curTime := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curTime }

	sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))

	svc := &Service{
		ID:        "svc-hb",
		TenantID:  "tenant-a",
		Namespace: "default",
		Name:      "hb-svc",
		AgentID:   "agent-hb",
		Spec: ServiceSpec{
			Replicas:      1,
			RestartPolicy: RestartNever,
			Health: HealthConfig{
				HeartbeatTTL:       10 * time.Second,
				UnhealthyThreshold: 2,
				CheckInterval:      5 * time.Second,
			},
		},
	}

	_, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	instances, _ := store.ListInstances(ctx, "tenant-a", "svc-hb")
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	instID := instances[0].ID

	// Send valid heartbeat
	curTime = curTime.Add(5 * time.Second)
	if err := sup.RecordHeartbeat(ctx, "tenant-a", "svc-hb", instID); err != nil {
		t.Fatalf("RecordHeartbeat failed: %v", err)
	}

	// Reconcile -> still healthy
	svcObj, _ := store.GetService(ctx, "tenant-a", "svc-hb")
	_ = sup.ReconcileService(ctx, svcObj)
	inst, _ := store.GetInstance(ctx, "tenant-a", "svc-hb", instID)
	if inst.Phase != InstanceRunning {
		t.Fatalf("expected Running, got %s", inst.Phase)
	}

	// Advance time past HeartbeatTTL (15s after last heartbeat)
	curTime = curTime.Add(15 * time.Second)
	_ = sup.ReconcileService(ctx, svcObj)

	// First failure: instance marked Degraded
	inst, _ = store.GetInstance(ctx, "tenant-a", "svc-hb", instID)
	if inst.Phase != InstanceDegraded {
		t.Fatalf("expected Degraded on first missed heartbeat, got %s", inst.Phase)
	}

	// Advance time again -> second missed heartbeat reached UnhealthyThreshold (2)
	_ = sup.ReconcileService(ctx, svcObj)
	inst, _ = store.GetInstance(ctx, "tenant-a", "svc-hb", instID)
	if inst.Phase != InstanceFailed {
		t.Fatalf("expected Failed after reaching UnhealthyThreshold, got %s", inst.Phase)
	}
	if inst.ExitCode != -1 {
		t.Fatalf("expected ExitCode -1, got %d", inst.ExitCode)
	}
}

func TestCrashAndRestartPolicies(t *testing.T) {
	ctx := context.Background()

	t.Run("RestartAlways with exponential backoff and max retries", func(t *testing.T) {
		store := NewMemoryStore()
		spawner := NewMockSpawner()
		curTime := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
		clock := func() time.Time { return curTime }

		sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))

		svc := &Service{
			ID:        "svc-restart-always",
			TenantID:  "tenant-a",
			Namespace: "default",
			Name:      "always-svc",
			AgentID:   "agent-always",
			Spec: ServiceSpec{
				Replicas:      1,
				RestartPolicy: RestartAlways,
				Backoff: BackoffConfig{
					InitialInterval: 1 * time.Second,
					MaxInterval:     10 * time.Second,
					Factor:          2.0,
					MaxRetries:      2,
				},
			},
		}

		_, err := sup.CreateService(ctx, svc)
		if err != nil {
			t.Fatalf("CreateService failed: %v", err)
		}

		instances, _ := store.ListInstances(ctx, "tenant-a", "svc-restart-always")
		instID := instances[0].ID

		// Crash 1: report exit code 1
		if err := sup.ReportInstanceExit(ctx, "tenant-a", "svc-restart-always", instID, 1, "process crashed"); err != nil {
			t.Fatalf("ReportInstanceExit failed: %v", err)
		}

		inst, _ := store.GetInstance(ctx, "tenant-a", "svc-restart-always", instID)
		if inst.Phase != InstanceFailed {
			t.Fatalf("expected Failed, got %s", inst.Phase)
		}
		if inst.NextRestartAt == nil {
			t.Fatalf("expected NextRestartAt to be scheduled")
		}

		// Before NextRestartAt: should NOT restart yet
		svcObj, _ := store.GetService(ctx, "tenant-a", "svc-restart-always")
		_ = sup.ReconcileService(ctx, svcObj)
		inst, _ = store.GetInstance(ctx, "tenant-a", "svc-restart-always", instID)
		if inst.Phase != InstanceFailed {
			t.Fatalf("expected still Failed before NextRestartAt, got %s", inst.Phase)
		}

		// Advance clock past NextRestartAt (1s)
		curTime = curTime.Add(2 * time.Second)
		_ = sup.ReconcileService(ctx, svcObj)
		inst, _ = store.GetInstance(ctx, "tenant-a", "svc-restart-always", instID)
		if inst.Phase != InstanceRunning {
			t.Fatalf("expected Running after restart, got %s", inst.Phase)
		}
		if inst.RestartCount != 1 {
			t.Fatalf("expected RestartCount 1, got %d", inst.RestartCount)
		}

		// Crash 2
		_ = sup.ReportInstanceExit(ctx, "tenant-a", "svc-restart-always", instID, 2, "crashed again")
		// Advance clock past backoff (2s)
		curTime = curTime.Add(3 * time.Second)
		_ = sup.ReconcileService(ctx, svcObj)
		inst, _ = store.GetInstance(ctx, "tenant-a", "svc-restart-always", instID)
		if inst.RestartCount != 2 {
			t.Fatalf("expected RestartCount 2, got %d", inst.RestartCount)
		}

		// Crash 3: reaches MaxRetries (2) -> should NOT restart
		_ = sup.ReportInstanceExit(ctx, "tenant-a", "svc-restart-always", instID, 3, "crashed 3rd time")
		curTime = curTime.Add(10 * time.Second)
		_ = sup.ReconcileService(ctx, svcObj)
		inst, _ = store.GetInstance(ctx, "tenant-a", "svc-restart-always", instID)
		if inst.Phase != InstanceFailed {
			t.Fatalf("expected Failed after max retries exceeded, got %s", inst.Phase)
		}
	})

	t.Run("RestartOnFailure clean exit vs crash", func(t *testing.T) {
		store := NewMemoryStore()
		spawner := NewMockSpawner()
		curTime := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
		clock := func() time.Time { return curTime }

		sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))

		svc := &Service{
			ID:        "svc-restart-failure",
			TenantID:  "tenant-a",
			Namespace: "default",
			Name:      "failure-svc",
			AgentID:   "agent-failure",
			Spec: ServiceSpec{
				Replicas:      1,
				RestartPolicy: RestartOnFailure,
			},
		}

		_, _ = sup.CreateService(ctx, svc)
		instances, _ := store.ListInstances(ctx, "tenant-a", "svc-restart-failure")
		instID := instances[0].ID

		// Clean exit (exitCode 0) -> no restart
		_ = sup.ReportInstanceExit(ctx, "tenant-a", "svc-restart-failure", instID, 0, "task completed gracefully")
		inst, _ := store.GetInstance(ctx, "tenant-a", "svc-restart-failure", instID)
		if inst.Phase != InstanceStopped {
			t.Fatalf("expected Stopped, got %s", inst.Phase)
		}
		if inst.NextRestartAt != nil {
			t.Fatalf("expected no NextRestartAt for clean exit under RestartOnFailure")
		}
	})

	t.Run("RestartNever", func(t *testing.T) {
		store := NewMemoryStore()
		spawner := NewMockSpawner()
		curTime := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
		clock := func() time.Time { return curTime }

		sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))

		svc := &Service{
			ID:        "svc-restart-never",
			TenantID:  "tenant-a",
			Namespace: "default",
			Name:      "never-svc",
			AgentID:   "agent-never",
			Spec: ServiceSpec{
				Replicas:      1,
				RestartPolicy: RestartNever,
			},
		}

		_, _ = sup.CreateService(ctx, svc)
		instances, _ := store.ListInstances(ctx, "tenant-a", "svc-restart-never")
		instID := instances[0].ID

		_ = sup.ReportInstanceExit(ctx, "tenant-a", "svc-restart-never", instID, 137, "killed")
		inst, _ := store.GetInstance(ctx, "tenant-a", "svc-restart-never", instID)
		if inst.Phase != InstanceFailed {
			t.Fatalf("expected Failed, got %s", inst.Phase)
		}
		if inst.NextRestartAt != nil {
			t.Fatalf("expected no restart scheduled for RestartNever")
		}
	})
}

func TestAutoWakeOnMailbox(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()
	mailbox := ipc.NewDurableMemoryMailbox()

	sup := NewSupervisor(store, WithSpawner(spawner), WithMailbox(mailbox))

	svc := &Service{
		ID:        "svc-autowake",
		TenantID:  "tenant-a",
		Namespace: "default",
		Name:      "autowake-svc",
		AgentID:   "agent-worker",
		Spec: ServiceSpec{
			Replicas:      0, // Start with 0 replicas
			AutoWake:      true,
			RestartPolicy: RestartAlways,
		},
	}

	_, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	// 0 replicas initial
	insts, _ := store.ListInstances(ctx, "tenant-a", "svc-autowake")
	if len(insts) != 0 {
		t.Fatalf("expected 0 instances initially, got %d", len(insts))
	}

	// Send a message to the agent's logical mailbox
	msg, err := ipc.NewMessage(
		ipc.NewAddress("tenant-a", "default", "client-1"),
		ipc.NewAddress("tenant-a", "default", "agent-worker"),
		ipc.MessageTypeRequest,
		[]byte("hello autowake"),
		time.Minute,
	)
	if err != nil {
		t.Fatalf("NewMessage failed: %v", err)
	}
	if err := mailbox.Send(ctx, msg); err != nil {
		t.Fatalf("Send message failed: %v", err)
	}

	// Router resolves address -> triggers wakeup
	router := NewRouter(sup, store)
	resolved, err := router.ResolveAddress(ctx, ipc.NewAddress("tenant-a", "default", "agent-worker"))
	if err != nil {
		t.Fatalf("ResolveAddress with AutoWake failed: %v", err)
	}

	if resolved.AgentID != "agent-worker" || !resolved.IsInstance() {
		t.Fatalf("expected resolved concrete instance address, got %+v", resolved)
	}

	// Verify that an instance was spawned
	instsAfter, _ := store.ListInstances(ctx, "tenant-a", "svc-autowake")
	if len(instsAfter) != 1 {
		t.Fatalf("expected 1 instance after AutoWake, got %d", len(instsAfter))
	}
	if instsAfter[0].Phase != InstanceRunning {
		t.Fatalf("expected instance running, got %s", instsAfter[0].Phase)
	}
}

func TestRouterRoundRobin(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	sup := NewSupervisor(store)

	svc := &Service{
		ID:        "svc-rr",
		TenantID:  "tenant-a",
		Namespace: "default",
		Name:      "rr-svc",
		AgentID:   "agent-rr",
		Spec: ServiceSpec{
			Replicas: 2,
		},
	}

	_, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	router := NewRouter(sup, store)
	target := ipc.NewAddress("tenant-a", "default", "agent-rr")

	addr1, err := router.ResolveAddress(ctx, target)
	if err != nil {
		t.Fatalf("ResolveAddress 1 failed: %v", err)
	}
	addr2, err := router.ResolveAddress(ctx, target)
	if err != nil {
		t.Fatalf("ResolveAddress 2 failed: %v", err)
	}

	if addr1.InstanceID == addr2.InstanceID {
		t.Fatalf("expected round robin across different instances, got same: %s", addr1.InstanceID)
	}
}

func TestConcurrentHeartbeatsAndReconciliation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()
	sup := NewSupervisor(store, WithSpawner(spawner))

	svc := &Service{
		ID:        "svc-concurrent",
		TenantID:  "tenant-c",
		Namespace: "default",
		Name:      "concurrent-svc",
		AgentID:   "agent-concurrent",
		Spec: ServiceSpec{
			Replicas: 5,
		},
	}

	_, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	instances, _ := store.ListInstances(ctx, "tenant-c", "svc-concurrent")
	if len(instances) != 5 {
		t.Fatalf("expected 5 instances, got %d", len(instances))
	}

	var wg sync.WaitGroup
	var hbSuccess atomic.Int64

	// Concurrently send heartbeats and reconcile
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			instID := instances[idx%len(instances)].ID
			for j := 0; j < 50; j++ {
				if err := sup.RecordHeartbeat(ctx, "tenant-c", "svc-concurrent", instID); err == nil {
					hbSuccess.Add(1)
				}
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				svcObj, err := store.GetService(ctx, "tenant-c", "svc-concurrent")
				if err == nil {
					_ = sup.ReconcileService(ctx, svcObj)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	if hbSuccess.Load() == 0 {
		t.Fatalf("expected heartbeats to succeed concurrently")
	}
}

func TestServiceDrainingAndGracefulScaleDown(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()
	curTime := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curTime }

	sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))
	router := NewRouter(sup, store)

	svc := &Service{
		ID:        "svc-drain-test",
		TenantID:  "tenant-d",
		Namespace: "default",
		Name:      "drain-service",
		AgentID:   "agent-drain",
		Spec: ServiceSpec{
			Replicas:      3,
			RestartPolicy: RestartAlways,
			DrainTimeout:  10 * time.Second,
		},
	}

	created, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}
	if created.Status.ReadyReplicas != 3 {
		t.Fatalf("expected 3 ready replicas, got %d", created.Status.ReadyReplicas)
	}

	instances, err := store.ListInstances(ctx, "tenant-d", "svc-drain-test")
	if err != nil || len(instances) != 3 {
		t.Fatalf("expected 3 instances in store, got %d (err: %v)", len(instances), err)
	}

	// Scale down to 1: 2 instances should enter Draining, not immediately stop
	scaled, err := sup.ScaleService(ctx, "tenant-d", "svc-drain-test", 1)
	if err != nil {
		t.Fatalf("ScaleService failed: %v", err)
	}
	if scaled.Status.DesiredReplicas != 1 {
		t.Fatalf("expected desired replicas 1, got %d", scaled.Status.DesiredReplicas)
	}

	instances, _ = store.ListInstances(ctx, "tenant-d", "svc-drain-test")
	var drainingCount, runningCount int
	var runningInst *Instance
	for _, inst := range instances {
		if inst.Phase == InstanceDraining {
			drainingCount++
			if inst.DrainingAt == nil || inst.DrainDeadline == nil {
				t.Fatalf("expected DrainingAt and DrainDeadline to be set on draining instance")
			}
		} else if inst.Phase == InstanceRunning {
			runningCount++
			runningInst = inst
		}
	}
	if drainingCount != 2 || runningCount != 1 {
		t.Fatalf("expected 2 draining and 1 running, got %d draining and %d running", drainingCount, runningCount)
	}

	// Spawner should NOT have stopped the instances yet!
	if spawner.StopCount() != 0 {
		t.Fatalf("expected 0 stopped instances during drain grace period, got %d", spawner.StopCount())
	}

	// Router should exclusively route to the single non-draining running instance
	for i := 0; i < 5; i++ {
		resolved, err := router.ResolveAddress(ctx, svc.LogicalAddress())
		if err != nil {
			t.Fatalf("router.ResolveAddress failed: %v", err)
		}
		if resolved.InstanceID != runningInst.ID {
			t.Fatalf("expected routed to running instance %s, got %s", runningInst.ID, resolved.InstanceID)
		}
	}

	// Advance clock past drain deadline (10s)
	curTime = curTime.Add(11 * time.Second)

	// Reconcile service: draining instances should now complete shutdown
	svcObj, _ := store.GetService(ctx, "tenant-d", "svc-drain-test")
	if err := sup.ReconcileService(ctx, svcObj); err != nil {
		t.Fatalf("ReconcileService failed: %v", err)
	}

	if spawner.StopCount() != 2 {
		t.Fatalf("expected 2 stopped instances after drain deadline, got %d", spawner.StopCount())
	}

	instances, _ = store.ListInstances(ctx, "tenant-d", "svc-drain-test")
	var stoppedCount int
	for _, inst := range instances {
		if inst.Phase == InstanceStopped {
			stoppedCount++
			if inst.ExitReason != "drained" {
				t.Fatalf("expected exit reason 'drained', got %q", inst.ExitReason)
			}
		}
	}
	if stoppedCount != 2 {
		t.Fatalf("expected 2 stopped instances in store, got %d", stoppedCount)
	}
}

func TestManualDrainInstance(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()
	curTime := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curTime }

	sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))

	svc := &Service{
		ID:        "svc-manual-drain",
		TenantID:  "tenant-d",
		Namespace: "default",
		Name:      "manual-drain-service",
		AgentID:   "agent-manual-drain",
		Spec: ServiceSpec{
			Replicas:      2,
			RestartPolicy: RestartAlways,
		},
	}

	_, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}

	instances, _ := store.ListInstances(ctx, "tenant-d", "svc-manual-drain")
	targetID := instances[0].ID

	// Manually drain target instance with 5s timeout
	if err := sup.DrainInstance(ctx, "tenant-d", "svc-manual-drain", targetID, 5*time.Second); err != nil {
		t.Fatalf("DrainInstance failed: %v", err)
	}

	inst, _ := store.GetInstance(ctx, "tenant-d", "svc-manual-drain", targetID)
	if inst.Phase != InstanceDraining {
		t.Fatalf("expected phase Draining, got %s", inst.Phase)
	}

	// Advance clock past 5s
	curTime = curTime.Add(6 * time.Second)
	svcObj, _ := store.GetService(ctx, "tenant-d", "svc-manual-drain")
	_ = sup.ReconcileService(ctx, svcObj)

	inst, _ = store.GetInstance(ctx, "tenant-d", "svc-manual-drain", targetID)
	if inst.Phase != InstanceStopped {
		t.Fatalf("expected phase Stopped after drain, got %s", inst.Phase)
	}
}

func TestServiceRollingUpgradeAndRollback(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	spawner := NewMockSpawner()
	curTime := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return curTime }

	sup := NewSupervisor(store, WithSpawner(spawner), WithClock(clock))

	svc := &Service{
		ID:        "svc-rollout",
		TenantID:  "tenant-r",
		Namespace: "default",
		Name:      "rollout-service",
		AgentID:   "agent-rollout",
		Spec: ServiceSpec{
			Replicas:      3,
			AgentVersion:  "v1",
			RestartPolicy: RestartAlways,
			DrainTimeout:  5 * time.Second,
			Rollout: RolloutConfig{
				MaxSurge:       1,
				MaxUnavailable: 1,
			},
		},
	}

	created, err := sup.CreateService(ctx, svc)
	if err != nil {
		t.Fatalf("CreateService failed: %v", err)
	}
	if created.Status.ActiveVersion != "v1" || created.Status.ReadyReplicas != 3 {
		t.Fatalf("unexpected created status: %+v", created.Status)
	}

	// 1. Trigger Rollout Upgrade to "v2"
	upgraded, err := sup.RolloutUpgrade(ctx, "tenant-r", "svc-rollout", "v2")
	if err != nil {
		t.Fatalf("RolloutUpgrade failed: %v", err)
	}
	if upgraded.Spec.AgentVersion != "v2" || upgraded.Status.PreviousVersion != "v1" {
		t.Fatalf("expected target v2 and previous v1, got: %+v", upgraded)
	}

	// Step-by-step rolling progression
	for step := 0; step < 10; step++ {
		curTime = curTime.Add(6 * time.Second)
		svcObj, _ := store.GetService(ctx, "tenant-r", "svc-rollout")
		_ = sup.ReconcileService(ctx, svcObj)

		current, _ := store.GetService(ctx, "tenant-r", "svc-rollout")
		if current.Status.UpdatedReplicas == 3 && current.Status.ReadyReplicas == 3 && current.Status.Phase == ServiceActive {
			break
		}
	}

	finalSvc, _ := store.GetService(ctx, "tenant-r", "svc-rollout")
	if finalSvc.Status.Phase != ServiceActive {
		t.Fatalf("expected final phase Active, got %s", finalSvc.Status.Phase)
	}
	if finalSvc.Status.UpdatedReplicas != 3 {
		t.Fatalf("expected 3 updated replicas, got %d", finalSvc.Status.UpdatedReplicas)
	}
	if finalSvc.Status.ActiveVersion != "v2" {
		t.Fatalf("expected active version v2, got %s", finalSvc.Status.ActiveVersion)
	}

	// 2. Trigger Rollback back to "v1"
	rolledBack, err := sup.RollbackService(ctx, "tenant-r", "svc-rollout")
	if err != nil {
		t.Fatalf("RollbackService failed: %v", err)
	}
	if rolledBack.Spec.AgentVersion != "v1" {
		t.Fatalf("expected rolled back version v1, got %s", rolledBack.Spec.AgentVersion)
	}

	// Advance clock and reconcile to converge back to v1
	for step := 0; step < 10; step++ {
		curTime = curTime.Add(6 * time.Second)
		svcObj, _ := store.GetService(ctx, "tenant-r", "svc-rollout")
		_ = sup.ReconcileService(ctx, svcObj)

		current, _ := store.GetService(ctx, "tenant-r", "svc-rollout")
		if current.Status.UpdatedReplicas == 3 && current.Status.ActiveVersion == "v1" && current.Status.Phase == ServiceActive {
			break
		}
	}

	restoredSvc, _ := store.GetService(ctx, "tenant-r", "svc-rollout")
	if restoredSvc.Status.ActiveVersion != "v1" || restoredSvc.Status.UpdatedReplicas != 3 {
		t.Fatalf("expected restored version v1 with 3 updated replicas, got: %+v", restoredSvc.Status)
	}
}
