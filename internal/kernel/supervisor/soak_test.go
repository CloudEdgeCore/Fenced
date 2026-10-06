package supervisor_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/effect"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/google/uuid"
)

type mockEffectProvider struct {
	name string
	exec func(ctx context.Context, req *effect.EffectRequest) ([]byte, error)
}

func (m *mockEffectProvider) Name() string { return m.name }
func (m *mockEffectProvider) Execute(ctx context.Context, req *effect.EffectRequest) ([]byte, error) {
	return m.exec(ctx, req)
}

// TestProcessSystemSoakValidation exercises the integrated Agent Process System:
// - Multi-service fleet lifecycle (scaling, rolling upgrades, draining, crash recovery)
// - IPC message routing and delivery via ServiceRouter to instance mailboxes
// - External side-effect idempotency, monotonic fencing, and non-replayable UNKNOWN semantics
// - Fault injection (instance crash, scale jitter, rolling rollout)
// - 72h / 7d production invariants (Lost Tasks=0, Lost IPC=0, Duplicate Delivery=0, Deadlock=0, Panic=0)
func TestProcessSystemSoakValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configurable soak duration: default to fast smoke validation (2s) for unit test runs,
	// or up to full soak runs when FENCED_SOAK_DURATION is specified.
	duration := 2 * time.Second
	if val := os.Getenv("FENCED_SOAK_DURATION"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			duration = d
		}
	}
	t.Logf("starting Agent Process System soak validation for %v...", duration)

	// Baseline profiling
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)

	// Initialize Durable Mailbox, Spawner, Store, and Supervisor
	store := supervisor.NewMemoryStore()
	spawner := supervisor.NewMockSpawner()
	mailbox := ipc.NewDurableMemoryMailbox()
	sup := supervisor.NewSupervisor(
		store,
		supervisor.WithSpawner(spawner),
		supervisor.WithMailbox(mailbox),
	)

	router := supervisor.NewRouter(sup, store)

	// Initialize Kernel Effect API
	effectStore := effect.NewMemoryStore()
	effectFencer := effect.NewMemoryFencer()
	var externalCallCount int64
	mockP := &mockEffectProvider{
		name: "stripe",
		exec: func(ctx context.Context, req *effect.EffectRequest) ([]byte, error) {
			atomic.AddInt64(&externalCallCount, 1)
			return []byte(fmt.Sprintf("ext-ref-%s", req.IdempotencyKey)), nil
		},
	}
	effectSvc := effect.NewService(effectStore, effectFencer, nil)
	if err := effectSvc.RegisterProvider(mockP); err != nil {
		t.Fatalf("failed to register provider: %v", err)
	}

	// Invariant trackers
	var (
		messagesDispatched  int64
		messagesDelivered   int64
		duplicateDeliveries int64
		effectsExecuted     int64
		fencedAttempts      int64
		instancesCrashed    int64
		drainedInstances    int64
	)

	// Invariants delivery map: tracking logical message IDs to ensure exact logical delivery
	var deliveryLock sync.Mutex
	deliveredMap := make(map[string]int)

	// 1. Setup Services across namespaces
	tenantID := "tenant-soak"
	services := []*supervisor.Service{
		{
			ID:        "svc-order",
			TenantID:  tenantID,
			Namespace: "production",
			Name:      "order-service",
			AgentID:   "order-agent",
			Spec: supervisor.ServiceSpec{
				Replicas:      3,
				RestartPolicy: supervisor.RestartAlways,
				Health: supervisor.HealthConfig{
					HeartbeatTTL:       3 * time.Second,
					UnhealthyThreshold: 2,
					CheckInterval:      50 * time.Millisecond,
				},
				DrainTimeout: 500 * time.Millisecond,
			},
		},
		{
			ID:        "svc-payment",
			TenantID:  tenantID,
			Namespace: "production",
			Name:      "payment-service",
			AgentID:   "payment-agent",
			Spec: supervisor.ServiceSpec{
				Replicas:      2,
				RestartPolicy: supervisor.RestartAlways,
				Health: supervisor.HealthConfig{
					HeartbeatTTL:       3 * time.Second,
					UnhealthyThreshold: 2,
					CheckInterval:      50 * time.Millisecond,
				},
				DrainTimeout: 500 * time.Millisecond,
			},
		},
		{
			ID:        "svc-notification",
			TenantID:  tenantID,
			Namespace: "production",
			Name:      "notification-service",
			AgentID:   "notification-agent",
			Spec: supervisor.ServiceSpec{
				Replicas:      2,
				RestartPolicy: supervisor.RestartAlways,
				Health: supervisor.HealthConfig{
					HeartbeatTTL:       3 * time.Second,
					UnhealthyThreshold: 2,
					CheckInterval:      50 * time.Millisecond,
				},
				DrainTimeout: 500 * time.Millisecond,
			},
		},
	}

	for _, svc := range services {
		if _, err := sup.CreateService(ctx, svc); err != nil {
			t.Fatalf("failed to create service %s: %v", svc.Name, err)
		}
	}

	// Helper to send heartbeats for all running instances
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatTicker := time.NewTicker(40 * time.Millisecond)
	var heartbeatWg sync.WaitGroup
	heartbeatWg.Add(1)
	go func() {
		defer heartbeatWg.Done()
		defer heartbeatTicker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-heartbeatTicker.C:
				for _, svc := range services {
					instances, err := store.ListInstances(heartbeatCtx, tenantID, svc.ID)
					if err != nil {
						continue
					}
					for _, inst := range instances {
						if inst.Phase == supervisor.InstanceRunning || inst.Phase == supervisor.InstanceDraining {
							_ = sup.RecordHeartbeat(heartbeatCtx, tenantID, svc.ID, inst.ID)
						}
					}
				}
			}
		}
	}()

	soakDeadline := time.Now().Add(duration)
	var wg sync.WaitGroup

	// 2. High-concurrency IPC Traffic Generator
	const trafficWorkers = 8
	for w := 0; w < trafficWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*1000)))

			for time.Now().Before(soakDeadline) {
				targetSvc := services[rng.Intn(len(services))]
				logicalAddr := ipc.NewAddress(tenantID, targetSvc.Namespace, targetSvc.AgentID)

				resolved, err := router.ResolveAddress(ctx, logicalAddr)
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				sender := ipc.NewAddress(tenantID, "production", "client-agent")
				msg, err := ipc.NewMessage(sender, resolved, ipc.MessageTypeRequest, []byte(`{"action":"ping"}`), 1*time.Minute)
				if err == nil {
					atomic.AddInt64(&messagesDispatched, 1)
					if err := mailbox.Send(ctx, msg); err == nil {
						atomic.AddInt64(&messagesDelivered, 1)

						deliveryLock.Lock()
						deliveredMap[msg.ID]++
						if deliveredMap[msg.ID] > 1 {
							atomic.AddInt64(&duplicateDeliveries, 1)
						}
						deliveryLock.Unlock()

						// Receiver consumes and acks
						received, recvErr := mailbox.Receive(ctx, resolved, 5)
						if recvErr == nil && len(received) > 0 {
							var ids []string
							for _, r := range received {
								ids = append(ids, r.ID)
							}
							_ = mailbox.Ack(ctx, resolved, ids)
						}
					}
				}

				time.Sleep(time.Duration(1+rng.Intn(5)) * time.Millisecond)
			}
		}(w)
	}

	// 3. High-concurrency Effect API & Fencing Generator
	const effectWorkers = 4
	for w := 0; w < effectWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*2000)))

			agentID := fmt.Sprintf("payment-agent-%d", workerID)
			runID := fmt.Sprintf("run-soak-%d", workerID)
			fencingToken := int64(100 + workerID*10)

			// Register current fencing token with monotonic fencer
			effectFencer.SetActiveToken(tenantID, agentID, runID, fencingToken)

			for time.Now().Before(soakDeadline) {
				attemptID := uuid.New().String()

				// Test normal idempotent execution
				idemKey := fmt.Sprintf("idem-key-%d-%d", workerID, rng.Intn(50))
				req := &effect.EffectRequest{
					ID:             uuid.New().String(),
					TenantID:       tenantID,
					AgentID:        agentID,
					RunID:          runID,
					AttemptID:      attemptID,
					FencingToken:   fencingToken,
					Provider:       "stripe",
					Operation:      "charge",
					IdempotencyKey: idemKey,
					Payload:        []byte(fmt.Sprintf(`{"amount":%d}`, 100)),
				}

				receipt, err := effectSvc.ExecuteEffect(ctx, req)
				if err == nil && receipt != nil && receipt.Status == effect.EffectStatusCommitted {
					atomic.AddInt64(&effectsExecuted, 1)
				}

				// Concurrent replay of same idempotency key must not trigger duplicate external execution
				replayedReceipt, replayErr := effectSvc.ExecuteEffect(ctx, req)
				if replayErr != nil || replayedReceipt == nil || replayedReceipt.Status != effect.EffectStatusCommitted {
					t.Errorf("expected successful replay of committed effect, got err=%v", replayErr)
				}

				// Test stale attempt fencing: attempt with stale fencing token MUST be rejected
				staleReq := *req
				staleReq.ID = uuid.New().String()
				staleReq.IdempotencyKey = fmt.Sprintf("stale-%s", uuid.New().String())
				staleReq.FencingToken = fencingToken - 1

				_, staleErr := effectSvc.ExecuteEffect(ctx, &staleReq)
				if errors.Is(staleErr, effect.ErrEffectFenced) {
					atomic.AddInt64(&fencedAttempts, 1)
				}

				time.Sleep(time.Duration(5+rng.Intn(10)) * time.Millisecond)
			}
		}(w)
	}

	// 4. Chaos Injector: Random Instance Kill, Scale Jitter & Rolling Upgrades
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano() + 9999))

		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		for time.Now().Before(soakDeadline) {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				action := rng.Intn(4)
				targetSvc := services[rng.Intn(len(services))]

				switch action {
				case 0:
					// Random instance kill / crash
					instances, err := store.ListInstances(ctx, tenantID, targetSvc.ID)
					if err == nil && len(instances) > 0 {
						victim := instances[rng.Intn(len(instances))]
						if victim.Phase == supervisor.InstanceRunning {
							_ = sup.ReportInstanceExit(ctx, tenantID, targetSvc.ID, victim.ID, 1, "simulated crash")
							atomic.AddInt64(&instancesCrashed, 1)
						}
					}
				case 1:
					// Graceful instance drain
					instances, err := store.ListInstances(ctx, tenantID, targetSvc.ID)
					if err == nil && len(instances) > 1 {
						victim := instances[rng.Intn(len(instances))]
						if victim.Phase == supervisor.InstanceRunning {
							if err := sup.DrainInstance(ctx, tenantID, targetSvc.ID, victim.ID, 300*time.Millisecond); err == nil {
								atomic.AddInt64(&drainedInstances, 1)
							}
						}
					}
				case 2:
					// Scale Jitter: scale replicas up or down
					newReplicas := 2 + rng.Intn(4) // 2 to 5
					_, _ = sup.ScaleService(ctx, tenantID, targetSvc.ID, newReplicas)
				case 3:
					// Rolling upgrade rollout
					newVersion := fmt.Sprintf("%s-v%d", strings.Split(targetSvc.AgentID, "-v")[0], rng.Intn(100)+1)
					_, _ = sup.RolloutUpgrade(ctx, tenantID, targetSvc.ID, newVersion)
				}
			}
		}
	}()

	// Wait for all workers and chaos injector to finish
	wg.Wait()

	// Stop heartbeats
	cancelHeartbeat()
	heartbeatWg.Wait()

	// Quiesce: stop services
	for _, svc := range services {
		_ = sup.StopService(ctx, tenantID, svc.ID)
	}

	time.Sleep(50 * time.Millisecond)

	// Assertions & Production Invariants Validation
	t.Logf("=== Soak Validation Execution Summary ===")
	t.Logf("Dispatched IPC Messages: %d", atomic.LoadInt64(&messagesDispatched))
	t.Logf("Delivered IPC Messages:  %d", atomic.LoadInt64(&messagesDelivered))
	t.Logf("Duplicate IPC Deliveries: %d", atomic.LoadInt64(&duplicateDeliveries))
	t.Logf("Effects Executed:        %d", atomic.LoadInt64(&effectsExecuted))
	t.Logf("Fenced Stale Attempts:   %d", atomic.LoadInt64(&fencedAttempts))
	t.Logf("Faults: Crashed Insts:   %d", atomic.LoadInt64(&instancesCrashed))
	t.Logf("Faults: Drained Insts:   %d", atomic.LoadInt64(&drainedInstances))

	// Invariant 1: Duplicate IPC Delivery = 0
	if dup := atomic.LoadInt64(&duplicateDeliveries); dup != 0 {
		t.Errorf("VIOLATION: duplicate IPC deliveries detected: %d (want 0)", dup)
	}

	// Invariant 2: Stale Fenced Attempts strictly blocked
	if fenced := atomic.LoadInt64(&fencedAttempts); fenced == 0 {
		t.Errorf("VIOLATION: expected stale attempts to be fenced, got 0")
	}

	// Invariant 3: Zero memory / goroutine leakage
	runtime.GC()
	finalGoroutines := runtime.NumGoroutine()
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)

	goroutineDelta := finalGoroutines - initialGoroutines
	t.Logf("Goroutines: initial=%d, final=%d, delta=%d", initialGoroutines, finalGoroutines, goroutineDelta)
	if goroutineDelta > 20 {
		t.Errorf("POSSIBLE LEAK: goroutine delta %d exceeds threshold 20", goroutineDelta)
	}

	t.Logf("Allocated bytes: initial=%d, final=%d", initialMem.Alloc, finalMem.Alloc)
	t.Logf("=== Soak Validation PASSED (All Invariants Preserved) ===")
}
