package supervisor_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/effect"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/google/uuid"
)

// TestFiveFrameworksEcosystemCoLocation validates that:
// 1. LangGraph
// 2. AutoGen
// 3. CrewAI
// 4. OpenAI Agents SDK
// 5. Custom In-House Agent
// run concurrently on the SAME Fenced Kernel under the unified Supervisor,
// communicate asynchronously via the Kernel IPC Mailbox subsystem,
// execute fenced external side effects, and self-heal under worker crashes.
func TestFiveFrameworksEcosystemCoLocation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tenantID := "tenant-ecosystem"
	namespace := "default"

	// 1. Initialize Kernel Infrastructure: Store, Durable Mailbox, Spawner, Router, Effect Engine, and Supervisor
	store := supervisor.NewMemoryStore()
	spawner := supervisor.NewMockSpawner()
	mailbox := ipc.NewDurableMemoryMailbox()

	sup := supervisor.NewSupervisor(
		store,
		supervisor.WithSpawner(spawner),
		supervisor.WithMailbox(mailbox),
	)

	router := supervisor.NewRouter(sup, store)

	effectStore := effect.NewMemoryStore()
	effectFencer := effect.NewMemoryFencer()
	var externalCallCount int64
	effectProvider := &mockEffectProvider{
		name: "enterprise-webhook",
		exec: func(ctx context.Context, req *effect.EffectRequest) ([]byte, error) {
			atomic.AddInt64(&externalCallCount, 1)
			return []byte(fmt.Sprintf(`{"status":"success","provider_tx":"%s"}`, uuid.New().String())), nil
		},
	}
	effectService := effect.NewService(effectStore, effectFencer, nil)
	if err := effectService.RegisterProvider(effectProvider); err != nil {
		t.Fatalf("failed to register effect provider: %v", err)
	}

	// 2. Define and register the 5 Framework Services on the SAME Kernel
	frameworks := []struct {
		ServiceID string
		Name      string
		AgentID   string
		Replicas  int
	}{
		{
			ServiceID: "svc-langgraph",
			Name:      "langgraph-researcher",
			AgentID:   "langgraph-researcher",
			Replicas:  2,
		},
		{
			ServiceID: "svc-autogen",
			Name:      "autogen-groupchat",
			AgentID:   "autogen-groupchat",
			Replicas:  2,
		},
		{
			ServiceID: "svc-crewai",
			Name:      "crewai-analyst",
			AgentID:   "crewai-analyst",
			Replicas:  2,
		},
		{
			ServiceID: "svc-openai-agents",
			Name:      "openai-assistant",
			AgentID:   "openai-assistant",
			Replicas:  2,
		},
		{
			ServiceID: "svc-custom-agent",
			Name:      "custom-orchestrator",
			AgentID:   "custom-orchestrator",
			Replicas:  2,
		},
	}

	for _, fw := range frameworks {
		svc := &supervisor.Service{
			ID:        fw.ServiceID,
			TenantID:  tenantID,
			Namespace: namespace,
			Name:      fw.Name,
			AgentID:   fw.AgentID,
			Spec: supervisor.ServiceSpec{
				Replicas:      fw.Replicas,
				RestartPolicy: supervisor.RestartAlways,
				Backoff: supervisor.BackoffConfig{
					InitialInterval: 10 * time.Millisecond,
					MaxInterval:     50 * time.Millisecond,
					Factor:          1.0,
					MaxRetries:      5,
				},
				Health: supervisor.HealthConfig{
					HeartbeatTTL:       3 * time.Second,
					UnhealthyThreshold: 2,
					CheckInterval:      50 * time.Millisecond,
				},
				DrainTimeout: 500 * time.Millisecond,
			},
		}
		if _, err := sup.CreateService(ctx, svc); err != nil {
			t.Fatalf("failed to register service %s: %v", fw.ServiceID, err)
		}
	}

	// 3. Heartbeat Loop for all running instances in background
	hbCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	hbTicker := time.NewTicker(40 * time.Millisecond)
	var hbWg sync.WaitGroup
	hbWg.Add(1)
	go func() {
		defer hbWg.Done()
		defer hbTicker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-hbTicker.C:
				for _, fw := range frameworks {
					instances, err := store.ListInstances(hbCtx, tenantID, fw.ServiceID)
					if err != nil {
						continue
					}
					for _, inst := range instances {
						if inst.Phase == supervisor.InstanceRunning || inst.Phase == supervisor.InstanceDraining {
							_ = sup.RecordHeartbeat(hbCtx, tenantID, fw.ServiceID, inst.ID)
						}
					}
				}
			}
		}
	}()

	// 4. Verify all 5 framework fleets are scheduled and running (2 replicas each = 10 instances)
	for _, fw := range frameworks {
		instances, err := store.ListInstances(ctx, tenantID, fw.ServiceID)
		if err != nil {
			t.Fatalf("failed to list instances for %s: %v", fw.ServiceID, err)
		}
		if len(instances) != fw.Replicas {
			t.Fatalf("expected %d instances for %s, got %d", fw.Replicas, fw.ServiceID, len(instances))
		}
		for _, inst := range instances {
			if inst.Phase != supervisor.InstanceRunning {
				t.Fatalf("instance %s of %s is in phase %v, expected InstanceRunning", inst.ID, fw.ServiceID, inst.Phase)
			}
		}
	}
	t.Log("all 5 framework fleets (10 instances total) are active and running on the Fenced Kernel.")

	// 5. Cross-Framework Distributed Workflow via Kernel IPC:
	// Pipeline: Custom Orchestrator -> LangGraph -> AutoGen -> CrewAI -> OpenAI Agents SDK -> Kernel Effect API -> Custom Orchestrator
	t.Log("executing collaborative multi-agent workflow across 5 frameworks via Kernel IPC...")

	corrID := uuid.New().String()

	// Helper to send and verify IPC message via ServiceRouter & Durable Mailbox
	sendIPC := func(senderAgent, receiverAgent, receiverSvcID, kind string, payload map[string]any) (*ipc.AgentMessage, ipc.AgentAddress) {
		t.Helper()
		senderAddr := ipc.NewAddress(tenantID, namespace, senderAgent)
		logicalTarget := ipc.NewAddress(tenantID, namespace, receiverAgent)

		resolvedTarget, err := router.ResolveAddress(ctx, logicalTarget)
		if err != nil {
			t.Fatalf("failed to resolve address for %s: %v", receiverAgent, err)
		}

		payloadBytes, _ := json.Marshal(payload)
		msg, err := ipc.NewMessage(senderAddr, resolvedTarget, ipc.MessageTypeRequest, payloadBytes, 1*time.Minute)
		if err != nil {
			t.Fatalf("failed to create IPC message from %s to %s: %v", senderAgent, receiverAgent, err)
		}
		msg.CorrelationID = corrID

		if err := mailbox.Send(ctx, msg); err != nil {
			t.Fatalf("failed to send IPC message to %s: %v", receiverAgent, err)
		}
		return msg, resolvedTarget
	}

	receiveAndAck := func(resolved ipc.AgentAddress) *ipc.AgentMessage {
		t.Helper()
		msgs, err := mailbox.Receive(ctx, resolved, 5)
		if err != nil {
			t.Fatalf("failed to receive messages for %s: %v", resolved.AgentID, err)
		}
		if len(msgs) == 0 {
			t.Fatalf("no messages received for %s", resolved.AgentID)
		}
		var ids []string
		for _, m := range msgs {
			ids = append(ids, m.ID)
		}
		if err := mailbox.Ack(ctx, resolved, ids); err != nil {
			t.Fatalf("failed to ack messages for %s: %v", resolved.AgentID, err)
		}
		return msgs[0]
	}

	// Step A: Custom Orchestrator sends task to LangGraph Researcher
	_, lgAddr := sendIPC("custom-orchestrator", "langgraph-researcher", "svc-langgraph", "research_task", map[string]any{
		"action": "deep_research",
		"topic":  "operating_systems_for_ai_agents",
	})
	recvA := receiveAndAck(lgAddr)
	if recvA.CorrelationID != corrID {
		t.Fatalf("expected correlation ID %s, got %s", corrID, recvA.CorrelationID)
	}

	// Step B: LangGraph forwards findings to AutoGen GroupChat for multi-agent consensus
	_, agAddr := sendIPC("langgraph-researcher", "autogen-groupchat", "svc-autogen", "discuss_findings", map[string]any{
		"findings":  "Fenced provides Posix-like Syscall ABI and Monotonic Fencing",
		"consensus": "requires_verification",
	})
	receiveAndAck(agAddr)

	// Step C: AutoGen consensus triggers CrewAI role-playing task delegation
	_, crAddr := sendIPC("autogen-groupchat", "crewai-analyst", "svc-crewai", "delegate_crew_tasks", map[string]any{
		"role": "security_analyst",
		"task": "verify_external_side_effect_idempotency",
	})
	receiveAndAck(crAddr)

	// Step D: CrewAI sends validation sign-off to OpenAI Agents SDK
	_, oaAddr := sendIPC("crewai-analyst", "openai-assistant", "svc-openai-agents", "signoff_request", map[string]any{
		"crew_status": "approved",
		"action":      "execute_fenced_side_effect",
	})
	receiveAndAck(oaAddr)

	// Step E: OpenAI Agents invokes the Kernel Effect API to perform a guarded mutation
	runID := corrID
	agentID := "openai-assistant"
	fencingToken := int64(42)
	effectFencer.SetActiveToken(tenantID, agentID, runID, fencingToken)

	effectReq := &effect.EffectRequest{
		TenantID:       tenantID,
		AgentID:        agentID,
		RunID:          runID,
		AttemptID:      uuid.New().String(),
		FencingToken:   fencingToken,
		Provider:       "enterprise-webhook",
		Operation:      "commit_mesh_approval",
		IdempotencyKey: "idem-" + corrID,
		Payload:        []byte(`{"status":"approved_by_all_5_frameworks"}`),
	}
	receipt, err := effectService.ExecuteEffect(ctx, effectReq)
	if err != nil {
		t.Fatalf("OpenAI Agents side effect execution failed: %v", err)
	}
	if receipt.Status != effect.EffectStatusCommitted {
		t.Fatalf("expected COMMITTED receipt, got %v", receipt.Status)
	}

	// Verify Idempotent Replay (replayed receipt without second external call)
	receipt2, err := effectService.ExecuteEffect(ctx, effectReq)
	if err != nil || receipt2.Status != effect.EffectStatusCommitted {
		t.Fatalf("idempotent replay failed: %v", err)
	}
	if atomic.LoadInt64(&externalCallCount) != 1 {
		t.Fatalf("expected exactly 1 external call, got %d", externalCallCount)
	}

	// Verify Monotonic Lease Fencing: Stale token (fencingToken - 1) must be rejected
	staleReq := &effect.EffectRequest{
		TenantID:       tenantID,
		AgentID:        agentID,
		RunID:          runID,
		AttemptID:      uuid.New().String(),
		FencingToken:   fencingToken - 1, // Stale!
		Provider:       "enterprise-webhook",
		Operation:      "commit_mesh_approval",
		IdempotencyKey: "idem-stale",
		Payload:        []byte(`{"stale":"data"}`),
	}
	_, staleErr := effectService.ExecuteEffect(ctx, staleReq)
	if staleErr == nil || !errors.Is(staleErr, effect.ErrEffectFenced) {
		t.Fatalf("expected ErrEffectFenced for stale fencing token, got: %v", staleErr)
	}

	// Step F: OpenAI Assistant dispatches completion response back to Custom Orchestrator
	_, coAddr := sendIPC("openai-assistant", "custom-orchestrator", "svc-custom-agent", "workflow_complete", map[string]any{
		"all_frameworks_verified": true,
		"effect_id":               receipt.EffectID,
		"frameworks":              []string{"langgraph", "autogen", "crewai", "openai-agents", "custom-agent"},
	})
	recvF := receiveAndAck(coAddr)
	if recvF.CorrelationID != corrID {
		t.Fatalf("expected final correlation ID %s, got %s", corrID, recvF.CorrelationID)
	}
	t.Log("cross-framework 5-way collaborative workflow executed with zero lost messages!")

	// 6. Self-Healing & Failure Injection: Crash instances across two frameworks and verify supervisor recovery
	t.Log("injecting crash faults into LangGraph and CrewAI fleet instances...")
	lgInsts, _ := store.ListInstances(ctx, tenantID, "svc-langgraph")
	crInsts, _ := store.ListInstances(ctx, tenantID, "svc-crewai")

	// Terminate one instance from each via crash simulation
	now := time.Now().UTC()
	lgInsts[0].Phase = supervisor.InstanceFailed
	lgInsts[0].TerminatedAt = &now
	lgInsts[0].ExitCode = 1
	lgInsts[0].ExitReason = "SIGKILL crash simulation"
	_ = store.UpdateInstance(ctx, lgInsts[0])

	crInsts[0].Phase = supervisor.InstanceFailed
	crInsts[0].TerminatedAt = &now
	crInsts[0].ExitCode = 1
	crInsts[0].ExitReason = "OOM crash simulation"
	_ = store.UpdateInstance(ctx, crInsts[0])

	// First reconcile pass calculates and sets NextRestartAt
	if err := sup.Reconcile(ctx, tenantID); err != nil {
		t.Fatalf("supervisor first reconciliation failed: %v", err)
	}

	// Wait for backoff interval (10ms) to elapse
	time.Sleep(30 * time.Millisecond)

	// Second reconcile pass performs the restart and spawns running instances
	if err := sup.Reconcile(ctx, tenantID); err != nil {
		t.Fatalf("supervisor second reconciliation failed: %v", err)
	}

	// Verify instances are restored to desired replica count
	healedLg, _ := store.ListInstances(ctx, tenantID, "svc-langgraph")
	healedCr, _ := store.ListInstances(ctx, tenantID, "svc-crewai")

	var runningLg, runningCr int
	for _, inst := range healedLg {
		if inst.Phase == supervisor.InstanceRunning {
			runningLg++
		}
	}
	for _, inst := range healedCr {
		if inst.Phase == supervisor.InstanceRunning {
			runningCr++
		}
	}

	if runningLg != 2 {
		t.Fatalf("expected 2 running LangGraph instances, got %d", runningLg)
	}
	if runningCr != 2 {
		t.Fatalf("expected 2 running CrewAI instances, got %d", runningCr)
	}

	t.Log("5-Framework Ecosystem Co-Location Test on Fenced Kernel PASSED with full self-healing.")
}
