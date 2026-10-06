//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestModelRegistryPersistenceAndImmutability(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	registered, err := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: "tenant-a", Provider: "openai", ModelName: "gpt-4o", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(3), OutputPriceMicroUSDPerMillion: money.MustFromUSD(15), PriceRevision: "v1",
	})
	if err != nil {
		t.Fatalf("register descriptor: %v", err)
	}
	if registered.SpecHash == ([32]byte{}) {
		t.Fatal("descriptor spec hash was not computed")
	}
	retried, err := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: "tenant-a", Provider: "openai", ModelName: "gpt-4o", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(3), OutputPriceMicroUSDPerMillion: money.MustFromUSD(15), PriceRevision: "v1",
	})
	if err != nil || retried.ID != registered.ID {
		t.Fatalf("idempotent re-registration: %+v err=%v", retried, err)
	}
	if _, err := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: "tenant-a", Provider: "openai", ModelName: "gpt-4o", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(30), OutputPriceMicroUSDPerMillion: money.MustFromUSD(150), PriceRevision: "v1",
	}); !errors.Is(err, kernelstore.ErrModelSpecConflict) {
		t.Fatalf("mutation of a registered identity: %v, want ErrModelSpecConflict", err)
	}
	// A new price revision is a new identity for the same model.
	revised, err := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: "tenant-a", Provider: "openai", ModelName: "gpt-4o", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(3), OutputPriceMicroUSDPerMillion: money.MustFromUSD(15), PriceRevision: "v2",
	})
	if err != nil || revised.ID == registered.ID {
		t.Fatalf("price revision must be a new registration: %+v err=%v", revised, err)
	}
	resolved, err := repository.GetModelDescriptor(ctx, "tenant-a", "openai", "gpt-4o")
	if err != nil || resolved.Ref() != "openai/gpt-4o" || resolved.PriceRevision != "v2" {
		t.Fatalf("resolve: %+v err=%v", resolved, err)
	}
}

func TestModelCallLedgerIdempotencyAndStateMachine(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "model-call-ledger", 3)

	input := kernelstore.CreateModelCallInput{
		ID: uuid.New(), TenantID: "tenant-a", TaskID: assignment.Task.ID, RunID: assignment.Run.ID,
		AttemptID: assignment.Attempt.ID, ModelRef: "openai/gpt-4o", PriceRevision: "v1", IdempotencyKey: "model-1",
	}
	created, err := repository.CreateModelCall(ctx, input)
	if err != nil || created.Existing || created.ModelCall.Status != kernelstore.ModelCallStarted {
		t.Fatalf("create call: %+v err=%v", created, err)
	}
	retried, err := repository.CreateModelCall(ctx, input)
	if err != nil || !retried.Existing || retried.ModelCall.ID != created.ModelCall.ID {
		t.Fatalf("idempotent retry: %+v err=%v", retried, err)
	}
	finished, err := repository.FinishModelCall(ctx, kernelstore.FinishModelCallInput{
		TenantID: "tenant-a", ModelCallID: created.ModelCall.ID, ExpectedVersion: 1,
		Status: kernelstore.ModelCallCompleted, InputTokens: 100, OutputTokens: 50, CostMicroUSD: money.MustFromUSD(0.001),
		PriceRevision: "v1", ProviderRequestID: "req-1", FinishReason: "stop",
	})
	if err != nil || finished.Status != kernelstore.ModelCallCompleted || finished.ResourceVersion != 2 {
		t.Fatalf("finish: %+v err=%v", finished, err)
	}
	if _, err := repository.FinishModelCall(ctx, kernelstore.FinishModelCallInput{
		TenantID: "tenant-a", ModelCallID: created.ModelCall.ID, ExpectedVersion: 1,
		Status: kernelstore.ModelCallCompleted, InputTokens: 0, OutputTokens: 0, CostMicroUSD: 0, PriceRevision: "v1",
	}); !errors.Is(err, kernelstore.ErrVersionConflict) {
		t.Fatalf("stale CAS: %v, want ErrVersionConflict", err)
	}
	if _, err := repository.FinishModelCall(ctx, kernelstore.FinishModelCallInput{
		TenantID: "tenant-a", ModelCallID: created.ModelCall.ID, ExpectedVersion: 2,
		Status: kernelstore.ModelCallFailed, InputTokens: 0, OutputTokens: 0, CostMicroUSD: 0, PriceRevision: "v1",
	}); !errors.Is(err, kernelstore.ErrInvalidTransition) {
		t.Fatalf("terminal transition: %v, want ErrInvalidTransition", err)
	}
}

// TestModelGatewayHardStopOnTokenBudget drives the streaming lifecycle against
// a real task budget: incremental settlements, an idempotent replay, the hard
// stop when the token reservation is exhausted, and cost computed from the
// pinned price table.
func TestModelGatewayHardStopOnTokenBudget(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "model-hardstop", 3)

	if _, err := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: "tenant-a", Provider: "openai", ModelName: "gpt-4o", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(3), OutputPriceMicroUSDPerMillion: money.MustFromUSD(15), PriceRevision: "v1",
	}); err != nil {
		t.Fatalf("register descriptor: %v", err)
	}
	engine, err := policy.New(policy.TenantPolicies{"tenant-a": {
		MaxPriority: 100, AllowedModels: []string{"openai/gpt-4o"},
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	gateway := model.NewGateway(engine, repository, repository, repository)

	begun, err := gateway.Begin(ctx, model.BeginInput{
		TenantID: "tenant-a", TaskID: assignment.Task.ID, RunID: assignment.Run.ID,
		AttemptID: assignment.Attempt.ID, FencingToken: 1, AgentVersionRef: "agent@1",
		ModelRef: "openai/gpt-4o", IdempotencyKey: "hardstop-model-1",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// Budget is 500 tokens: 200 + 200 lands inside, the next 200 must stop.
	for sequence := int64(1); sequence <= 2; sequence++ {
		if err := gateway.Settle(ctx, begun.Call, sequence, model.Usage{OutputTokens: 200}); err != nil {
			t.Fatalf("settle %d: %v", sequence, err)
		}
	}
	if err := gateway.Settle(ctx, begun.Call, 2, model.Usage{OutputTokens: 200}); err != nil {
		t.Fatalf("idempotent replay of settled step: %v", err)
	}
	if err := gateway.Settle(ctx, begun.Call, 3, model.Usage{OutputTokens: 200}); !errors.Is(err, model.ErrBudgetExhausted) {
		t.Fatalf("over-budget settle: %v, want ErrBudgetExhausted", err)
	}

	status, err := repository.GetTaskBudget(ctx, "tenant-a", assignment.Task.ID)
	if err != nil {
		t.Fatalf("read budget: %v", err)
	}
	if status.Consumed.Tokens != 400 {
		t.Fatalf("consumed tokens = %d, want 400 (replay must not double-charge)", status.Consumed.Tokens)
	}
}

// TestModelGatewayFinishSettlesExactlyOnceAgainstRealLedger proves the exact
// accounting contract of the Model Gateway: a stream that settles steps and
// then finalizes with the cumulative total is charged exactly the total (never
// steps + total), and a replayed delta settlement for the same family
// converges without charging again (crash between the delta settlement and the
// call row update).
func TestModelGatewayFinishSettlesExactlyOnceAgainstRealLedger(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "model-exact-settle", 3)

	if _, err := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
		TenantID: "tenant-a", Provider: "openai", ModelName: "gpt-4o", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(3), OutputPriceMicroUSDPerMillion: money.MustFromUSD(15), PriceRevision: "v1",
	}); err != nil {
		t.Fatalf("register descriptor: %v", err)
	}
	engine, err := policy.New(policy.TenantPolicies{"tenant-a": {
		MaxPriority: 100, AllowedModels: []string{"openai/gpt-4o"},
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	gateway := model.NewGateway(engine, repository, repository, repository)

	begun, err := gateway.Begin(ctx, model.BeginInput{
		TenantID: "tenant-a", TaskID: assignment.Task.ID, RunID: assignment.Run.ID,
		AttemptID: assignment.Attempt.ID, FencingToken: 1, AgentVersionRef: "agent@1",
		ModelRef: "openai/gpt-4o", IdempotencyKey: "exact-model-1",
	})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	// Stream: 80 + 40 tokens settle as steps, then Finish reports the
	// cumulative total of 150 (10 input + 140 output).
	if err := gateway.Settle(ctx, begun.Call, 1, model.Usage{OutputTokens: 80}); err != nil {
		t.Fatalf("settle 1: %v", err)
	}
	if err := gateway.Settle(ctx, begun.Call, 2, model.Usage{OutputTokens: 40}); err != nil {
		t.Fatalf("settle 2: %v", err)
	}
	if _, err := gateway.Finish(ctx, begun.Call, model.FinishInput{
		TenantID: "tenant-a", ModelCallID: begun.Call.ID, ExpectedVersion: begun.Call.ResourceVersion,
		Status: kernelstore.ModelCallCompleted, InputTokens: 10, OutputTokens: 140, FinishReason: "stream-end",
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}
	status, err := repository.GetTaskBudget(ctx, "tenant-a", assignment.Task.ID)
	if err != nil {
		t.Fatalf("read budget: %v", err)
	}
	if status.Consumed.Tokens != 150 {
		t.Fatalf("consumed tokens = %d, want exactly 150 (steps + finish must not double-charge)", status.Consumed.Tokens)
	}
	var settlementCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM task_budget_settlements
		WHERE tenant_id = $1 AND task_id = $2 AND idempotency_key LIKE $3`,
		"tenant-a", assignment.Task.ID.String(), "model:"+begun.Call.ID.String()+":%").Scan(&settlementCount); err != nil {
		t.Fatalf("count family settlements: %v", err)
	}
	if settlementCount != 3 {
		t.Fatalf("family settlements = %d, want 3 (seq 1, seq 2, finish delta)", settlementCount)
	}

	// Crash replay: the caller never saw the Finish response, so it retries
	// the delta settlement for the same family and target. The family already
	// reached the target: the delta is zero and nothing more is charged.
	replayed, err := repository.SettleTaskUsageDelta(ctx, kernelstore.SettleTaskUsageDeltaInput{
		TenantID: "tenant-a", TaskID: assignment.Task.ID,
		FamilyPrefix: "model:" + begun.Call.ID.String(), Target: kernelstore.TaskBudget{Tokens: 150},
		IdempotencyKey: "model:" + begun.Call.ID.String() + ":finish",
	})
	if err != nil {
		t.Fatalf("delta replay: %v", err)
	}
	if replayed.Consumed.Tokens != 150 {
		t.Fatalf("consumed after delta replay = %d, want 150", replayed.Consumed.Tokens)
	}
}
