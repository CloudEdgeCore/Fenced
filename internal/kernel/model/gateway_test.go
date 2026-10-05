package model

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestBeginRejectsModelOutsidePolicy(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{DenyReasons: []string{"MODEL_NOT_ALLOWED"}}
	_, err := gateway.Begin(context.Background(), beginInput("openai/gpt-4o"))
	if !errors.Is(err, ErrModelDenied) {
		t.Fatalf("Begin: %v, want ErrModelDenied", err)
	}
	if fakes.models.calls != 0 {
		t.Fatalf("denied model must not open a call")
	}
}

func TestBeginRejectsExhaustedBudgetBeforeStream(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{Allow: true}
	fakes.budget.exhausted = true
	_, err := gateway.Begin(context.Background(), beginInput("openai/gpt-4o"))
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("Begin: %v, want ErrBudgetExhausted", err)
	}
	if fakes.models.calls != 0 {
		t.Fatalf("hard stop must precede opening a call")
	}
}

func TestBeginCapsOmittedOutputLimitToRemainingBudget(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{Allow: true}
	fakes.budget.reservedTokens = 700
	fakes.budget.settledTokens = 200
	input := beginInput("openai/gpt-4o")
	input.EstimatedInputTokens = 100

	begun, err := gateway.Begin(context.Background(), input)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if begun.EffectiveMaxOutputTokens != 400 {
		t.Fatalf("effective max output = %d, want remaining 400", begun.EffectiveMaxOutputTokens)
	}
	if fakes.budget.lastReserved.Tokens != 500 {
		t.Fatalf("reservation = %d, want input 100 + output 400", fakes.budget.lastReserved.Tokens)
	}
}

func TestStreamSettlesIncrementallyAndHardStops(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{Allow: true}
	fakes.budget.reservedTokens = 100

	begun, err := gateway.Begin(context.Background(), beginInput("openai/gpt-4o"))
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if begun.Call.Status != kernelstore.ModelCallStarted {
		t.Fatalf("call status = %s", begun.Call.Status)
	}

	if err := gateway.Settle(context.Background(), begun.Call, 1, Usage{InputTokens: 40}); err != nil {
		t.Fatalf("settle 1: %v", err)
	}
	if err := gateway.Settle(context.Background(), begun.Call, 2, Usage{OutputTokens: 40}); err != nil {
		t.Fatalf("settle 2: %v", err)
	}
	// Idempotent replay of a settled step must not double-charge.
	if err := gateway.Settle(context.Background(), begun.Call, 2, Usage{OutputTokens: 40}); err != nil {
		t.Fatalf("settle replay: %v", err)
	}
	if fakes.budget.settledTokens != 80 {
		t.Fatalf("settled tokens = %d, want 80", fakes.budget.settledTokens)
	}

	// The next step pushes past the reservation: hard stop.
	if err := gateway.Settle(context.Background(), begun.Call, 3, Usage{OutputTokens: 30}); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("over-budget settle: %v, want ErrBudgetExhausted", err)
	}
	if !fakes.budget.exhausted {
		t.Fatal("ledger must be marked exhausted")
	}
}

func TestFinishSettlesOnlyTheRemainderAfterSteps(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{Allow: true}
	fakes.budget.reservedTokens = 1000
	begun, err := gateway.Begin(context.Background(), beginInput("openai/gpt-4o"))
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	// A streaming caller settles steps incrementally (120 tokens), then
	// finalizes with the cumulative total (150): the ledger must be charged
	// exactly 150, not 120 + 150.
	if err := gateway.Settle(context.Background(), begun.Call, 1, Usage{OutputTokens: 80}); err != nil {
		t.Fatalf("settle 1: %v", err)
	}
	if err := gateway.Settle(context.Background(), begun.Call, 2, Usage{OutputTokens: 40}); err != nil {
		t.Fatalf("settle 2: %v", err)
	}
	if _, err := gateway.Finish(context.Background(), begun.Call, FinishInput{
		TenantID: "tenant-a", ModelCallID: begun.Call.ID, ExpectedVersion: 1,
		Status: kernelstore.ModelCallCompleted, InputTokens: 10, OutputTokens: 140,
		FinishReason: "stream-end",
	}); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if fakes.budget.settledTokens != 150 {
		t.Fatalf("settled tokens = %d, want 150 (steps + finish must not double-charge)", fakes.budget.settledTokens)
	}
}

func TestFinishComputesCostFromPriceTable(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{Allow: true}
	begun, err := gateway.Begin(context.Background(), beginInput("openai/gpt-4o"))
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	finished, err := gateway.Finish(context.Background(), begun.Call, FinishInput{
		TenantID: "tenant-a", ModelCallID: begun.Call.ID, ExpectedVersion: 1,
		Status: kernelstore.ModelCallCompleted, InputTokens: 1000, OutputTokens: 2000,
		ProviderRequestID: "req-1", FinishReason: "stop",
	})
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	// prices: 3 USD / 1M input, 15 USD / 1M output
	wantCost := money.MustFromUSD(0.001*3 + 0.002*15)
	if finished.CostMicroUSD != wantCost {
		t.Fatalf("cost = %v, want %v", finished.CostMicroUSD, wantCost)
	}
	if finished.Status != kernelstore.ModelCallCompleted || finished.FinishReason != "stop" {
		t.Fatalf("finished call: %+v", finished)
	}
	if len(fakes.receipts.written) != 1 || !json.Valid(fakes.receipts.written[0].Response) {
		t.Fatalf("receipt not written: %+v", fakes.receipts.written)
	}
}

func TestFinishOverBudgetStopsCall(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{Allow: true}
	fakes.budget.reservedTokens = 50
	begun, err := gateway.Begin(context.Background(), beginInput("openai/gpt-4o"))
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	_, err = gateway.Finish(context.Background(), begun.Call, FinishInput{
		TenantID: "tenant-a", ModelCallID: begun.Call.ID, ExpectedVersion: 1,
		Status: kernelstore.ModelCallCompleted, InputTokens: 100,
	})
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("over-budget finish: %v, want ErrBudgetExhausted", err)
	}
	call := fakes.models.callsByID[begun.Call.ID]
	if call.Status != kernelstore.ModelCallStopped {
		t.Fatalf("call status = %s, want STOPPED", call.Status)
	}
}

func TestBeginRejectsMalformedInput(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["openai/gpt-4o"] = policy.Decision{Allow: true}
	input := beginInput("openai/gpt-4o")
	input.ModelRef = "not-a-ref"
	if _, err := gateway.Begin(context.Background(), input); err == nil {
		t.Fatal("malformed model ref accepted")
	}
	if _, err := gateway.Begin(context.Background(), beginInput("openai/gpt-4o")); err != nil {
		t.Fatalf("valid begin: %v", err)
	}
}

// --- test doubles ---

type testFakes struct {
	policy   *fakeModelPolicy
	models   *fakeModelStore
	budget   *fakeModelBudget
	receipts *fakeModelReceipts
}

func newTestGateway() (*Gateway, *testFakes) {
	fakes := &testFakes{
		policy:   &fakeModelPolicy{decisions: map[string]policy.Decision{}},
		models:   newFakeModelStore(),
		budget:   &fakeModelBudget{},
		receipts: &fakeModelReceipts{},
	}
	gateway := NewGateway(fakes.policy, fakes.models, fakes.budget, fakes.receipts)
	return gateway, fakes
}

func beginInput(modelRef string) BeginInput {
	return BeginInput{
		TenantID: "tenant-a", TaskID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		RunID:        uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		AttemptID:    uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		FencingToken: 1, AgentVersionRef: "agent@1", ModelRef: modelRef, IdempotencyKey: "model-1",
	}
}

type fakeModelPolicy struct {
	decisions map[string]policy.Decision
}

func (f *fakeModelPolicy) EvaluateModel(_ context.Context, _ string, input policy.ModelContext) policy.Decision {
	if decision, ok := f.decisions[input.Name]; ok {
		return decision
	}
	return policy.Decision{}
}

type fakeModelStore struct {
	descriptors map[string]kernelstore.ModelDescriptor
	callsByID   map[uuid.UUID]kernelstore.ModelCall
	calls       int
}

func newFakeModelStore() *fakeModelStore {
	return &fakeModelStore{
		descriptors: map[string]kernelstore.ModelDescriptor{
			"openai/gpt-4o": {ID: uuid.New(), TenantID: "tenant-a", Provider: "openai", ModelName: "gpt-4o",
				SupportsStreaming: true, InputPriceMicroUSDPerMillion: money.MustFromUSD(3), OutputPriceMicroUSDPerMillion: money.MustFromUSD(15), PriceRevision: "v1"},
		},
		callsByID: map[uuid.UUID]kernelstore.ModelCall{},
	}
}

func (f *fakeModelStore) GetModelDescriptor(_ context.Context, tenantID, provider, modelName string) (kernelstore.ModelDescriptor, error) {
	descriptor, ok := f.descriptors[provider+"/"+modelName]
	if !ok || descriptor.TenantID != tenantID {
		return kernelstore.ModelDescriptor{}, kernelstore.ErrModelNotFound
	}
	return descriptor, nil
}

func (f *fakeModelStore) CreateModelCall(_ context.Context, in kernelstore.CreateModelCallInput) (kernelstore.CreateModelCallResult, error) {
	for _, call := range f.callsByID {
		if call.AttemptID == in.AttemptID && call.ModelRef == in.ModelRef && call.IdempotencyKey == in.IdempotencyKey {
			return kernelstore.CreateModelCallResult{ModelCall: call, Existing: true}, nil
		}
	}
	hash, err := in.RequestHash()
	if err != nil {
		return kernelstore.CreateModelCallResult{}, err
	}
	call := kernelstore.ModelCall{ID: in.ID, TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID,
		AttemptID: in.AttemptID, ModelRef: in.ModelRef, Status: kernelstore.ModelCallStarted,
		PriceRevision: in.PriceRevision, IdempotencyKey: in.IdempotencyKey, RequestHash: hash,
		ResourceVersion: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	f.callsByID[call.ID] = call
	f.calls++
	return kernelstore.CreateModelCallResult{ModelCall: call}, nil
}

func (f *fakeModelStore) GetModelCall(_ context.Context, tenantID string, id uuid.UUID) (kernelstore.ModelCall, error) {
	call, ok := f.callsByID[id]
	if !ok || call.TenantID != tenantID {
		return kernelstore.ModelCall{}, kernelstore.ErrNotFound
	}
	return call, nil
}

func (f *fakeModelStore) FinishModelCall(_ context.Context, in kernelstore.FinishModelCallInput) (kernelstore.ModelCall, error) {
	call, ok := f.callsByID[in.ModelCallID]
	if !ok || call.TenantID != in.TenantID {
		return kernelstore.ModelCall{}, kernelstore.ErrNotFound
	}
	if call.ResourceVersion != in.ExpectedVersion {
		return kernelstore.ModelCall{}, kernelstore.ErrVersionConflict
	}
	if !kernelstore.CanTransitionModelCall(call.Status, in.Status) {
		return kernelstore.ModelCall{}, kernelstore.ErrInvalidTransition
	}
	call.Status = in.Status
	call.InputTokens, call.OutputTokens, call.CostMicroUSD = in.InputTokens, in.OutputTokens, in.CostMicroUSD
	call.PriceRevision = in.PriceRevision
	call.ProviderRequestID, call.FinishReason = in.ProviderRequestID, in.FinishReason
	call.UsageCertainty = in.UsageCertainty
	call.ResourceVersion++
	call.UpdatedAt = time.Now()
	f.callsByID[call.ID] = call
	return call, nil
}

type fakeModelBudget struct {
	exhausted      bool
	reservedTokens int64
	settledTokens  int64
	settled        map[string]int64
	lastReserved   kernelstore.TaskBudget
}

func (f *fakeModelBudget) ReserveTaskUsage(_ context.Context, in kernelstore.ReserveTaskUsageInput) error {
	if f.exhausted {
		return kernelstore.ErrBudgetExceeded
	}
	f.lastReserved = in.Amount
	return nil
}

func (f *fakeModelBudget) ReleaseTaskUsageReservation(context.Context, string, uuid.UUID, string) error {
	return nil
}

func (f *fakeModelBudget) GetTaskBudget(context.Context, string, uuid.UUID) (kernelstore.TaskBudgetStatus, error) {
	return kernelstore.TaskBudgetStatus{
		Reserved:  kernelstore.TaskBudget{Tokens: f.reservedTokens},
		Consumed:  kernelstore.TaskBudget{Tokens: f.settledTokens},
		Exhausted: f.exhausted,
	}, nil
}

func (f *fakeModelBudget) SettleTaskUsage(_ context.Context, in kernelstore.SettleTaskUsageInput) (kernelstore.TaskBudgetStatus, error) {
	if f.settled == nil {
		f.settled = map[string]int64{}
	}
	if _, replay := f.settled[in.IdempotencyKey]; replay {
		return kernelstore.TaskBudgetStatus{Reserved: kernelstore.TaskBudget{Tokens: f.reservedTokens},
			Consumed: kernelstore.TaskBudget{Tokens: f.settledTokens}}, nil
	}
	if f.exhausted || (f.reservedTokens > 0 && f.settledTokens+in.Usage.Tokens > f.reservedTokens) {
		f.exhausted = true
		return kernelstore.TaskBudgetStatus{}, kernelstore.ErrBudgetExceeded
	}
	f.settled[in.IdempotencyKey] = in.Usage.Tokens
	f.settledTokens += in.Usage.Tokens
	return kernelstore.TaskBudgetStatus{Reserved: kernelstore.TaskBudget{Tokens: f.reservedTokens},
		Consumed: kernelstore.TaskBudget{Tokens: f.settledTokens}}, nil
}

// SettleTaskUsageDelta mirrors the real ledger semantics: only the remainder
// between the family's settled usage and the target is charged, exactly once
// per idempotency key.
func (f *fakeModelBudget) SettleTaskUsageDelta(_ context.Context, in kernelstore.SettleTaskUsageDeltaInput) (kernelstore.TaskBudgetStatus, error) {
	if f.settled == nil {
		f.settled = map[string]int64{}
	}
	var family int64
	for key, tokens := range f.settled {
		if strings.HasPrefix(key, in.FamilyPrefix+":") {
			family += tokens
		}
	}
	delta := in.Target.Tokens - family
	if delta <= 0 {
		return kernelstore.TaskBudgetStatus{Reserved: kernelstore.TaskBudget{Tokens: f.reservedTokens},
			Consumed: kernelstore.TaskBudget{Tokens: f.settledTokens}}, nil
	}
	if _, replay := f.settled[in.IdempotencyKey]; replay {
		return kernelstore.TaskBudgetStatus{Reserved: kernelstore.TaskBudget{Tokens: f.reservedTokens},
			Consumed: kernelstore.TaskBudget{Tokens: f.settledTokens}}, nil
	}
	if f.exhausted || (f.reservedTokens > 0 && f.settledTokens+delta > f.reservedTokens) {
		f.exhausted = true
		return kernelstore.TaskBudgetStatus{}, kernelstore.ErrBudgetExceeded
	}
	f.settled[in.IdempotencyKey] = delta
	f.settledTokens += delta
	return kernelstore.TaskBudgetStatus{Reserved: kernelstore.TaskBudget{Tokens: f.reservedTokens},
		Consumed: kernelstore.TaskBudget{Tokens: f.settledTokens}}, nil
}

type fakeModelReceipts struct {
	written []kernelstore.WriteRuntimeReceiptInput
}

func (f *fakeModelReceipts) WriteRuntimeReceipt(_ context.Context, in kernelstore.WriteRuntimeReceiptInput) error {
	f.written = append(f.written, in)
	return nil
}
