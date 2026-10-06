package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestInvokeAllowedToolExecutesAndWritesReceipt(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{Allow: true}

	result, err := gateway.InvokeTool(context.Background(), invokeInput("read", `{"path":"a.txt"}`))
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED", result.Outcome)
	}
	if !json.Valid(result.Result) {
		t.Fatalf("result is not valid JSON: %s", result.Result)
	}
	if fakes.executor.calls != 1 || len(fakes.receipts.written) != 1 {
		t.Fatalf("executions=%d receipts=%d, want 1/1", fakes.executor.calls, len(fakes.receipts.written))
	}
	call := fakes.tools.callsByID[result.ToolCall.ID]
	if call.Status != store.ToolCallExecuted {
		t.Fatalf("call status = %s, want EXECUTED", call.Status)
	}
	if fakes.budget.settlements != 1 {
		t.Fatalf("budget settlements = %d, want 1", fakes.budget.settlements)
	}
	if result.ReceiptOperation != "TOOL:fs.read@1.0.0" {
		t.Fatalf("receipt operation = %s", result.ReceiptOperation)
	}
}

func TestInvokeDeniedToolRecordsDenialAndNeverExecutes(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{DenyReasons: []string{"TOOL_NOT_ALLOWED"}}

	result, err := gateway.InvokeTool(context.Background(), invokeInput("read", `{"path":"a.txt"}`))
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	if result.Outcome != OutcomeDenied || !slices.Contains(result.DenyReasons, "TOOL_NOT_ALLOWED") {
		t.Fatalf("outcome = %s reasons=%v", result.Outcome, result.DenyReasons)
	}
	if fakes.executor.calls != 0 || len(fakes.receipts.written) != 0 {
		t.Fatalf("denied invocation executed or wrote receipts")
	}
	call := fakes.tools.callsByID[result.ToolCall.ID]
	if call.Status != store.ToolCallDenied {
		t.Fatalf("call status = %s, want DENIED", call.Status)
	}
}

func TestInvokeRequiresApprovalThenExecutesAfterDecision(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["write"] = policy.Decision{Allow: true, RequiresApproval: true}
	input := invokeInput("write", `{"path":"a.txt","content":"x"}`)

	pending, err := gateway.InvokeTool(context.Background(), input)
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	if pending.Outcome != OutcomeRequiresApproval || pending.ApprovalID == nil {
		t.Fatalf("outcome = %s approval=%v, want REQUIRES_APPROVAL with id", pending.Outcome, pending.ApprovalID)
	}
	if fakes.executor.calls != 0 || fakes.budget.settlements != 0 {
		t.Fatalf("approval gate must precede execution and budget settlement")
	}

	approval := fakes.tools.approvalsByID[*pending.ApprovalID]
	decided, err := fakes.tools.DecideToolApproval(context.Background(), store.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: approval.ID, ExpectedVersion: approval.ResourceVersion,
		Decision: store.ToolApprovalApproved, DecidedBy: "human-1", Now: time.Now(),
	})
	if err != nil || decided.Status != store.ToolApprovalApproved {
		t.Fatalf("decide approval: %v status=%s", err, decided.Status)
	}

	input.ApprovalID = &decided.ID
	approved, err := gateway.InvokeTool(context.Background(), input)
	if err != nil {
		t.Fatalf("re-invoke with approval: %v", err)
	}
	if approved.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED", approved.Outcome)
	}
	if fakes.executor.calls != 1 {
		t.Fatalf("executions = %d, want 1", fakes.executor.calls)
	}
}

func TestInvokeRejectsRejectedApproval(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["write"] = policy.Decision{Allow: true, RequiresApproval: true}
	input := invokeInput("write", `{"path":"a.txt"}`)

	pending, err := gateway.InvokeTool(context.Background(), input)
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	approval := fakes.tools.approvalsByID[*pending.ApprovalID]
	if _, err := fakes.tools.DecideToolApproval(context.Background(), store.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: approval.ID, ExpectedVersion: approval.ResourceVersion,
		Decision: store.ToolApprovalRejected, DecidedBy: "human-1", Now: time.Now(),
	}); err != nil {
		t.Fatalf("decide approval: %v", err)
	}
	input.ApprovalID = &approval.ID
	if _, err := gateway.InvokeTool(context.Background(), input); !errors.Is(err, store.ErrApprovalNotUsable) {
		t.Fatalf("re-invoke with rejected approval: %v, want ErrApprovalNotUsable", err)
	}
	if fakes.executor.calls != 0 {
		t.Fatalf("rejected approval must never execute")
	}
}

func TestInvokeApprovalBindingMismatchIsNotUsable(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["write"] = policy.Decision{Allow: true, RequiresApproval: true}
	input := invokeInput("write", `{"path":"a.txt"}`)

	pending, err := gateway.InvokeTool(context.Background(), input)
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	approval := fakes.tools.approvalsByID[*pending.ApprovalID]
	if _, err := fakes.tools.DecideToolApproval(context.Background(), store.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: approval.ID, ExpectedVersion: approval.ResourceVersion,
		Decision: store.ToolApprovalApproved, DecidedBy: "human-1", Now: time.Now(),
	}); err != nil {
		t.Fatalf("decide approval: %v", err)
	}

	stolen := invokeInput("write", `{"path":"a.txt"}`)
	stolen.Resource = "fs:/etc"        // different target resource
	stolen.IdempotencyKey = "invoke-2" // a genuinely different invocation
	stolen.ApprovalID = &approval.ID
	if _, err := gateway.InvokeTool(context.Background(), stolen); !errors.Is(err, store.ErrApprovalNotUsable) {
		t.Fatalf("approval reused for another resource: %v, want ErrApprovalNotUsable", err)
	}
	if fakes.executor.calls != 0 {
		t.Fatalf("mismatched approval must never execute")
	}
}

func TestInvokeStopsOnExhaustedBudget(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{Allow: true}
	fakes.budget.exhausted = true

	_, err := gateway.InvokeTool(context.Background(), invokeInput("read", `{"path":"a.txt"}`))
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("InvokeTool: %v, want ErrBudgetExhausted", err)
	}
	if fakes.executor.calls != 0 {
		t.Fatalf("hard stop must precede execution")
	}
	call := fakes.tools.callsByID[uuid.MustParse(fakes.tools.lastCallID)]
	if call.Status != store.ToolCallFailed || !slices.Contains(call.DecisionReasons, "BUDGET_EXHAUSTED") {
		t.Fatalf("call = %s %v, want FAILED with BUDGET_EXHAUSTED", call.Status, call.DecisionReasons)
	}
}

func TestInvokeReplaysFromReceiptWithoutReexecution(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{Allow: true}
	input := invokeInput("read", `{"path":"a.txt"}`)

	first, err := gateway.InvokeTool(context.Background(), input)
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	second, err := gateway.InvokeTool(context.Background(), input)
	if err != nil {
		t.Fatalf("replay invoke: %v", err)
	}
	if second.Outcome != OutcomeReplayed || string(second.Result) != string(first.Result) {
		t.Fatalf("replay outcome = %s result=%s", second.Outcome, second.Result)
	}
	if fakes.executor.calls != 1 {
		t.Fatalf("executions = %d, want 1 (receipt must prevent re-execution)", fakes.executor.calls)
	}
}

func TestInvokeIdempotencyKeyReuseWithDifferentArgsConflicts(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{Allow: true}
	input := invokeInput("read", `{"path":"a.txt"}`)

	if _, err := gateway.InvokeTool(context.Background(), input); err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	input.Args = json.RawMessage(`{"path":"b.txt"}`)
	if _, err := gateway.InvokeTool(context.Background(), input); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("reused key with different args: %v, want ErrIdempotencyConflict", err)
	}
}

func TestInvokeRejectsInvalidArgs(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{Allow: true}
	if _, err := gateway.InvokeTool(context.Background(), invokeInput("read", `{"path":123}`)); !errors.Is(err, ErrToolArgsInvalid) {
		t.Fatalf("invalid args: %v, want ErrToolArgsInvalid", err)
	}
	if fakes.executor.calls != 0 {
		t.Fatalf("invalid args must never execute")
	}
}

func TestInvokeRedactsSecretHandleFromResult(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{Allow: true}
	fakes.executor.output = json.RawMessage(`{"content":"token sk-secret-abc"}`)
	fakes.secrets.handle = SecretHandle("sk-secret-abc")

	input := invokeInput("read", `{"path":"a.txt"}`)
	input.SecretRef = "filesystem/read"
	result, err := gateway.InvokeTool(context.Background(), input)
	if err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	if bytes.Contains(result.Result, []byte("sk-secret-abc")) {
		t.Fatalf("secret handle leaked through result: %s", result.Result)
	}
	if fakes.secrets.calls != 1 || fakes.secrets.scope.SecretRef != "filesystem/read" {
		t.Fatalf("explicit secret scope was not brokered: %+v", fakes.secrets.scope)
	}
}

func TestInvokeDoesNotIssueImplicitSecret(t *testing.T) {
	gateway, fakes := newTestGateway()
	fakes.policy.decisions["read"] = policy.Decision{Allow: true}
	if _, err := gateway.InvokeTool(context.Background(), invokeInput("read", `{"path":"a.txt"}`)); err != nil {
		t.Fatalf("InvokeTool: %v", err)
	}
	if fakes.secrets.calls != 0 {
		t.Fatal("tool without an explicit secret grant reached the Secret Broker")
	}
}

// --- test doubles ---

type testFakes struct {
	policy   *fakePolicyChecker
	tools    *fakeToolStore
	budget   *fakeBudgetStore
	receipts *fakeReceiptStore
	executor *fakeExecutor
	secrets  *fakeSecretBroker
}

func newTestGateway() (*Gateway, *testFakes) {
	fakes := &testFakes{
		policy:   &fakePolicyChecker{decisions: map[string]policy.Decision{}},
		tools:    newFakeToolStore(),
		budget:   &fakeBudgetStore{},
		receipts: &fakeReceiptStore{},
		executor: &fakeExecutor{output: json.RawMessage(`{"ok":true}`)},
		secrets:  &fakeSecretBroker{handle: "handle-1"},
	}
	gateway := NewGateway(fakes.policy, fakes.tools, fakes.budget, fakes.receipts, fakes.executor, fakes.secrets,
		WithClock(func() time.Time { return time.Unix(1_800_000_000, 0).UTC() }, func() uuid.UUID { return uuid.MustParse("00000000-0000-0000-0000-000000000001") }))
	return gateway, fakes
}

func invokeInput(action, args string) InvokeInput {
	return InvokeInput{
		TenantID: "tenant-a", TaskID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		RunID:           uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		AttemptID:       uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		AgentVersionRef: "agent@1", ToolName: "fs.read", Action: action,
		Resource: "fs:/tmp", Args: json.RawMessage(args), IdempotencyKey: "invoke-1",
	}
}

type fakePolicyChecker struct {
	decisions map[string]policy.Decision
}

func (f *fakePolicyChecker) EvaluateTool(_ context.Context, _ string, input policy.ToolContext) policy.Decision {
	if decision, ok := f.decisions[input.Action]; ok {
		return decision
	}
	return policy.Decision{}
}

var paramsSchema = json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path"]}`)

type fakeToolStore struct {
	descriptors   map[string]store.ToolDescriptor
	callsByID     map[uuid.UUID]store.ToolCall
	approvalsByID map[uuid.UUID]store.ToolApproval
	lastCallID    string
}

func newFakeToolStore() *fakeToolStore {
	return &fakeToolStore{
		descriptors: map[string]store.ToolDescriptor{
			"fs.read@1.0.0": {ID: uuid.New(), TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0",
				SideEffectRisk: store.ToolRiskLow, Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
				ParamsSchema: paramsSchema},
			"fs.write@1.0.0": {ID: uuid.New(), TenantID: "tenant-a", Name: "fs.write", Version: "1.0.0",
				SideEffectRisk: store.ToolRiskHigh, Actions: []string{"write"}, ResourcePatterns: []string{"fs:/tmp"},
				ParamsSchema: paramsSchema},
		},
		callsByID:     map[uuid.UUID]store.ToolCall{},
		approvalsByID: map[uuid.UUID]store.ToolApproval{},
	}
}

func (f *fakeToolStore) descriptor(tenantID, name, version string) (store.ToolDescriptor, error) {
	if version == "" {
		version = f.latestVersion(name)
	}
	descriptor, ok := f.descriptors[name+"@"+version]
	if !ok || descriptor.TenantID != tenantID {
		return store.ToolDescriptor{}, store.ErrToolNotFound
	}
	return descriptor, nil
}

func (f *fakeToolStore) latestVersion(name string) string {
	var latest string
	for _, descriptor := range f.descriptors {
		if descriptor.Name == name && descriptor.Version > latest {
			latest = descriptor.Version
		}
	}
	return latest
}

func (f *fakeToolStore) RegisterToolDescriptor(_ context.Context, in store.RegisterToolDescriptorInput) (store.ToolDescriptor, error) {
	return store.ToolDescriptor{}, errors.New("not used")
}
func (f *fakeToolStore) GetToolDescriptor(_ context.Context, tenantID, name, version string) (store.ToolDescriptor, error) {
	return f.descriptor(tenantID, name, version)
}
func (f *fakeToolStore) ListToolDescriptors(_ context.Context, tenantID string) ([]store.ToolDescriptor, error) {
	var descriptors []store.ToolDescriptor
	for _, descriptor := range f.descriptors {
		if descriptor.TenantID == tenantID {
			descriptors = append(descriptors, descriptor)
		}
	}
	return descriptors, nil
}
func (f *fakeToolStore) CreateToolCall(_ context.Context, in store.CreateToolCallInput) (store.CreateToolCallResult, error) {
	for _, call := range f.callsByID {
		if call.AttemptID == in.AttemptID && call.ToolName == in.ToolName && call.IdempotencyKey == in.IdempotencyKey {
			hash, err := in.RequestHash()
			if err != nil {
				return store.CreateToolCallResult{}, err
			}
			if call.RequestHash != hash {
				return store.CreateToolCallResult{}, store.ErrIdempotencyConflict
			}
			return store.CreateToolCallResult{ToolCall: call, Existing: true}, nil
		}
	}
	hash, err := in.RequestHash()
	if err != nil {
		return store.CreateToolCallResult{}, err
	}
	call := store.ToolCall{ID: in.ID, TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID, AttemptID: in.AttemptID,
		ToolName: in.ToolName, ToolVersion: in.ToolVersion, Action: in.Action, Resource: in.Resource,
		ArgsHash: in.ArgsHash, Status: store.ToolCallPending, IdempotencyKey: in.IdempotencyKey,
		RequestHash: hash, ResourceVersion: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	f.callsByID[call.ID] = call
	f.lastCallID = call.ID.String()
	return store.CreateToolCallResult{ToolCall: call}, nil
}
func (f *fakeToolStore) GetToolCall(_ context.Context, tenantID string, id uuid.UUID) (store.ToolCall, error) {
	call, ok := f.callsByID[id]
	if !ok || call.TenantID != tenantID {
		return store.ToolCall{}, store.ErrNotFound
	}
	return call, nil
}
func (f *fakeToolStore) UpdateToolCall(_ context.Context, in store.UpdateToolCallInput) (store.ToolCall, error) {
	call, ok := f.callsByID[in.ToolCallID]
	if !ok || call.TenantID != in.TenantID {
		return store.ToolCall{}, store.ErrNotFound
	}
	if call.ResourceVersion != in.ExpectedVersion {
		return store.ToolCall{}, store.ErrVersionConflict
	}
	if !store.CanTransitionTo(call.Status, in.Status) {
		return store.ToolCall{}, fmt.Errorf("%w: %s -> %s", store.ErrInvalidTransition, call.Status, in.Status)
	}
	call.Status = in.Status
	call.DecisionReasons = slices.Clone(in.DecisionReasons)
	call.PolicyRevision = in.PolicyRevision
	call.ApprovalID = in.ApprovalID
	call.ResourceVersion++
	call.UpdatedAt = time.Now()
	f.callsByID[call.ID] = call
	return call, nil
}
func (f *fakeToolStore) CreateToolApproval(_ context.Context, in store.CreateToolApprovalInput) (store.CreateToolApprovalResult, error) {
	for _, approval := range f.approvalsByID {
		if approval.AttemptID == in.AttemptID && approval.ToolName == in.ToolName && approval.ToolVersion == in.ToolVersion &&
			approval.Action == in.Action && approval.Resource == in.Resource && approval.ArgsHash == in.ArgsHash {
			return store.CreateToolApprovalResult{ToolApproval: approval, Existing: true}, nil
		}
	}
	approval := store.ToolApproval{ID: in.ID, TenantID: in.TenantID, CallID: in.CallID, TaskID: in.TaskID,
		RunID: in.RunID, AttemptID: in.AttemptID, ToolName: in.ToolName, ToolVersion: in.ToolVersion,
		Action: in.Action, Resource: in.Resource, ArgsHash: in.ArgsHash, Status: store.ToolApprovalPending,
		RequestedAt: in.RequestedAt, ExpiresAt: in.ExpiresAt, ResourceVersion: 1}
	f.approvalsByID[approval.ID] = approval
	return store.CreateToolApprovalResult{ToolApproval: approval}, nil
}
func (f *fakeToolStore) GetToolApproval(_ context.Context, tenantID string, id uuid.UUID) (store.ToolApproval, error) {
	approval, ok := f.approvalsByID[id]
	if !ok || approval.TenantID != tenantID {
		return store.ToolApproval{}, store.ErrNotFound
	}
	return approval, nil
}
func (f *fakeToolStore) DecideToolApproval(_ context.Context, in store.DecideToolApprovalInput) (store.ToolApproval, error) {
	if !in.Valid() {
		return store.ToolApproval{}, fmt.Errorf("invalid decision input")
	}
	approval, ok := f.approvalsByID[in.ApprovalID]
	if !ok || approval.TenantID != in.TenantID {
		return store.ToolApproval{}, store.ErrNotFound
	}
	if approval.ResourceVersion != in.ExpectedVersion {
		return store.ToolApproval{}, store.ErrVersionConflict
	}
	if approval.Status != store.ToolApprovalPending {
		return store.ToolApproval{}, fmt.Errorf("%w: not pending", store.ErrInvalidTransition)
	}
	if !in.Now.Before(approval.ExpiresAt) {
		approval.Status = store.ToolApprovalExpired
		approval.ResourceVersion++
		f.approvalsByID[approval.ID] = approval
		return store.ToolApproval{}, store.ErrApprovalNotUsable
	}
	approval.Status = in.Decision
	approval.DecidedAt = &in.Now
	approval.DecidedBy = in.DecidedBy
	approval.ResourceVersion++
	f.approvalsByID[approval.ID] = approval
	return approval, nil
}

type fakeBudgetStore struct {
	exhausted   bool
	settlements int
}

func (f *fakeBudgetStore) ReserveTaskUsage(context.Context, store.ReserveTaskUsageInput) error {
	return nil
}

func (f *fakeBudgetStore) ReleaseTaskUsageReservation(context.Context, string, uuid.UUID, string) error {
	return nil
}

func (f *fakeBudgetStore) GetTaskBudget(context.Context, string, uuid.UUID) (store.TaskBudgetStatus, error) {
	return store.TaskBudgetStatus{Exhausted: f.exhausted}, nil
}
func (f *fakeBudgetStore) SettleTaskUsage(context.Context, store.SettleTaskUsageInput) (store.TaskBudgetStatus, error) {
	if f.exhausted {
		return store.TaskBudgetStatus{}, store.ErrBudgetExceeded
	}
	f.settlements++
	return store.TaskBudgetStatus{}, nil
}
func (f *fakeBudgetStore) SettleTaskUsageDelta(context.Context, store.SettleTaskUsageDeltaInput) (store.TaskBudgetStatus, error) {
	if f.exhausted {
		return store.TaskBudgetStatus{}, store.ErrBudgetExceeded
	}
	f.settlements++
	return store.TaskBudgetStatus{}, nil
}

type fakeReceiptStore struct {
	written []store.WriteRuntimeReceiptInput
}

func (f *fakeReceiptStore) GetRuntimeReceipt(_ context.Context, tenantID string, attemptID uuid.UUID, operation, key string) (store.RuntimeReceipt, error) {
	for _, receipt := range f.written {
		if receipt.TenantID == tenantID && receipt.AttemptID == attemptID && receipt.Operation == operation && receipt.IdempotencyKey == key {
			return store.RuntimeReceipt{RequestHash: receipt.RequestHash, Response: receipt.Response}, nil
		}
	}
	return store.RuntimeReceipt{}, store.ErrNotFound
}
func (f *fakeReceiptStore) WriteRuntimeReceipt(_ context.Context, in store.WriteRuntimeReceiptInput) error {
	f.written = append(f.written, in)
	return nil
}

type fakeExecutor struct {
	calls       int
	output      json.RawMessage
	failureCode string
}

func (f *fakeExecutor) Execute(_ context.Context, in ExecutionRequest) (ExecutionResult, error) {
	f.calls++
	return ExecutionResult{Output: f.output, FailureCode: f.failureCode}, nil
}

type fakeSecretBroker struct {
	handle SecretHandle
	calls  int
	scope  SecretScope
}

func (f *fakeSecretBroker) Issue(_ context.Context, scope SecretScope) (SecretHandle, error) {
	f.calls++
	f.scope = scope
	return f.handle, nil
}
