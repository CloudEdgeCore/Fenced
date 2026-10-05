package security

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
)

// --- tool gateway fakes (negative scenarios) ---

type gatewayFakes struct {
	policy   *fakePolicy
	tools    *fakeTools
	budget   *fakeBudget
	receipts *fakeReceipts
	executor tool.ToolExecutor
	secrets  *fakeSecrets
}

func newGatewayFakes() *gatewayFakes {
	return &gatewayFakes{
		policy:   &fakePolicy{decisions: map[string]policy.Decision{}},
		tools:    newFakeTools(),
		budget:   &fakeBudget{},
		receipts: &fakeReceipts{},
		executor: &countingExecutor{},
		secrets:  &fakeSecrets{handle: "dev-handle"},
	}
}

func newSecurityGateway(fakes *gatewayFakes) *tool.Gateway {
	return tool.NewGateway(fakes.policy, fakes.tools, fakes.budget, fakes.receipts, fakes.executor, fakes.secrets,
		tool.WithClock(func() time.Time { return time.Unix(1_800_000_000, 0).UTC() },
			func() uuid.UUID { return uuid.MustParse("00000000-0000-0000-0000-000000000001") }))
}

func invoke(action, args, idempotencyKey string) tool.InvokeInput {
	return tool.InvokeInput{
		TenantID: "tenant-a", TaskID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		RunID:           uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		AttemptID:       uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		AgentVersionRef: "agent@1", ToolName: "fs.write", Action: action,
		Resource: "fs:/tmp", Args: json.RawMessage(args), IdempotencyKey: idempotencyKey,
	}
}

func invokeWithApproval(action, args, idempotencyKey string, approvalID uuid.UUID) tool.InvokeInput {
	input := invoke(action, args, idempotencyKey)
	input.ApprovalID = &approvalID
	return input
}

type fakePolicy struct {
	decisions map[string]policy.Decision
}

func (f *fakePolicy) EvaluateTool(_ context.Context, _ string, input policy.ToolContext) policy.Decision {
	if decision, ok := f.decisions[input.Action]; ok {
		return decision
	}
	return policy.Decision{Allow: true}
}

type fakeTools struct {
	descriptors map[string]store.ToolDescriptor
	calls       map[uuid.UUID]store.ToolCall
	approvals   map[uuid.UUID]store.ToolApproval
}

func newFakeTools() *fakeTools {
	return &fakeTools{
		descriptors: map[string]store.ToolDescriptor{
			"fs.write@1.0.0": {ID: uuid.New(), TenantID: "tenant-a", Name: "fs.write", Version: "1.0.0",
				SideEffectRisk: store.ToolRiskHigh, Actions: []string{"write"}, ResourcePatterns: []string{"fs:/tmp"},
				ParamsSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`)},
		},
		calls:     map[uuid.UUID]store.ToolCall{},
		approvals: map[uuid.UUID]store.ToolApproval{},
	}
}

func (f *fakeTools) RegisterToolDescriptor(context.Context, store.RegisterToolDescriptorInput) (store.ToolDescriptor, error) {
	return store.ToolDescriptor{}, errors.New("not used")
}

func (f *fakeTools) GetToolDescriptor(_ context.Context, tenantID, name, version string) (store.ToolDescriptor, error) {
	if version == "" {
		for _, descriptor := range f.descriptors {
			if descriptor.Name == name && descriptor.TenantID == tenantID {
				version = descriptor.Version
				break
			}
		}
	}
	descriptor, ok := f.descriptors[name+"@"+version]
	if !ok || descriptor.TenantID != tenantID {
		return store.ToolDescriptor{}, store.ErrToolNotFound
	}
	return descriptor, nil
}

func (f *fakeTools) ListToolDescriptors(_ context.Context, tenantID string) ([]store.ToolDescriptor, error) {
	var result []store.ToolDescriptor
	for _, descriptor := range f.descriptors {
		if descriptor.TenantID == tenantID {
			result = append(result, descriptor)
		}
	}
	return result, nil
}

func (f *fakeTools) CreateToolCall(_ context.Context, in store.CreateToolCallInput) (store.CreateToolCallResult, error) {
	hash, err := in.RequestHash()
	if err != nil {
		return store.CreateToolCallResult{}, err
	}
	for _, call := range f.calls {
		if call.AttemptID == in.AttemptID && call.ToolName == in.ToolName && call.IdempotencyKey == in.IdempotencyKey {
			if call.RequestHash != hash {
				return store.CreateToolCallResult{}, store.ErrIdempotencyConflict
			}
			return store.CreateToolCallResult{ToolCall: call, Existing: true}, nil
		}
	}
	call := store.ToolCall{
		ID: in.ID, TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID, AttemptID: in.AttemptID,
		ToolName: in.ToolName, ToolVersion: in.ToolVersion, Action: in.Action, Resource: in.Resource,
		ArgsHash: hash, IdempotencyKey: in.IdempotencyKey, Status: store.ToolCallPending,
		ResourceVersion: 1, CreatedAt: time.Unix(1_800_000_000, 0).UTC(), UpdatedAt: time.Unix(1_800_000_000, 0).UTC(),
	}
	f.calls[call.ID] = call
	return store.CreateToolCallResult{ToolCall: call}, nil
}

func (f *fakeTools) GetToolCall(_ context.Context, _ string, id uuid.UUID) (store.ToolCall, error) {
	call, ok := f.calls[id]
	if !ok {
		return store.ToolCall{}, store.ErrNotFound
	}
	return call, nil
}

func (f *fakeTools) UpdateToolCall(_ context.Context, in store.UpdateToolCallInput) (store.ToolCall, error) {
	call, ok := f.calls[in.ToolCallID]
	if !ok {
		return store.ToolCall{}, store.ErrNotFound
	}
	if call.ResourceVersion != in.ExpectedVersion {
		return store.ToolCall{}, store.ErrVersionConflict
	}
	if !store.CanTransitionTo(call.Status, in.Status) {
		return store.ToolCall{}, errors.New("invalid tool call transition")
	}
	call.Status, call.ResourceVersion, call.ApprovalID = in.Status, call.ResourceVersion+1, in.ApprovalID
	call.UpdatedAt = time.Unix(1_800_000_000, 0).UTC()
	f.calls[call.ID] = call
	return call, nil
}

func (f *fakeTools) CreateToolApproval(_ context.Context, in store.CreateToolApprovalInput) (store.CreateToolApprovalResult, error) {
	approval := store.ToolApproval{
		ID: in.ID, TenantID: in.TenantID, CallID: in.CallID, TaskID: in.TaskID, RunID: in.RunID,
		AttemptID: in.AttemptID, ToolName: in.ToolName, ToolVersion: in.ToolVersion,
		Action: in.Action, Resource: in.Resource, ArgsHash: in.ArgsHash,
		Status: store.ToolApprovalPending, ResourceVersion: 1,
		RequestedAt: in.RequestedAt, ExpiresAt: in.ExpiresAt,
	}
	f.approvals[approval.ID] = approval
	return store.CreateToolApprovalResult{ToolApproval: approval}, nil
}

func (f *fakeTools) GetToolApproval(_ context.Context, tenantID string, id uuid.UUID) (store.ToolApproval, error) {
	approval, ok := f.approvals[id]
	if !ok || approval.TenantID != tenantID {
		return store.ToolApproval{}, store.ErrNotFound
	}
	return approval, nil
}

func (f *fakeTools) DecideToolApproval(_ context.Context, in store.DecideToolApprovalInput) (store.ToolApproval, error) {
	if !in.Valid() {
		return store.ToolApproval{}, errors.New("invalid decision")
	}
	approval, ok := f.approvals[in.ApprovalID]
	if !ok || approval.TenantID != in.TenantID {
		return store.ToolApproval{}, store.ErrNotFound
	}
	approval.Status, approval.DecidedBy, approval.DecidedAt = in.Decision, in.DecidedBy, &in.Now
	f.approvals[approval.ID] = approval
	return approval, nil
}

type fakeBudget struct{}

func (f *fakeBudget) ReserveTaskUsage(context.Context, store.ReserveTaskUsageInput) error {
	return nil
}

func (f *fakeBudget) ReleaseTaskUsageReservation(context.Context, string, uuid.UUID, string) error {
	return nil
}

func (f *fakeBudget) GetTaskBudget(context.Context, string, uuid.UUID) (store.TaskBudgetStatus, error) {
	return store.TaskBudgetStatus{}, store.ErrBudgetNotReserved
}

func (f *fakeBudget) SettleTaskUsage(context.Context, store.SettleTaskUsageInput) (store.TaskBudgetStatus, error) {
	return store.TaskBudgetStatus{}, nil
}

func (f *fakeBudget) SettleTaskUsageDelta(context.Context, store.SettleTaskUsageDeltaInput) (store.TaskBudgetStatus, error) {
	return store.TaskBudgetStatus{}, nil
}

type fakeReceipts struct {
	lastResponse []byte
}

func (f *fakeReceipts) GetRuntimeReceipt(context.Context, string, uuid.UUID, string, string) (store.RuntimeReceipt, error) {
	return store.RuntimeReceipt{}, store.ErrNotFound
}

func (f *fakeReceipts) WriteRuntimeReceipt(_ context.Context, in store.WriteRuntimeReceiptInput) error {
	f.lastResponse = append([]byte(nil), in.Response...)
	return nil
}

type countingExecutor struct {
	calls int
}

func (e *countingExecutor) Execute(_ context.Context, request tool.ExecutionRequest) (tool.ExecutionResult, error) {
	e.calls++
	return tool.ExecutionResult{Output: json.RawMessage(`{"ok":true}`)}, nil
}

type fakeSecrets struct {
	handle tool.SecretHandle
}

func (f *fakeSecrets) Issue(context.Context, tool.SecretScope) (tool.SecretHandle, error) {
	return f.handle, nil
}

// --- gateway boundary fakes ---

type countingInvoker struct {
	invokes int
}

func (i *countingInvoker) InvokeTool(context.Context, tool.InvokeInput) (tool.InvokeResult, error) {
	i.invokes++
	return tool.InvokeResult{}, nil
}

func (i *countingInvoker) ListTools(context.Context, string) ([]store.ToolDescriptor, error) {
	i.invokes++
	return nil, nil
}

func (i *countingInvoker) GetToolDescriptor(context.Context, string, string, string) (store.ToolDescriptor, error) {
	return store.ToolDescriptor{}, nil
}

// fencedStore returns ErrFenced for every runtime mutation: any call that
// reaches it with a stale token is already past validation and must fail.
type fencedStore struct {
	store.RuntimeStore
}

func (s *fencedStore) HeartbeatLease(context.Context, store.HeartbeatLeaseInput) (store.Lease, error) {
	return store.Lease{}, store.ErrFenced
}

func (s *fencedStore) CommitCheckpoint(context.Context, store.CommitCheckpointInput) (store.Checkpoint, store.Attempt, error) {
	return store.Checkpoint{}, store.Attempt{}, store.ErrFenced
}

func (s *fencedStore) TransitionAttempt(context.Context, store.TransitionAttemptInput) (store.Attempt, error) {
	return store.Attempt{}, store.ErrFenced
}

func (s *fencedStore) PollRuntimeAssignment(context.Context, string, string) (store.RuntimeAssignment, error) {
	return store.RuntimeAssignment{}, store.ErrFenced
}
