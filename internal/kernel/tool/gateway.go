package tool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/platform/redact"
	"github.com/google/uuid"
)

var (
	// ErrToolArgsInvalid reports arguments that fail schema validation or
	// size bounds.
	ErrToolArgsInvalid = errors.New("tool arguments are invalid")
	// ErrBudgetExhausted is the hard-stop outcome: the task budget is
	// exhausted and new consumption must stop (invariant 12).
	ErrBudgetExhausted = errors.New("task budget exhausted: new consumption stopped")
	// ErrToolExecutionFailed reports a tool that ran but did not succeed; the
	// failure is already recorded in the side-effect receipt.
	ErrToolExecutionFailed = errors.New("tool execution failed")
)

// ToolExecutionError carries the machine-readable failure code of a tool that
// ran but did not succeed. The receipt already records the failure, so a
// replay returns the same outcome.
type ToolExecutionError struct {
	Code string
	Err  error
}

func (e *ToolExecutionError) Error() string { return fmt.Sprintf("%s: %s", e.Err, e.Code) }

func (e *ToolExecutionError) Unwrap() error { return e.Err }

// ApprovalNotUsableReason classifies why a bound approval cannot authorize an
// invocation, so the fenced boundary and the runtime can react differently to
// "still pending" (park) versus "rejected or expired" (fail).
type ApprovalNotUsableReason string

const (
	ApprovalPending         ApprovalNotUsableReason = "PENDING"
	ApprovalRejected        ApprovalNotUsableReason = "REJECTED"
	ApprovalExpired         ApprovalNotUsableReason = "EXPIRED"
	ApprovalBindingMismatch ApprovalNotUsableReason = "BINDING_MISMATCH"
)

// ApprovalNotUsableError carries the reason behind an unusable approval.
type ApprovalNotUsableError struct {
	Reason ApprovalNotUsableReason
	Err    error
}

func (e *ApprovalNotUsableError) Error() string {
	return fmt.Sprintf("%s: %s", e.Err, e.Reason)
}

func (e *ApprovalNotUsableError) Unwrap() error { return e.Err }

// PolicyChecker decides tool invocations outside the LLM. The engine is
// fail-closed: missing tenant data and evaluation errors deny by default.
type PolicyChecker interface {
	EvaluateTool(context.Context, string, policy.ToolContext) policy.Decision
}

// SecretScope describes the constrained capability a tool may use.
type SecretScope struct {
	TenantID  string
	AttemptID string
	ToolName  string
	Resource  string
	SecretRef string
}

// SecretHandle is an opaque, scope-limited capability handle. The actual
// credential is injected by the Tool Gateway into the executing adapter and
// never reaches the Agent (architecture §12.4).
type SecretHandle string

// SecretBroker issues scoped handles for a tool invocation.
type SecretBroker interface {
	Issue(context.Context, SecretScope) (SecretHandle, error)
}

// ExecutionRequest is the sanitized input an executing adapter receives.
type ExecutionRequest struct {
	Descriptor store.ToolDescriptor
	Action     string
	Resource   string
	Args       json.RawMessage
	Secret     SecretHandle
}

// ExecutionResult is the outcome of one tool execution. Output must already be
// sanitized by the adapter; the gateway additionally bounds and redacts it.
type ExecutionResult struct {
	Output      json.RawMessage
	FailureCode string
}

// ToolExecutor runs one tool invocation with a constrained credential.
type ToolExecutor interface {
	Execute(context.Context, ExecutionRequest) (ExecutionResult, error)
}

// ReceiptStore persists and replays side-effect receipts (at-least-once +
// idempotent consumer contract, ADR-003).
type ReceiptStore interface {
	GetRuntimeReceipt(context.Context, string, uuid.UUID, string, string) (store.RuntimeReceipt, error)
	WriteRuntimeReceipt(context.Context, store.WriteRuntimeReceiptInput) error
}

// InvokeInput is a fenced tool invocation from a running Attempt.
type InvokeInput struct {
	TenantID  string
	TaskID    uuid.UUID
	RunID     uuid.UUID
	AttemptID uuid.UUID
	// FencingToken is the fenced identity of the invoking Attempt. The
	// gateway itself does not verify it (lease/fencing is enforced by the
	// Runtime Protocol boundary); carriers pass it through so downstream
	// fenced transports (for example a worker's gRPC forwarding) can
	// preserve it.
	FencingToken    int64
	AgentVersionRef string
	// SecretRef is an optional symbolic capability from the immutable
	// AgentVersion manifest. Empty means this invocation receives no secret.
	SecretRef string
	ToolName  string
	// ToolVersion selects a descriptor; empty resolves the latest version.
	ToolVersion    string
	Action         string
	Resource       string
	Args           json.RawMessage
	IdempotencyKey string
	// ApprovalID is required when the previous decision was REQUIRES_APPROVAL
	// and a human decided it; the gateway re-checks the binding (§9.2).
	ApprovalID *uuid.UUID
}

func (in InvokeInput) validate() error {
	if strings.TrimSpace(in.TenantID) == "" || strings.TrimSpace(in.AgentVersionRef) == "" ||
		in.TaskID == uuid.Nil || in.RunID == uuid.Nil || in.AttemptID == uuid.Nil {
		return fmt.Errorf("tenant, task, run, attempt, and agent version are required")
	}
	if err := store.ValidateToolName(in.ToolName); err != nil {
		return err
	}
	if strings.TrimSpace(in.Action) == "" || strings.TrimSpace(in.Resource) == "" {
		return fmt.Errorf("action and resource are required")
	}
	if strings.TrimSpace(in.IdempotencyKey) == "" || len(in.IdempotencyKey) > 128 {
		return fmt.Errorf("idempotency key is required and bounded to 128 characters")
	}
	return nil
}

// InvokeOutcome classifies the result of an invocation.
type InvokeOutcome string

const (
	// OutcomeExecuted: the tool ran and its receipt is durable.
	OutcomeExecuted InvokeOutcome = "EXECUTED"
	// OutcomeDenied: policy denied the invocation; nothing ran.
	OutcomeDenied InvokeOutcome = "DENIED"
	// OutcomeRequiresApproval: a human decision is required; no budget or
	// side effect was touched.
	OutcomeRequiresApproval InvokeOutcome = "REQUIRES_APPROVAL"
	// OutcomeReplayed: the identical invocation already ran; the stored
	// receipt is returned without re-execution.
	OutcomeReplayed InvokeOutcome = "REPLAYED"
)

// InvokeResult is the machine-readable outcome of an invocation.
type InvokeResult struct {
	Outcome          InvokeOutcome
	ToolCall         store.ToolCall
	ApprovalID       *uuid.UUID
	Result           json.RawMessage
	ReceiptOperation string
	DenyReasons      []string
	PolicyRevision   string
}

// Gateway orchestrates the tool decision chain. It is the enforcement point
// for policy, approval binding, budget hard-stop and receipts; the executing
// adapter is injected so the chain is testable without real tools.
type Gateway struct {
	policy         PolicyChecker
	tools          store.ToolStore
	budget         store.BudgetStore
	receipts       ReceiptStore
	executor       ToolExecutor
	secrets        SecretBroker
	approvalTTL    time.Duration
	maxArgsBytes   int64
	maxResultBytes int64
	now            func() time.Time
	newID          func() uuid.UUID
}

// GatewayOption configures a Gateway.
type GatewayOption func(*Gateway)

// WithApprovalTTL sets the lifetime of created approval requests
// (default 15 minutes).
func WithApprovalTTL(ttl time.Duration) GatewayOption {
	return func(g *Gateway) { g.approvalTTL = ttl }
}

// WithBounds sets the argument and result size bounds (defaults 64 KiB and
// 1 MiB).
func WithBounds(maxArgsBytes, maxResultBytes int64) GatewayOption {
	return func(g *Gateway) { g.maxArgsBytes, g.maxResultBytes = maxArgsBytes, maxResultBytes }
}

// WithClock overrides the clock and ID source for tests.
func WithClock(now func() time.Time, newID func() uuid.UUID) GatewayOption {
	return func(g *Gateway) { g.now, g.newID = now, newID }
}

func NewGateway(policyChecker PolicyChecker, tools store.ToolStore, budget store.BudgetStore,
	receipts ReceiptStore, executor ToolExecutor, secrets SecretBroker, options ...GatewayOption) *Gateway {
	gateway := &Gateway{
		policy: policyChecker, tools: tools, budget: budget, receipts: receipts,
		executor: executor, secrets: secrets,
		approvalTTL: 15 * time.Minute, maxArgsBytes: 64 << 10, maxResultBytes: 1 << 20,
		now: time.Now, newID: func() uuid.UUID {
			id, err := uuid.NewV7()
			if err != nil {
				return uuid.New()
			}
			return id
		},
	}
	for _, option := range options {
		option(gateway)
	}
	return gateway
}

// ListTools returns the tenant's registered tool descriptors.
func (g *Gateway) ListTools(ctx context.Context, tenantID string) ([]store.ToolDescriptor, error) {
	return g.tools.ListToolDescriptors(ctx, tenantID)
}

// GetToolDescriptor returns one immutable descriptor. An empty version selects
// the registry's latest; the gateway service pins a concrete version before it
// reaches here whenever a capability freeze is enforced.
func (g *Gateway) GetToolDescriptor(ctx context.Context, tenantID, name, version string) (store.ToolDescriptor, error) {
	return g.tools.GetToolDescriptor(ctx, tenantID, name, version)
}

// InvokeTool runs one invocation through the full decision chain.
func (g *Gateway) InvokeTool(ctx context.Context, in InvokeInput) (InvokeResult, error) {
	var result InvokeResult
	if err := in.validate(); err != nil {
		return result, err
	}
	descriptor, err := g.tools.GetToolDescriptor(ctx, in.TenantID, in.ToolName, in.ToolVersion)
	if err != nil {
		return result, fmt.Errorf("resolve tool %s@%s: %w", in.ToolName, in.ToolVersion, err)
	}
	normalizedArgs, argsHash, err := NormalizeArgs(descriptor.ParamsSchema, in.Args, g.maxArgsBytes)
	if err != nil {
		return result, fmt.Errorf("%w: %v", ErrToolArgsInvalid, err)
	}
	operation := "TOOL:" + descriptor.Name + "@" + descriptor.Version

	callInput := store.CreateToolCallInput{
		TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID, AttemptID: in.AttemptID,
		ToolName: descriptor.Name, ToolVersion: descriptor.Version,
		Action: in.Action, Resource: in.Resource, ArgsHash: argsHash, IdempotencyKey: in.IdempotencyKey,
	}
	requestHash, err := callInput.RequestHash()
	if err != nil {
		return result, err
	}

	// At-least-once delivery: a durable receipt means this exact invocation
	// already ran; replay its outcome instead of executing again. A failed
	// outcome replays as the same failure.
	if receipt, err := g.receipts.GetRuntimeReceipt(ctx, in.TenantID, in.AttemptID, operation, in.IdempotencyKey); err == nil {
		if !bytes.Equal(receipt.RequestHash[:], requestHash[:]) {
			return result, fmt.Errorf("%w: idempotency key reused with a different invocation", store.ErrIdempotencyConflict)
		}
		var envelope struct {
			FailureCode string          `json:"failureCode"`
			Output      json.RawMessage `json:"output"`
		}
		if err := json.Unmarshal(receipt.Response, &envelope); err != nil {
			return result, fmt.Errorf("decode stored receipt: %w", err)
		}
		if envelope.FailureCode != "" {
			return result, fmt.Errorf("%w: %s", ErrToolExecutionFailed, envelope.FailureCode)
		}
		return InvokeResult{
			Outcome: OutcomeReplayed, Result: envelope.Output, ReceiptOperation: operation,
		}, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return result, fmt.Errorf("read runtime receipt: %w", err)
	}

	callInput.ID = g.newID()
	created, err := g.tools.CreateToolCall(ctx, callInput)
	if err != nil {
		return result, err
	}
	call := created.ToolCall
	revision := policy.Revision

	decision := g.policy.EvaluateTool(ctx, in.TenantID, policy.ToolContext{
		Name: descriptor.Name, Version: descriptor.Version,
		Action: in.Action, Resource: in.Resource, Risk: string(descriptor.SideEffectRisk),
	})
	if !decision.Allow {
		call, err = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
			TenantID: in.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
			Status: store.ToolCallDenied, DecisionReasons: decision.DenyReasons, PolicyRevision: revision,
		})
		if err != nil {
			return result, err
		}
		return InvokeResult{
			Outcome: OutcomeDenied, ToolCall: call, DenyReasons: decision.DenyReasons, PolicyRevision: revision,
		}, nil
	}

	if decision.RequiresApproval {
		if in.ApprovalID == nil {
			return g.requestApproval(ctx, created, descriptor, in, argsHash, revision)
		}
		approval, err := g.tools.GetToolApproval(ctx, in.TenantID, *in.ApprovalID)
		if err != nil {
			return result, err
		}
		if err := g.checkApproval(approval, call, descriptor, in, argsHash); err != nil {
			return result, err
		}
		call, err = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
			TenantID: in.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
			Status: store.ToolCallApproved, PolicyRevision: revision, ApprovalID: &approval.ID,
		})
		if err != nil {
			return result, err
		}
	}

	if err := g.enforceBudget(ctx, call); err != nil {
		return result, err
	}

	var handle SecretHandle
	if strings.TrimSpace(in.SecretRef) != "" {
		handle, err = g.secrets.Issue(ctx, SecretScope{
			TenantID: in.TenantID, AttemptID: in.AttemptID.String(), ToolName: descriptor.Name,
			Resource: in.Resource, SecretRef: in.SecretRef,
		})
		if err != nil {
			_, _ = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
				TenantID: in.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
				Status: store.ToolCallFailed, DecisionReasons: []string{"SECRET_BROKER_FAILED"}, PolicyRevision: revision,
			})
			return result, fmt.Errorf("issue scoped credential: %w", err)
		}
	}

	executed, execErr := g.executor.Execute(ctx, ExecutionRequest{
		Descriptor: descriptor, Action: in.Action, Resource: in.Resource,
		Args: normalizedArgs, Secret: handle,
	})
	output, failureCode := g.sanitize(executed.Output, handle), executed.FailureCode
	if execErr != nil && failureCode == "" {
		failureCode = "tool_execution_error"
	}
	response, err := json.Marshal(map[string]any{
		"tool": descriptor.Name, "version": descriptor.Version,
		"action": in.Action, "resource": in.Resource,
		"output": json.RawMessage(output), "failureCode": failureCode,
		"usage": map[string]any{"toolCalls": 1},
	})
	if err != nil {
		return result, err
	}
	if err := g.receipts.WriteRuntimeReceipt(ctx, store.WriteRuntimeReceiptInput{
		TenantID: in.TenantID, AttemptID: in.AttemptID, Operation: operation,
		IdempotencyKey: in.IdempotencyKey, RequestHash: requestHash, Response: response,
	}); err != nil {
		return result, fmt.Errorf("write side-effect receipt: %w", err)
	}
	if execErr != nil || failureCode != "" {
		_, _ = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
			TenantID: in.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
			Status: store.ToolCallFailed, DecisionReasons: []string{failureCode}, PolicyRevision: revision,
		})
		return result, &ToolExecutionError{Code: failureCode, Err: ErrToolExecutionFailed}
	}
	call, err = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
		TenantID: in.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
		Status: store.ToolCallExecuted, PolicyRevision: revision,
	})
	if err != nil {
		return result, err
	}
	return InvokeResult{
		Outcome: OutcomeExecuted, ToolCall: call, Result: output,
		ReceiptOperation: operation, PolicyRevision: revision,
	}, nil
}

// requestApproval creates the bound approval request and parks the call in
// REQUIRES_APPROVAL. Identical concurrent requests converge on one approval,
// and an already-parked call is left untouched.
func (g *Gateway) requestApproval(ctx context.Context, created store.CreateToolCallResult,
	descriptor store.ToolDescriptor, in InvokeInput, argsHash [sha256.Size]byte, revision string) (InvokeResult, error) {
	var result InvokeResult
	call := created.ToolCall
	now := g.now()
	approvalInput := store.CreateToolApprovalInput{
		ID: g.newID(), TenantID: in.TenantID, CallID: call.ID,
		TaskID: in.TaskID, RunID: in.RunID, AttemptID: in.AttemptID,
		ToolName: descriptor.Name, ToolVersion: descriptor.Version,
		Action: in.Action, Resource: in.Resource, ArgsHash: argsHash,
		RequestedAt: now, ExpiresAt: now.Add(g.approvalTTL),
	}
	createdApproval, err := g.tools.CreateToolApproval(ctx, approvalInput)
	if err != nil {
		return result, err
	}
	approvalID := createdApproval.ToolApproval.ID
	if !created.Existing || call.Status != store.ToolCallRequiresApproval {
		call, err = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
			TenantID: in.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
			Status: store.ToolCallRequiresApproval, PolicyRevision: revision, ApprovalID: &approvalID,
		})
		if err != nil {
			return result, err
		}
	}
	return InvokeResult{
		Outcome: OutcomeRequiresApproval, ToolCall: call, ApprovalID: &approvalID,
		PolicyRevision: revision,
	}, nil
}

// checkApproval enforces invariant 9: the approval must be APPROVED, not
// expired, and bound to exactly this call summary (tool, version, action,
// resource, canonical args hash, attempt).
func (g *Gateway) checkApproval(approval store.ToolApproval, call store.ToolCall,
	descriptor store.ToolDescriptor, in InvokeInput, argsHash [sha256.Size]byte) error {
	switch approval.Status {
	case store.ToolApprovalRejected:
		return &ApprovalNotUsableError{Reason: ApprovalRejected, Err: store.ErrApprovalNotUsable}
	case store.ToolApprovalPending:
		return &ApprovalNotUsableError{Reason: ApprovalPending, Err: store.ErrApprovalNotUsable}
	case store.ToolApprovalExpired:
		return &ApprovalNotUsableError{Reason: ApprovalExpired, Err: store.ErrApprovalNotUsable}
	}
	if approval.CallID != call.ID || approval.AttemptID != in.AttemptID ||
		approval.ToolName != descriptor.Name || approval.ToolVersion != descriptor.Version ||
		approval.Action != in.Action || approval.Resource != in.Resource ||
		approval.ArgsHash != argsHash {
		return &ApprovalNotUsableError{Reason: ApprovalBindingMismatch, Err: store.ErrApprovalNotUsable}
	}
	if now := g.now(); !now.Before(approval.ExpiresAt) {
		return &ApprovalNotUsableError{Reason: ApprovalExpired, Err: store.ErrApprovalNotUsable}
	}
	return nil
}

// enforceBudget applies the hard-stop invariant: no new consumption after the
// reserved ledger is exhausted, enforced by the ledger itself on settlement.
// Tasks without a reservation carry no budget and are not gated.
func (g *Gateway) enforceBudget(ctx context.Context, call store.ToolCall) error {
	status, err := g.budget.GetTaskBudget(ctx, call.TenantID, call.TaskID)
	if err != nil {
		if errors.Is(err, store.ErrBudgetNotReserved) {
			return nil
		}
		return fmt.Errorf("read task budget: %w", err)
	}
	if status.Exhausted {
		_, _ = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
			TenantID: call.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
			Status: store.ToolCallFailed, DecisionReasons: []string{"BUDGET_EXHAUSTED"},
		})
		return ErrBudgetExhausted
	}
	if _, err := g.budget.SettleTaskUsage(ctx, store.SettleTaskUsageInput{
		TenantID: call.TenantID, TaskID: call.TaskID,
		IdempotencyKey: "tool:" + call.ID.String(), Usage: store.TaskBudget{ToolCalls: 1},
	}); err != nil {
		if errors.Is(err, store.ErrBudgetExceeded) {
			_, _ = g.tools.UpdateToolCall(ctx, store.UpdateToolCallInput{
				TenantID: call.TenantID, ToolCallID: call.ID, ExpectedVersion: call.ResourceVersion,
				Status: store.ToolCallFailed, DecisionReasons: []string{"BUDGET_EXHAUSTED"},
			})
			return ErrBudgetExhausted
		}
		return fmt.Errorf("settle tool usage: %w", err)
	}
	return nil
}

// sanitize bounds the result and redacts the scoped credential handle so a
// secret can never leak through a tool result.
func (g *Gateway) sanitize(output json.RawMessage, handle SecretHandle) []byte {
	if len(output) == 0 {
		return []byte("null")
	}
	if int64(len(output)) > g.maxResultBytes {
		output = output[:g.maxResultBytes]
	}
	redacted, ok := redact.RedactJSON(output, string(handle))
	if !ok {
		return []byte("null")
	}
	return redacted
}
