//go:build integration

package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
)

func TestToolRegistryPersistenceAndImmutability(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	registered, err := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"}}}`),
	})
	if err != nil {
		t.Fatalf("register descriptor: %v", err)
	}
	if registered.SpecHash == ([32]byte{}) {
		t.Fatal("descriptor spec hash was not computed")
	}

	retried, err := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"}}}`),
	})
	if err != nil || retried.ID != registered.ID {
		t.Fatalf("idempotent re-registration: %+v err=%v", retried, err)
	}
	if _, err := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskHigh,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/etc"},
		ParamsSchema: []byte(`{"type":"object"}`),
	}); !errors.Is(err, kernelstore.ErrToolSpecConflict) {
		t.Fatalf("mutation of a published version: %v, want ErrToolSpecConflict", err)
	}

	if _, err := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: "tenant-a", Name: "fs.read", Version: "2.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"},"recursive":{"type":"boolean"}}}`),
	}); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	latest, err := repository.GetToolDescriptor(ctx, "tenant-a", "fs.read", "")
	if err != nil || latest.Version != "2.0.0" {
		t.Fatalf("latest lookup: %+v err=%v", latest, err)
	}
	exact, err := repository.GetToolDescriptor(ctx, "tenant-a", "fs.read", "1.0.0")
	if err != nil || exact.ID != registered.ID {
		t.Fatalf("exact version lookup: %+v err=%v", exact, err)
	}
	if _, err := repository.GetToolDescriptor(ctx, "tenant-a", "fs.missing", "1.0.0"); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("missing tool: %v, want ErrNotFound", err)
	}
	if descriptors, err := repository.ListToolDescriptors(ctx, "tenant-a"); err != nil || len(descriptors) != 2 {
		t.Fatalf("list descriptors: %v err=%v", descriptors, err)
	}
}

func TestToolCallLedgerIdempotencyAndCAS(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "tool-call-ledger", 3)

	callInput := kernelstore.CreateToolCallInput{
		ID: uuid.New(), TenantID: "tenant-a", TaskID: assignment.Task.ID, RunID: assignment.Run.ID,
		AttemptID: assignment.Attempt.ID, ToolName: "fs.read", ToolVersion: "1.0.0",
		Action: "read", Resource: "fs:/tmp", ArgsHash: argsHashOf(t, `{"path":"a.txt"}`), IdempotencyKey: "tool-invoke-1",
	}
	created, err := repository.CreateToolCall(ctx, callInput)
	if err != nil || created.Existing || created.ToolCall.Status != kernelstore.ToolCallPending {
		t.Fatalf("create call: %+v err=%v", created, err)
	}

	retried, err := repository.CreateToolCall(ctx, callInput)
	if err != nil || !retried.Existing || retried.ToolCall.ID != created.ToolCall.ID {
		t.Fatalf("idempotent retry: %+v err=%v", retried, err)
	}
	conflict := callInput
	conflict.ArgsHash = argsHashOf(t, `{"path":"b.txt"}`)
	if _, err := repository.CreateToolCall(ctx, conflict); !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("key reuse with different args: %v, want ErrIdempotencyConflict", err)
	}

	updated, err := repository.UpdateToolCall(ctx, kernelstore.UpdateToolCallInput{
		TenantID: "tenant-a", ToolCallID: created.ToolCall.ID, ExpectedVersion: 1,
		Status: kernelstore.ToolCallExecuted, PolicyRevision: policy.Revision,
	})
	if err != nil || updated.Status != kernelstore.ToolCallExecuted || updated.ResourceVersion != 2 {
		t.Fatalf("update call: %+v err=%v", updated, err)
	}
	if _, err := repository.UpdateToolCall(ctx, kernelstore.UpdateToolCallInput{
		TenantID: "tenant-a", ToolCallID: created.ToolCall.ID, ExpectedVersion: 1,
		Status: kernelstore.ToolCallFailed,
	}); !errors.Is(err, kernelstore.ErrVersionConflict) {
		t.Fatalf("stale CAS: %v, want ErrVersionConflict", err)
	}
	if _, err := repository.UpdateToolCall(ctx, kernelstore.UpdateToolCallInput{
		TenantID: "tenant-a", ToolCallID: created.ToolCall.ID, ExpectedVersion: 2,
		Status: kernelstore.ToolCallFailed,
	}); !errors.Is(err, kernelstore.ErrInvalidTransition) {
		t.Fatalf("invalid transition from terminal state: %v, want ErrInvalidTransition", err)
	}
}

func TestToolApprovalLifecycleAndExpiry(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "tool-approval", 3)
	call, err := repository.CreateToolCall(ctx, kernelstore.CreateToolCallInput{
		ID: uuid.New(), TenantID: "tenant-a", TaskID: assignment.Task.ID, RunID: assignment.Run.ID,
		AttemptID: assignment.Attempt.ID, ToolName: "fs.write", ToolVersion: "1.0.0",
		Action: "write", Resource: "fs:/tmp", ArgsHash: argsHashOf(t, `{"path":"a.txt","content":"x"}`),
		IdempotencyKey: "tool-write-1",
	})
	if err != nil {
		t.Fatalf("create call: %v", err)
	}
	now := clock.Now()
	approvalInput := kernelstore.CreateToolApprovalInput{
		ID: uuid.New(), TenantID: "tenant-a", CallID: call.ToolCall.ID, TaskID: assignment.Task.ID,
		RunID: assignment.Run.ID, AttemptID: assignment.Attempt.ID,
		ToolName: "fs.write", ToolVersion: "1.0.0", Action: "write", Resource: "fs:/tmp",
		ArgsHash:    argsHashOf(t, `{"path":"a.txt","content":"x"}`),
		RequestedAt: now, ExpiresAt: now.Add(15 * time.Minute),
	}
	created, err := repository.CreateToolApproval(ctx, approvalInput)
	if err != nil || created.Existing || created.ToolApproval.Status != kernelstore.ToolApprovalPending {
		t.Fatalf("create approval: %+v err=%v", created, err)
	}
	duplicate, err := repository.CreateToolApproval(ctx, approvalInput)
	if err != nil || !duplicate.Existing || duplicate.ToolApproval.ID != created.ToolApproval.ID {
		t.Fatalf("duplicate binding must converge on one approval: %+v err=%v", duplicate, err)
	}

	approved, err := repository.DecideToolApproval(ctx, kernelstore.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: created.ToolApproval.ID, ExpectedVersion: 1,
		Decision: kernelstore.ToolApprovalApproved, DecidedBy: "human-1", Now: now.Add(time.Minute),
	})
	if err != nil || approved.Status != kernelstore.ToolApprovalApproved || approved.DecidedBy != "human-1" {
		t.Fatalf("decide approval: %+v err=%v", approved, err)
	}
	if _, err := repository.DecideToolApproval(ctx, kernelstore.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: created.ToolApproval.ID, ExpectedVersion: approved.ResourceVersion,
		Decision: kernelstore.ToolApprovalRejected, DecidedBy: "human-2", Now: now.Add(2 * time.Minute),
	}); !errors.Is(err, kernelstore.ErrInvalidTransition) {
		t.Fatalf("double decision: %v, want ErrInvalidTransition", err)
	}

	expiring, err := repository.CreateToolApproval(ctx, kernelstore.CreateToolApprovalInput{
		ID: uuid.New(), TenantID: "tenant-a", CallID: call.ToolCall.ID, TaskID: assignment.Task.ID,
		RunID: assignment.Run.ID, AttemptID: assignment.Attempt.ID,
		ToolName: "fs.write", ToolVersion: "1.0.0", Action: "write", Resource: "fs:/tmp2",
		ArgsHash:    argsHashOf(t, `{"path":"c.txt","content":"y"}`),
		RequestedAt: now, ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("create expiring approval: %v", err)
	}
	if _, err := repository.DecideToolApproval(ctx, kernelstore.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: expiring.ToolApproval.ID, ExpectedVersion: 1,
		Decision: kernelstore.ToolApprovalApproved, DecidedBy: "human-1", Now: now.Add(2 * time.Minute),
	}); !errors.Is(err, kernelstore.ErrApprovalNotUsable) {
		t.Fatalf("expired approval decision: %v, want ErrApprovalNotUsable", err)
	}
	expired, err := repository.GetToolApproval(ctx, "tenant-a", expiring.ToolApproval.ID)
	if err != nil || expired.Status != kernelstore.ToolApprovalExpired {
		t.Fatalf("expired approval status: %+v err=%v", expired, err)
	}
}

func TestToolReceiptPersistenceAndIdempotency(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	assignment := scheduleRuntimeTask(t, ctx, repository, "tool-receipt", 3)
	requestHash := sha256.Sum256([]byte("request-a"))

	if err := repository.WriteRuntimeReceipt(ctx, kernelstore.WriteRuntimeReceiptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, Operation: "TOOL:fs.read@1.0.0",
		IdempotencyKey: "invoke-a", RequestHash: requestHash, Response: []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	receipt, err := repository.GetRuntimeReceipt(ctx, "tenant-a", assignment.Attempt.ID, "TOOL:fs.read@1.0.0", "invoke-a")
	if err != nil || receipt.RequestHash != requestHash || !jsonEqual(t, receipt.Response, `{"ok":true}`) {
		t.Fatalf("read receipt: %+v err=%v", receipt, err)
	}
	if err := repository.WriteRuntimeReceipt(ctx, kernelstore.WriteRuntimeReceiptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, Operation: "TOOL:fs.read@1.0.0",
		IdempotencyKey: "invoke-a", RequestHash: requestHash, Response: []byte(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("idempotent receipt write: %v", err)
	}
	if err := repository.WriteRuntimeReceipt(ctx, kernelstore.WriteRuntimeReceiptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, Operation: "TOOL:fs.read@1.0.0",
		IdempotencyKey: "invoke-a", RequestHash: sha256.Sum256([]byte("different-request")), Response: []byte(`{"ok":false}`),
	}); !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("key reuse with different hash: %v, want ErrIdempotencyConflict", err)
	}
}

func TestToolGatewayHardStopOnExhaustedBudget(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	_, err := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
		TenantID: "tenant-a", Name: "fs.read", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskLow,
		Actions: []string{"read"}, ResourcePatterns: []string{"fs:/tmp"},
		ParamsSchema: []byte(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
	})
	if err != nil {
		t.Fatalf("register descriptor: %v", err)
	}
	engine, err := policy.New(policy.TenantPolicies{"tenant-a": {
		MaxPriority: 100, AllowedTools: []string{"fs.read"}, ApprovalRequiredRisk: "high",
	}})
	if err != nil {
		t.Fatalf("policy engine: %v", err)
	}
	executor := &countingExecutor{}
	gateway := tool.NewGateway(engine, repository, repository, repository, executor, &devSecretBroker{})

	rescheduled := scheduleRuntimeTask(t, ctx, repository, "tool-hardstop", 3)
	callInput := func(key string) tool.InvokeInput {
		return tool.InvokeInput{
			TenantID: "tenant-a", TaskID: rescheduled.Task.ID, RunID: rescheduled.Run.ID, AttemptID: rescheduled.Attempt.ID,
			AgentVersionRef: "agent@1", ToolName: "fs.read", Action: "read", Resource: "fs:/tmp",
			Args: []byte(`{"path":"a.txt"}`), IdempotencyKey: key,
		}
	}
	invoked := 0
	for invoked < 10 {
		if _, err := gateway.InvokeTool(ctx, callInput("hardstop-"+string(rune('a'+invoked)))); err != nil {
			t.Fatalf("invoke %d: %v", invoked, err)
		}
		invoked++
	}
	if _, err := gateway.InvokeTool(ctx, callInput("hardstop-over")); !errors.Is(err, tool.ErrBudgetExhausted) {
		t.Fatalf("invoke past budget: %v, want ErrBudgetExhausted", err)
	}
	if executor.calls != 10 {
		t.Fatalf("executions = %d, want 10 (hard stop must precede the 11th execution)", executor.calls)
	}
}

type countingExecutor struct{ calls int }

func (e *countingExecutor) Execute(context.Context, tool.ExecutionRequest) (tool.ExecutionResult, error) {
	e.calls++
	return tool.ExecutionResult{Output: []byte(`{"ok":true}`)}, nil
}

type devSecretBroker struct{}

func (d *devSecretBroker) Issue(context.Context, tool.SecretScope) (tool.SecretHandle, error) {
	return "dev-handle", nil
}

func argsHashOf(t *testing.T, args string) [32]byte {
	t.Helper()
	_, hash, err := tool.NormalizeArgs([]byte(`{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path"]}`), []byte(args), 1<<16)
	if err != nil {
		t.Fatalf("normalize args: %v", err)
	}
	return hash
}

// jsonEqual compares two JSON documents semantically, because jsonb columns
// normalize whitespace on read.
func jsonEqual(t *testing.T, actual []byte, expected string) bool {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(actual, &a); err != nil {
		t.Fatalf("actual receipt response is not JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(expected), &b); err != nil {
		t.Fatalf("expected response is not JSON: %v", err)
	}
	return reflect.DeepEqual(a, b)
}
