// Package security is the v0.3 negative test suite: it proves that the four
// security boundaries hold under attack, not just under happy paths:
//
//  1. Secret leak — a scoped credential handle must never reach the Agent,
//     the result, or the receipt, even when the executing tool echoes it.
//  2. Approval binding — a human approval for one invocation must not
//     authorize a different tool, resource, argument set, or attempt.
//  3. Tenant escape — a request claiming another tenant must be rejected
//     before any store access on every fenced boundary.
//  4. Fencing replay — a replayed stale fencing token must be rejected after
//     the attempt's ownership moved on.
//
// The integration leg (real PostgreSQL, real takeover sequence) lives in
// negative_integration_test.go.
package security

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- scenario 1: secret leak ---

type leakyExecutor struct{}

// Execute echoes the scoped credential back into the output: the worst-case
// tool behavior the gateway must contain.
func (e *leakyExecutor) Execute(_ context.Context, request tool.ExecutionRequest) (tool.ExecutionResult, error) {
	output, err := json.Marshal(map[string]any{"secret": string(request.Secret), "result": "ok"})
	if err != nil {
		return tool.ExecutionResult{}, err
	}
	return tool.ExecutionResult{Output: output}, nil
}

// TestSecretHandleNeverLeaksProves the handle is redacted from the result and
// never written into the receipt or the tool call record.
func TestSecretHandleNeverLeaks(t *testing.T) {
	ctx := context.Background()
	const leaked = "sk-scoped-secret-42"
	fakes := newGatewayFakes()
	fakes.secrets.handle = tool.SecretHandle(leaked)
	fakes.executor = &leakyExecutor{}
	gateway := newSecurityGateway(fakes)

	result, err := gateway.InvokeTool(ctx, invoke("read", `{"path":"fs:/tmp/notes"}`, "invoke-leak-1"))
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result.Outcome != tool.OutcomeExecuted {
		t.Fatalf("outcome = %s", result.Outcome)
	}
	if bytes.Contains(result.Result, []byte(leaked)) {
		t.Fatalf("secret leaked through the result: %s", result.Result)
	}
	if bytes.Contains(result.Result, []byte("sk-scoped")) {
		t.Fatalf("secret-shaped fragment leaked through the result: %s", result.Result)
	}
	// The receipt response must not carry the credential either.
	if receipt := fakes.receipts.lastResponse; receipt != nil && bytes.Contains(receipt, []byte(leaked)) {
		t.Fatalf("secret leaked through the receipt: %s", receipt)
	}
	// The stored tool call is identity and decision data only.
	for _, call := range fakes.tools.calls {
		encoded, err := json.Marshal(call)
		if err != nil {
			t.Fatalf("marshal call: %v", err)
		}
		if bytes.Contains(encoded, []byte(leaked)) {
			t.Fatalf("secret leaked through the tool call record: %s", encoded)
		}
	}
}

// --- scenario 2: approval binding ---

// TestApprovalCannotAuthorizeDifferentInvocation proves a decided approval is
// bound to the exact call summary (tool, action, resource, canonical args,
// attempt) and cannot be replayed onto a different invocation.
func TestApprovalCannotAuthorizeDifferentInvocation(t *testing.T) {
	ctx := context.Background()
	fakes := newGatewayFakes()
	fakes.policy.decisions["write"] = policy.Decision{Allow: true, RequiresApproval: true}
	gateway := newSecurityGateway(fakes)

	// First invocation parks with a bound approval.
	first, err := gateway.InvokeTool(ctx, invoke("write", `{"path":"fs:/tmp/a"}`, "approval-1"))
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	if first.Outcome != tool.OutcomeRequiresApproval || first.ApprovalID == nil {
		t.Fatalf("first outcome = %s approval = %v", first.Outcome, first.ApprovalID)
	}
	approval, err := fakes.tools.DecideToolApproval(ctx, store.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: *first.ApprovalID, ExpectedVersion: 1,
		Decision: store.ToolApprovalApproved, DecidedBy: "human-1",
		Now: time.Unix(1_800_000_000, 0).UTC(),
	})
	if err != nil || approval.Status != store.ToolApprovalApproved {
		t.Fatalf("decide: %+v err=%v", approval, err)
	}

	// The identical invocation with the approval executes.
	same, err := gateway.InvokeTool(ctx, invokeWithApproval("write", `{"path":"fs:/tmp/a"}`, "approval-2", *first.ApprovalID))
	if err != nil {
		t.Fatalf("identical invoke: %v", err)
	}
	if same.Outcome != tool.OutcomeExecuted {
		t.Fatalf("identical outcome = %s", same.Outcome)
	}

	// Different canonical args: binding mismatch, nothing runs.
	var binding *tool.ApprovalNotUsableError
	if _, err := gateway.InvokeTool(ctx, invokeWithApproval("write", `{"path":"fs:/tmp/b"}`, "approval-3", *first.ApprovalID)); !errors.As(err, &binding) || binding.Reason != tool.ApprovalBindingMismatch {
		t.Fatalf("different args error = %v, want BINDING_MISMATCH", err)
	}
	if fakes.executor.(*countingExecutor).calls != 1 {
		t.Fatalf("executor ran %d times, want 1 (mismatched approval must not execute)", fakes.executor.(*countingExecutor).calls)
	}

	// Different resource: binding mismatch.
	if _, err := gateway.InvokeTool(ctx, invokeWithApproval("write", `{"path":"other"}`, "approval-4", *first.ApprovalID)); !errors.As(err, &binding) {
		t.Fatalf("different resource error = %v, want BINDING_MISMATCH", err)
	}
	if fakes.executor.(*countingExecutor).calls != 1 {
		t.Fatalf("executor ran %d times after resource mismatch, want 1", fakes.executor.(*countingExecutor).calls)
	}

	// A rejected approval cannot be reused.
	rejected := invoke("write", `{"path":"fs:/tmp/c"}`, "approval-6")
	rejectedApproval, err := gateway.InvokeTool(ctx, rejected)
	if err != nil || rejectedApproval.Outcome != tool.OutcomeRequiresApproval || rejectedApproval.ApprovalID == nil {
		t.Fatalf("second park: %+v err=%v", rejectedApproval, err)
	}
	if _, err := fakes.tools.DecideToolApproval(ctx, store.DecideToolApprovalInput{
		TenantID: "tenant-a", ApprovalID: *rejectedApproval.ApprovalID, ExpectedVersion: 1,
		Decision: store.ToolApprovalRejected, DecidedBy: "human-1",
		Now: time.Unix(1_800_000_000, 0).UTC(),
	}); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if _, err := gateway.InvokeTool(ctx, invokeWithApproval("write", `{"path":"fs:/tmp/c"}`, "approval-7", *rejectedApproval.ApprovalID)); !errors.As(err, &binding) || binding.Reason != tool.ApprovalRejected {
		t.Fatalf("rejected approval error = %v, want REJECTED", err)
	}
	if fakes.executor.(*countingExecutor).calls != 1 {
		t.Fatalf("executor ran %d times after rejection, want 1", fakes.executor.(*countingExecutor).calls)
	}
}

// --- scenario 3: tenant escape ---

// TestTenantEscapeRejectedAtGatewayBoundary proves the fenced Tool Gateway
// rejects every request claiming a tenant other than its own before the
// invoker is consulted.
func TestTenantEscapeRejectedAtGatewayBoundary(t *testing.T) {
	ctx := context.Background()
	invoker := &countingInvoker{}
	service := gateway.NewService(invoker, "tenant-a")

	if _, err := service.ListTools(ctx, &gatewayv1.ListToolsRequest{TenantId: "tenant-b"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant ListTools error = %v, want PermissionDenied", err)
	}
	_, err := service.InvokeTool(ctx, &gatewayv1.InvokeToolRequest{
		Identity: &gatewayv1.AttemptIdentity{
			TenantId: "tenant-b", AttemptId: uuid.New().String(), FencingToken: 1,
		},
		TaskId: uuid.New().String(), RunId: uuid.New().String(),
		AgentVersionRef: "agent@1", ToolName: "fs.read", Action: "read", Resource: "fs:/tmp",
		ArgsJson: []byte(`{"path":"fs:/tmp"}`), IdempotencyKey: "escape-1",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant InvokeTool error = %v, want PermissionDenied", err)
	}
	if invoker.invokes != 0 {
		t.Fatalf("invoker was consulted %d times for a cross-tenant call", invoker.invokes)
	}
}

// TestTenantEscapeRejectedAtRuntimeControlBoundary proves the Runtime
// Protocol rejects cross-tenant polls before the store is consulted.
func TestTenantEscapeRejectedAtRuntimeControlBoundary(t *testing.T) {
	service := control.NewService(&fencedStore{}, "tenant-a", time.Minute)
	_, err := service.PollAssignment(context.Background(), &runtimev1.PollAssignmentRequest{
		TenantId: "tenant-b", RuntimeInstanceId: "worker-1",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant poll error = %v, want PermissionDenied", err)
	}
}

// --- scenario 4: fencing replay (protocol mapping) ---

// TestFencingReplayRejectedAtProtocolBoundary proves a stale fencing token
// surfaces as PermissionDenied at the Runtime Protocol boundary, so a worker
// replaying an old execution cannot mutate state it no longer owns.
func TestFencingReplayRejectedAtProtocolBoundary(t *testing.T) {
	service := control.NewService(&fencedStore{}, "tenant-a", time.Minute)
	identity := &runtimev1.AttemptIdentity{
		TenantId: "tenant-a", AttemptId: uuid.New().String(), FencingToken: 1,
	}
	_, err := service.Heartbeat(context.Background(), &runtimev1.HeartbeatRequest{
		Identity: identity, ExpectedLeaseVersion: 3, RequestedTtlSeconds: 30, IdempotencyKey: "replay-heartbeat",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale heartbeat error = %v, want PermissionDenied", err)
	}
	_, err = service.CommitCheckpoint(context.Background(), &runtimev1.CommitCheckpointRequest{
		Identity: identity, CheckpointId: uuid.New().String(), ExpectedAttemptVersion: 1,
		IdempotencyKey: "replay-checkpoint", AgentVersionRef: "agent@1", Provider: "reference-go",
		RuntimeAbi: "fenced.reference/v1", SchemaVersion: "state/v1",
		State: &runtimev1.ArtifactReference{Uri: "artifact://tenant-a/sha256/ab", Sha256: sha256Hex("x"), SizeBytes: 1, MediaType: "application/octet-stream"},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale checkpoint error = %v, want PermissionDenied", err)
	}
	_, err = service.TransitionAttempt(context.Background(), &runtimev1.TransitionAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: 1, IdempotencyKey: "replay-transition",
		TargetPhase: runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale transition error = %v, want PermissionDenied", err)
	}
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
