package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/tokens"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// invokerEnv assembles the decision chain (policy + store fakes reused from
// the gateway tests) around one fake OpenAI-compatible endpoint.
type invokerEnv struct {
	gateway   *Gateway
	policy    *fakeModelPolicy
	store     *fakeModelStore
	budget    *fakeModelBudget
	receipts  *fakeModelReceipts
	registry  *provider.Registry
	endpoint  *httptest.Server
	responses chan fakeReply
}

type fakeReply struct {
	status int
	body   string
	stream []string
}

func newInvokerEnv(t *testing.T, script ...fakeReply) *invokerEnv {
	t.Helper()
	store := newFakeModelStore()
	store.descriptors["fake/agent-model"] = kernelstore.ModelDescriptor{
		TenantID: "tenant-1", Provider: "fake", ModelName: "agent-model", SupportsStreaming: true,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(3), OutputPriceMicroUSDPerMillion: money.MustFromUSD(6), PriceRevision: "p1",
	}
	store.descriptors["fake/batch-model"] = kernelstore.ModelDescriptor{
		TenantID: "tenant-1", Provider: "fake", ModelName: "batch-model", SupportsStreaming: false,
		InputPriceMicroUSDPerMillion: money.MustFromUSD(1), OutputPriceMicroUSDPerMillion: money.MustFromUSD(1), PriceRevision: "p1",
	}
	env := &invokerEnv{
		policy: &fakeModelPolicy{decisions: map[string]policy.Decision{
			"fake/agent-model": {Allow: true}, "fake/batch-model": {Allow: true},
		}},
		store:  store,
		budget: &fakeModelBudget{},
	}
	env.receipts = &fakeModelReceipts{}
	env.gateway = NewGateway(env.policy, env.store, env.budget, env.receipts)
	env.responses = make(chan fakeReply, len(script)+8)
	for _, reply := range script {
		env.responses <- reply
	}
	env.endpoint = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var reply fakeReply
		select {
		case reply = <-env.responses:
		default:
			reply = fakeReply{status: http.StatusInternalServerError, body: "script exhausted"}
		}
		if reply.status != 0 && reply.status/100 != 2 {
			http.Error(writer, reply.body, reply.status)
			return
		}
		if len(reply.stream) > 0 {
			writer.Header().Set("Content-Type", "text/event-stream")
			for _, line := range reply.stream {
				fmt.Fprintf(writer, "data: %s\n\n", line)
			}
			fmt.Fprint(writer, "data: [DONE]\n\n")
			return
		}
		fmt.Fprint(writer, reply.body)
	}))
	t.Cleanup(env.endpoint.Close)
	env.registry = provider.NewRegistry()
	if err := env.registry.Register(provider.Config{
		Name: "fake", BaseURL: env.endpoint.URL, APIKey: "test-key-123", TimeoutMs: 2000, MaxAttempts: 1,
	}); err != nil {
		t.Fatalf("register provider: %v", err)
	}
	return env
}

func streamReply(deltas ...string) fakeReply {
	lines := make([]string, 0, len(deltas)+1)
	for _, delta := range deltas {
		encoded, _ := json.Marshal(map[string]any{
			"choices": []map[string]any{{"index": 0, "delta": map[string]any{"content": delta}}},
		})
		lines = append(lines, string(encoded))
	}
	finish, _ := json.Marshal(map[string]any{
		"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
		"usage":   map[string]any{"prompt_tokens": 100, "completion_tokens": 50},
	})
	lines = append(lines, string(finish))
	return fakeReply{stream: lines}
}

func completionReply(content string, input, output int64) fakeReply {
	return fakeReply{body: fmt.Sprintf(`{"id":"c1","choices":[{"index":0,"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":%d,"completion_tokens":%d}}`,
		content, input, output)}
}

func invokeInput() InvokeInput {
	return InvokeInput{
		TenantID: "tenant-1", TaskID: uuid.New(), RunID: uuid.New(), AttemptID: uuid.New(),
		FencingToken: 7, AgentVersionRef: "agent@1", ModelRef: "fake/agent-model",
		IdempotencyKey: "invoke-1",
		Messages:       []provider.Message{{Role: "user", Content: "hello"}},
	}
}

func (in InvokeInput) beginInput() BeginInput {
	return BeginInput{
		TenantID: in.TenantID, TaskID: in.TaskID, RunID: in.RunID, AttemptID: in.AttemptID,
		FencingToken: in.FencingToken, AgentVersionRef: in.AgentVersionRef,
		ModelRef: in.ModelRef, IdempotencyKey: in.IdempotencyKey,
	}
}

func TestInvokerCompleteChainsBeginProviderFinish(t *testing.T) {
	env := newInvokerEnv(t, completionReply("final answer", 100, 50))
	invoker := NewInvoker(env.gateway, env.registry)

	output, err := invoker.Invoke(context.Background(), invokeInput())
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if output.Content != "final answer" {
		t.Fatalf("content = %q", output.Content)
	}
	call := output.Call
	if call.Status != kernelstore.ModelCallCompleted {
		t.Fatalf("call status = %s, want COMPLETED", call.Status)
	}
	if call.UsageCertainty != kernelstore.ModelUsageKnown {
		t.Fatalf("usage certainty = %s, want KNOWN_USAGE", call.UsageCertainty)
	}
	if call.InputTokens != 100 || call.OutputTokens != 50 {
		t.Fatalf("usage = %d/%d, want exact provider-reported 100/50", call.InputTokens, call.OutputTokens)
	}
	if want := money.MicroUSD(600); call.CostMicroUSD != want {
		t.Fatalf("cost = %v, want %v computed from the pinned price table", call.CostMicroUSD, want)
	}
	if call.FinishReason != "stop" {
		t.Fatalf("finish reason = %q", call.FinishReason)
	}
	if got := env.budget.settledTokens; got != 150 {
		t.Fatalf("settled tokens = %d, want exactly 150 (charged once)", got)
	}
	if len(env.receipts.written) != 1 {
		t.Fatalf("audit receipts = %d, want 1 (metadata only)", len(env.receipts.written))
	}
	if strings.Contains(string(env.receipts.written[0].Response), "final answer") {
		t.Fatalf("receipt leaks completion content: %s", env.receipts.written[0].Response)
	}
}

func TestInvokerOmittedOutputLimitUsesBoundedReservation(t *testing.T) {
	env := newInvokerEnv(t, completionReply("bounded", 5, 2))
	invoker := NewInvoker(env.gateway, env.registry)
	input := invokeInput()
	input.MaxOutputTokens = 0

	if _, err := invoker.Invoke(context.Background(), input); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	want := estimateInputTokens(input.Messages, input.Tools, tokens.Heuristic) + defaultMaxOutputTokens
	if got := env.budget.lastReserved.Tokens; got != want {
		t.Fatalf("reserved tokens = %d, want bounded default %d", got, want)
	}
}

func TestInvokerStreamDeliversDeltasAndSettlesExactly(t *testing.T) {
	env := newInvokerEnv(t, streamReply("Hel", "lo ", "world"))
	invoker := NewInvoker(env.gateway, env.registry)

	var deltas []string
	output, err := invoker.InvokeStream(context.Background(), invokeInput(), func(delta string) { deltas = append(deltas, delta) })
	if err != nil {
		t.Fatalf("invoke stream: %v", err)
	}
	if strings.Join(deltas, "") != "Hello world" || output.Content != "Hello world" {
		t.Fatalf("deltas=%v content=%q", deltas, output.Content)
	}
	if got := env.budget.settledTokens; got != 150 {
		t.Fatalf("settled tokens = %d, want exactly 150 after final correction", got)
	}
	if output.Call.Status != kernelstore.ModelCallCompleted {
		t.Fatalf("status = %s", output.Call.Status)
	}
}

func TestInvokerFailsClosedWithoutProvider(t *testing.T) {
	env := newInvokerEnv(t)
	invoker := NewInvoker(env.gateway, nil) // no execution endpoints

	_, err := invoker.Invoke(context.Background(), invokeInput())
	if !errors.Is(err, ErrNoProviderExecution) {
		t.Fatalf("expected ErrNoProviderExecution, got %v", err)
	}
	if env.store.calls != 0 {
		t.Fatalf("no ledger row may open without a provider: %d rows", env.store.calls)
	}
	if env.budget.settledTokens != 0 {
		t.Fatalf("no tokens may settle, got %d", env.budget.settledTokens)
	}
}

func TestInvokerProviderFailureFinishesLedgerFailed(t *testing.T) {
	env := newInvokerEnv(t, fakeReply{status: http.StatusUnauthorized, body: "bad key"})
	invoker := NewInvoker(env.gateway, env.registry)

	output, err := invoker.Invoke(context.Background(), invokeInput())
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("expected provider rejection, got %v", err)
	}
	if env.store.calls != 1 {
		t.Fatalf("ledger rows = %d, want 1", env.store.calls)
	}
	if output.Call.Status != kernelstore.ModelCallFailed {
		t.Fatalf("status = %s, want FAILED", output.Call.Status)
	}
	if output.Call.FinishReason != "provider_rejected" {
		t.Fatalf("finish reason = %q", output.Call.FinishReason)
	}
	if output.Call.UsageCertainty != kernelstore.ModelUsageKnownZero {
		t.Fatalf("usage certainty = %s, want KNOWN_ZERO_USAGE", output.Call.UsageCertainty)
	}
	if env.budget.settledTokens != 0 {
		t.Fatalf("failed call must not bill tokens, got %d", env.budget.settledTokens)
	}
}

func TestInvokerAmbiguousProviderFailureRecordsUnknownUsage(t *testing.T) {
	env := newInvokerEnv(t, fakeReply{body: `{"id":"accepted-but-response-truncated"`})
	invoker := NewInvoker(env.gateway, env.registry)

	output, err := invoker.Invoke(context.Background(), invokeInput())
	if err == nil || !errors.Is(err, provider.ErrProviderUnavailable) {
		t.Fatalf("expected ambiguous provider failure, got %v", err)
	}
	if output.Call.Status != kernelstore.ModelCallFailed {
		t.Fatalf("status = %s, want FAILED", output.Call.Status)
	}
	if output.Call.UsageCertainty != kernelstore.ModelUsageUnknown {
		t.Fatalf("usage certainty = %s, want UNKNOWN_USAGE", output.Call.UsageCertainty)
	}
	if env.budget.settledTokens != 0 {
		t.Fatalf("unknown usage may not be silently settled as zero, got %d", env.budget.settledTokens)
	}
}

func TestInvokerBudgetExhaustedBeforeStart(t *testing.T) {
	env := newInvokerEnv(t, completionReply("never", 1, 1))
	env.budget.exhausted = true
	invoker := NewInvoker(env.gateway, env.registry)

	_, err := invoker.Invoke(context.Background(), invokeInput())
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("expected hard stop, got %v", err)
	}
}

func TestInvokerPolicyDenialBlocksExecution(t *testing.T) {
	env := newInvokerEnv(t, completionReply("never", 1, 1))
	env.policy.decisions = map[string]policy.Decision{} // default deny
	invoker := NewInvoker(env.gateway, env.registry)

	_, err := invoker.Invoke(context.Background(), invokeInput())
	if !errors.Is(err, ErrModelDenied) {
		t.Fatalf("expected policy denial, got %v", err)
	}
	if len(env.responses) != 1 {
		t.Fatalf("provider must not be called after denial: %d scripted replies left", len(env.responses))
	}
}

// TestInvokerResumesCrashedLedgerRow proves the retry convergence property:
// an attempt that crashed after Begin but before Finish leaves the row
// STARTED; the retry replays the same row, executes the provider once more,
// and settles the family exactly once at the final total.
func TestInvokerResumesCrashedLedgerRow(t *testing.T) {
	env := newInvokerEnv(t, completionReply("recovered", 10, 5))
	invoker := NewInvoker(env.gateway, env.registry)
	input := invokeInput()

	crashed, err := env.gateway.Begin(context.Background(), input.beginInput())
	if err != nil {
		t.Fatalf("seed crashed row: %v", err)
	}
	output, err := invoker.Invoke(context.Background(), input)
	if err != nil {
		t.Fatalf("resume invoke: %v", err)
	}
	if output.Call.ID != crashed.Call.ID {
		t.Fatalf("retry must replay the crashed row: %s vs %s", output.Call.ID, crashed.Call.ID)
	}
	if output.Call.Status != kernelstore.ModelCallCompleted {
		t.Fatalf("status = %s, want COMPLETED", output.Call.Status)
	}
	if env.budget.settledTokens != 15 {
		t.Fatalf("settled tokens = %d, want exactly 15 after crash recovery", env.budget.settledTokens)
	}
}

func TestInvokerStreamingDescriptorFallback(t *testing.T) {
	// The descriptor declares no streaming support: a stream request must
	// fall back to the non-streaming wire call.
	env := newInvokerEnv(t, completionReply("plain", 20, 10))
	invoker := NewInvoker(env.gateway, env.registry)
	input := invokeInput()
	input.ModelRef = "fake/batch-model"
	input.Stream = true

	var deltas []string
	output, err := invoker.InvokeStream(context.Background(), input, func(delta string) { deltas = append(deltas, delta) })
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if output.Content != "plain" || len(deltas) != 0 {
		t.Fatalf("fallback content=%q deltas=%v", output.Content, deltas)
	}
}

// TestInvokerStreamBudgetGuardStopsMidStream proves P1-03 acceptance item 3:
// the budget ceiling trips DURING the stream (not at Finish). The reservation
// is far below the first delta's estimate, so the guarded settle exhausts the
// budget, the provider call is cancelled, and the ledger row finishes STOPPED
// with ErrBudgetExhausted — a runaway stream cannot outrun the ceiling.
func TestInvokerStreamBudgetGuardStopsMidStream(t *testing.T) {
	env := newInvokerEnv(t, streamReply(strings.Repeat("token ", 400), " and more", " and yet more"))
	env.budget.reservedTokens = 100 // below one mid-stream settle increment (128)
	invoker := NewInvoker(env.gateway, env.registry)

	_, err := invoker.InvokeStream(context.Background(), invokeInput(), func(string) {})
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("mid-stream guard err = %v, want ErrBudgetExhausted", err)
	}
	if !env.budget.exhausted {
		t.Fatal("ledger must be marked exhausted by the mid-stream settle")
	}
	// The final row settles STOPPED, not COMPLETED: the guard cut the stream.
	stopped := false
	for _, call := range env.store.callsByID {
		if call.Status == kernelstore.ModelCallStopped {
			stopped = true
		}
	}
	if !stopped {
		t.Fatal("no model call finished STOPPED after the mid-stream budget stop")
	}
}

// TestInvokerStreamSettlesWithoutDeltaConsumer proves P1-03 acceptance item 4:
// ledger settlement is driven entirely by the kernel invoker, never by a
// downstream delta consumer. Passing a nil onDelta is the exact seam the MCP
// broker uses today (internal/mcp/broker.go), and it is also what a client
// disconnect looks like from here — the settlement is identical to the
// delta-consuming path (exactly 150, COMPLETED).
func TestInvokerStreamSettlesWithoutDeltaConsumer(t *testing.T) {
	env := newInvokerEnv(t, streamReply("Hel", "lo ", "world"))
	invoker := NewInvoker(env.gateway, env.registry)

	output, err := invoker.InvokeStream(context.Background(), invokeInput(), nil)
	if err != nil {
		t.Fatalf("invoke stream (nil consumer): %v", err)
	}
	if output.Content != "Hello world" {
		t.Fatalf("content = %q, want assembled stream even with no consumer", output.Content)
	}
	if got := env.budget.settledTokens; got != 150 {
		t.Fatalf("settled tokens = %d, want exactly 150 independent of any consumer", got)
	}
	if output.Call.Status != kernelstore.ModelCallCompleted {
		t.Fatalf("status = %s, want COMPLETED", output.Call.Status)
	}
}

// TestInvokerReceiptRecordsResolvedRoute proves P2-10: the model-call audit
// receipt pins the physical deployment identity the logical modelRef resolved
// to (provider + wire model) plus a content-addressed route revision, so a
// call is reproducible against the exact route that served it. Endpoints and
// credentials never appear.
func TestInvokerReceiptRecordsResolvedRoute(t *testing.T) {
	env := newInvokerEnv(t, completionReply("routed", 10, 5))
	invoker := NewInvoker(env.gateway, env.registry)

	if _, err := invoker.Invoke(context.Background(), invokeInput()); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if len(env.receipts.written) != 1 {
		t.Fatalf("receipts = %d, want 1", len(env.receipts.written))
	}
	var receipt struct {
		ModelRef           string `json:"modelRef"`
		RouteRevision      string `json:"routeRevision"`
		ProviderDeployment struct {
			Provider  string `json:"provider"`
			WireModel string `json:"wireModel"`
		} `json:"providerDeployment"`
	}
	if err := json.Unmarshal(env.receipts.written[0].Response, &receipt); err != nil {
		t.Fatalf("decode receipt: %v", err)
	}
	if receipt.ProviderDeployment.Provider != "fake" || receipt.ProviderDeployment.WireModel != "agent-model" {
		t.Fatalf("resolved route = %+v, want provider=fake wireModel=agent-model", receipt.ProviderDeployment)
	}
	if len(receipt.RouteRevision) != 16 {
		t.Fatalf("route revision = %q, want a 16-char content hash", receipt.RouteRevision)
	}
	// The revision is content-addressed: same logical ref + same physical route
	// must reproduce the same revision (a repoint would change it).
	want := sha256.Sum256([]byte(receipt.ModelRef + "|fake|agent-model"))
	if receipt.RouteRevision != hex.EncodeToString(want[:])[:16] {
		t.Fatalf("route revision %q is not the content hash of the resolved route", receipt.RouteRevision)
	}
	if strings.Contains(string(env.receipts.written[0].Response), env.endpoint.URL) ||
		strings.Contains(string(env.receipts.written[0].Response), "test-key-123") {
		t.Fatalf("receipt leaks endpoint or credential: %s", env.receipts.written[0].Response)
	}
}
