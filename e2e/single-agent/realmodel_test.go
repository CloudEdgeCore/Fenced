//go:build integration

// Real-model smoke: exercises the v1.1 OpenAI-compatible execution layer
// against a live self-hosted endpoint (no PostgreSQL needed). Point it at
// your deployment:
//
//	FENCED_REAL_MODEL_URL=http://host:8080/v1
//	FENCED_REAL_MODEL_NAME=DeepSeek-V4-Flash-w8a8-mtp
//	FENCED_REAL_MODEL_KEY=optional-bearer (unset for keyless vLLM)
//
// Unset variables skip the test.
package e2e_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
)

func TestV11RealModelSmoke(t *testing.T) {
	baseURL := os.Getenv("FENCED_REAL_MODEL_URL")
	modelName := os.Getenv("FENCED_REAL_MODEL_NAME")
	if baseURL == "" || modelName == "" {
		t.Skip("FENCED_REAL_MODEL_URL and FENCED_REAL_MODEL_NAME are not set")
	}
	config := provider.Config{
		Name: "real", BaseURL: baseURL, TimeoutMs: 120000, MaxAttempts: 2,
		BreakerOpens: 5, BreakerCoolMs: 30000,
	}
	if key := os.Getenv("FENCED_REAL_MODEL_KEY"); key != "" {
		config.APIKey = key
	}
	executor := provider.NewExecutor(config, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := executor.Health(ctx); err != nil {
		t.Fatalf("health probe against the real endpoint: %v", err)
	}

	messages := []provider.Message{{Role: "user", Content: "Reply with exactly: fenced real model smoke ok"}}

	// Non-streaming: exact provider-reported usage and request id.
	complete, err := executor.Complete(ctx, provider.Invocation{ModelName: modelName, Messages: messages, MaxOutputTokens: 64})
	if err != nil {
		t.Fatalf("real completion: %v", err)
	}
	if strings.TrimSpace(complete.Content) == "" {
		t.Fatalf("empty completion: %+v", complete)
	}
	if complete.InputTokens <= 0 || complete.OutputTokens <= 0 {
		t.Fatalf("usage not reported: %+v", complete)
	}
	if complete.ProviderRequestID == "" {
		t.Fatalf("provider request id missing: %+v", complete)
	}
	t.Logf("real model (non-stream): usage=%d/%d finish=%s request=%s content=%q",
		complete.InputTokens, complete.OutputTokens, complete.FinishReason, complete.ProviderRequestID, truncate(complete.Content, 80))

	// Streaming: deltas delivered incrementally with exact final usage.
	var deltas []string
	streamed, err := executor.Stream(ctx, provider.Invocation{ModelName: modelName, Messages: messages, MaxOutputTokens: 64, Stream: true}, func(delta string) {
		deltas = append(deltas, delta)
	})
	if err != nil {
		t.Fatalf("real stream: %v", err)
	}
	if len(deltas) == 0 {
		t.Fatal("no deltas were delivered")
	}
	if streamed.InputTokens <= 0 || streamed.OutputTokens <= 0 {
		t.Fatalf("stream usage not reported: %+v", streamed)
	}
	if strings.Join(deltas, "") != streamed.Content {
		t.Fatalf("assembled content diverges from the delta stream")
	}
	t.Logf("real model (stream): deltas=%d usage=%d/%d finish=%s request=%s",
		len(deltas), streamed.InputTokens, streamed.OutputTokens, streamed.FinishReason, streamed.ProviderRequestID)
}

func truncate(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

// TestV11RealAgentAgainstLiveModel runs the full v1.1 governed loop against
// a live self-hosted model: real inference flows through the fenced Model
// Gateway (policy, budget, ledger, audit), the agent writes its run summary
// to memory, and the task completes with the exact live usage settled once.
// Same environment gates as the smoke test plus FENCED_TEST_DATABASE_URL.
func TestV11RealAgentAgainstLiveModel(t *testing.T) {
	baseURL := os.Getenv("FENCED_REAL_MODEL_URL")
	modelName := os.Getenv("FENCED_REAL_MODEL_NAME")
	if baseURL == "" || modelName == "" {
		t.Skip("FENCED_REAL_MODEL_URL and FENCED_REAL_MODEL_NAME are not set")
	}
	if os.Getenv("FENCED_TEST_DATABASE_URL") == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	e2eLiveModel.provider, e2eLiveModel.name, e2eLiveModel.url = "deepseek", modelName, baseURL
	e2eModelRef = "deepseek/" + modelName
	defer func() {
		e2eLiveModel.url = ""
		e2eModelRef = "fake/agent-model"
	}()

	env := newE2EEnv(t, "fenced_e2e_live", 0, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stack := startStack(t, env)
	worker := makeWorker(t, env, stack, "worker-live-1", 30*time.Second)

	publishVersion(ctx, t, env, stack.agentURL)
	task := submitTask(ctx, t, env, "live-real-1",
		"Answer in one sentence: why do production agent workloads need budget enforcement?")
	reconcile(ctx, t, env, poolsFor("worker-live-1"), false)
	go driveWorker(ctx, t, worker, "worker-live-1")

	finished := waitForTerminal(ctx, t, env, task.ID, 3*time.Minute)
	if finished.Phase != "SUCCEEDED" {
		attempts := queryInt(ctx, t, env, `SELECT count(*) FROM attempts a JOIN runs r ON r.id = a.run_id WHERE r.task_id = $1`, task.ID)
		t.Fatalf("live task phase = %s (attempts=%d), want SUCCEEDED", finished.Phase, attempts)
	}

	var calls int
	var usage int64
	var requestID string
	rows, err := env.pool.Query(ctx, `SELECT status, input_tokens + output_tokens, provider_request_id FROM model_calls
		WHERE tenant_id = $1 AND task_id = $2`, e2eTenant, task.ID)
	if err != nil {
		t.Fatalf("read live model calls: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var callUsage int64
		var id string
		if err := rows.Scan(&status, &callUsage, &id); err != nil {
			t.Fatalf("scan live model call: %v", err)
		}
		if status != "COMPLETED" {
			t.Fatalf("live model call status = %s", status)
		}
		calls++
		usage += callUsage
		if id != "" {
			requestID = id
		}
	}
	if calls == 0 || usage <= 0 || requestID == "" {
		t.Fatalf("live model ledger: calls=%d usage=%d requestID=%q", calls, usage, requestID)
	}
	settled := queryInt(ctx, t, env, `SELECT COALESCE(SUM(tokens),0) FROM task_budget_settlements WHERE task_id = $1`, task.ID)
	if settled != usage {
		t.Fatalf("settled tokens = %d, want exactly the live usage %d", settled, usage)
	}
	memories := queryInt(ctx, t, env, `SELECT count(*) FROM memory_records WHERE tenant_id = $1 AND namespace = 'runs'`, e2eTenant)
	if memories != 1 {
		t.Fatalf("memory records = %d, want 1", memories)
	}
	result := readResultArtifact(t, env, finished.ResultRef)
	output, _ := result["output"].(map[string]any)
	if output == nil || strings.TrimSpace(fmt.Sprint(output["answer"])) == "" {
		t.Fatalf("live result output = %#v", result["output"])
	}
	t.Logf("live-model governed loop: modelCalls=%d exactUsage=%d settled=%d answer=%q",
		calls, usage, settled, truncate(fmt.Sprint(output["answer"]), 100))
}

// TestRealModelToolCalling is the P1-04 live acceptance: a real
// OpenAI-compatible model, handed the Fenced tool surface, autonomously emits
// a weather.lookup tool call; the Tool Gateway executes the real webhook, the
// result is re-injected, and the model produces a final answer — the full
// real-model↔real-tool loop the fake provider only simulates. It also proves
// capability filtering (a never-granted tool never reaches the model) and the
// budget/audit chain. Same env gates as the live smoke test plus
// FENCED_TEST_DATABASE_URL; skipped otherwise (nightly / needs-real-run).
func TestRealModelToolCalling(t *testing.T) {
	baseURL := os.Getenv("FENCED_REAL_MODEL_URL")
	modelName := os.Getenv("FENCED_REAL_MODEL_NAME")
	if baseURL == "" || modelName == "" {
		t.Skip("FENCED_REAL_MODEL_URL and FENCED_REAL_MODEL_NAME are not set")
	}
	if os.Getenv("FENCED_TEST_DATABASE_URL") == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	e2eLiveModel.provider, e2eLiveModel.name, e2eLiveModel.url = "deepseek", modelName, baseURL
	e2eModelRef = "deepseek/" + modelName
	e2eExposeLiveTools = true
	defer func() {
		e2eLiveModel.url = ""
		e2eModelRef = "fake/agent-model"
		e2eExposeLiveTools = false
	}()

	env := newE2EEnv(t, "fenced_e2e_livetool", 0, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stack := startStack(t, env)
	worker := makeWorker(t, env, stack, "worker-livetool-1", 30*time.Second)

	publishVersion(ctx, t, env, stack.agentURL)
	// A goal only the tool can satisfy: the model has no live weather feed, so
	// the rational action is to call the granted weather.lookup tool. The
	// system prompt already instructs it to prefer tools when they help.
	task := submitTask(ctx, t, env, "live-tool-1",
		"Report the current weather using the available weather tool; do not guess, call the tool and answer from its result. city:paris")
	reconcile(ctx, t, env, poolsFor("worker-livetool-1"), false)
	go driveWorker(ctx, t, worker, "worker-livetool-1")

	finished := waitForTerminal(ctx, t, env, task.ID, 3*time.Minute)
	if finished.Phase != "SUCCEEDED" {
		attempts := queryInt(ctx, t, env, `SELECT count(*) FROM attempts a JOIN runs r ON r.id = a.run_id WHERE r.task_id = $1`, task.ID)
		t.Fatalf("live tool-calling task phase = %s (attempts=%d), want SUCCEEDED\npython: %s", finished.Phase, attempts, stack.pythonOut.String())
	}

	// The model produced a tool call and the Tool Gateway executed the real
	// webhook: an EXECUTED row can only exist if the arguments passed the
	// registry-authored schema (a malformed or unauthorized call never reaches
	// EXECUTED), so this simultaneously proves "参数 Schema 正确".
	executed := queryInt(ctx, t, env, `SELECT count(*) FROM tool_calls
		WHERE task_id = $1 AND tool_name = 'weather.lookup' AND status = 'EXECUTED'`, task.ID)
	if executed < 1 {
		t.Fatalf("executed weather.lookup calls = %d, want >= 1 (the live model did not call the tool)\npython: %s", executed, stack.pythonOut.String())
	}
	// Capability enforcement: the never-granted payments.charge tool was
	// neither offered to the model nor executed. No call outside the granted
	// surface may exist in any status.
	foreign := queryInt(ctx, t, env, `SELECT count(*) FROM tool_calls
		WHERE task_id = $1 AND tool_name <> 'weather.lookup'`, task.ID)
	if foreign != 0 {
		t.Fatalf("tool calls outside the granted surface = %d, want 0 (capability filtering leaked)", foreign)
	}

	// Model ledger: at least two completed invocations — the turn that emitted
	// the tool call and the turn that answered after the tool result was
	// re-injected — each with real usage and a provider request id.
	var calls int
	var usage int64
	requestIDs := map[string]struct{}{}
	rows, err := env.pool.Query(ctx, `SELECT status, input_tokens + output_tokens, provider_request_id FROM model_calls
		WHERE tenant_id = $1 AND task_id = $2`, e2eTenant, task.ID)
	if err != nil {
		t.Fatalf("read live model calls: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var callUsage int64
		var id string
		if err := rows.Scan(&status, &callUsage, &id); err != nil {
			t.Fatalf("scan live model call: %v", err)
		}
		if status != "COMPLETED" {
			t.Fatalf("live model call status = %s, want COMPLETED", status)
		}
		calls++
		usage += callUsage
		if id != "" {
			requestIDs[id] = struct{}{}
		}
	}
	if calls < 2 {
		t.Fatalf("completed model calls = %d, want >= 2 (tool-call turn + post-result answer turn)", calls)
	}
	if usage <= 0 || len(requestIDs) == 0 {
		t.Fatalf("live model ledger: usage=%d distinctRequestIDs=%d", usage, len(requestIDs))
	}

	// Budget/Audit chain: the task settled exactly the model usage, the tool
	// calls settled to the executed count, and both model and tool receipts
	// exist (metadata only).
	settledTokens := queryInt(ctx, t, env, `SELECT COALESCE(SUM(tokens),0) FROM task_budget_settlements WHERE task_id = $1`, task.ID)
	if settledTokens != usage {
		t.Fatalf("settled tokens = %d, want exactly the live usage %d", settledTokens, usage)
	}
	settledToolCalls := queryInt(ctx, t, env, `SELECT COALESCE(SUM(tool_calls),0) FROM task_budget_settlements WHERE task_id = $1`, task.ID)
	if settledToolCalls != executed {
		t.Fatalf("settled tool calls = %d, want the executed count %d", settledToolCalls, executed)
	}
	modelReceipts := queryInt(ctx, t, env, `SELECT count(*) FROM runtime_operation_receipts r
		JOIN attempts a ON a.id = r.attempt_id JOIN runs rn ON rn.id = a.run_id
		WHERE rn.task_id = $1 AND r.operation LIKE 'MODEL:%'`, task.ID)
	toolReceipts := queryInt(ctx, t, env, `SELECT count(*) FROM runtime_operation_receipts r
		JOIN attempts a ON a.id = r.attempt_id JOIN runs rn ON rn.id = a.run_id
		WHERE rn.task_id = $1 AND r.operation = 'TOOL:weather.lookup@1.0.0'`, task.ID)
	if modelReceipts < 2 || int64(toolReceipts) < executed {
		t.Fatalf("audit receipts: model=%d (want >= 2) tool=%d (want >= executed %d)", modelReceipts, toolReceipts, executed)
	}

	// The run summary landed in the tenant memory store, and the final answer
	// is non-empty (the model reasoned over the re-injected tool result).
	memories := queryInt(ctx, t, env, `SELECT count(*) FROM memory_records WHERE tenant_id = $1 AND namespace = 'runs'`, e2eTenant)
	if memories != 1 {
		t.Fatalf("memory records = %d, want 1", memories)
	}
	result := readResultArtifact(t, env, finished.ResultRef)
	output, _ := result["output"].(map[string]any)
	if output == nil || strings.TrimSpace(fmt.Sprint(output["answer"])) == "" {
		t.Fatalf("live tool-calling result output = %#v", result["output"])
	}
	if len(asList(output["toolCalls"])) < 1 {
		t.Fatalf("result toolCalls = %#v, want >= 1", output["toolCalls"])
	}

	// Credential isolation (only when the live endpoint uses a bearer key): the
	// provider key must never reach an agent-visible surface.
	if key := os.Getenv("FENCED_REAL_MODEL_KEY"); key != "" {
		if strings.Contains(resultDocument(t, env, finished), key) {
			t.Fatal("result document leaks the provider credential")
		}
		if strings.Contains(stack.pythonOut.String(), key) {
			t.Fatal("python agent stderr leaks the provider credential")
		}
	}

	t.Logf("live-model tool-calling loop: modelCalls=%d executedTools=%d exactUsage=%d settled=%d answer=%q",
		calls, executed, usage, settledTokens, truncate(fmt.Sprint(output["answer"]), 100))
}
