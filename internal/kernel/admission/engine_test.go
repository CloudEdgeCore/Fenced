package admission

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func TestEngineReturnsMachineReadableReasons(t *testing.T) {
	engine := New(testLimits())
	engine.now = func() time.Time { return time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC) }
	task := store.Task{Spec: json.RawMessage(`{
		"priority": 101,
		"deadline": "2026-08-13T11:00:00Z",
		"budget": {"tokens": 2000, "costUsd": 2, "toolCalls": 10, "wallSeconds": 60},
		"placement": {"runtimeClasses": ["microvm"], "region": "", "cpuMillis": 100, "memoryMiB": 128, "llmConcurrency": 1}
	}`)}
	decision := engine.Evaluate(task)
	if decision.Admit {
		t.Fatal("invalid task was admitted")
	}
	want := []string{"PRIORITY_OUT_OF_RANGE", "DEADLINE_EXPIRED", "RUNTIME_CLASS_DENIED", "REGION_REQUIRED", "BUDGET_TOKENS_INVALID"}
	for i, code := range want {
		if len(decision.Reasons) <= i || decision.Reasons[i].Code != code {
			t.Fatalf("reason[%d] = %+v, want %s", i, decision.Reasons, code)
		}
	}
	if decision.ReasonCode != want[0] {
		t.Fatalf("summary reason = %q, want %q", decision.ReasonCode, want[0])
	}
}

func TestEngineAdmitsBoundedWorkload(t *testing.T) {
	engine := New(testLimits())
	engine.now = func() time.Time { return time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC) }
	task := store.Task{Spec: json.RawMessage(`{
		"priority": 70,
		"deadline": "2026-08-14T12:00:00Z",
		"budget": {"tokens": 500, "costUsd": 2, "toolCalls": 10, "wallSeconds": 60},
		"placement": {"runtimeClasses": ["oci"], "preferredClass": "oci", "region": "cn-east", "cpuMillis": 100, "memoryMiB": 128, "llmConcurrency": 1}
	}`)}
	decision := engine.Evaluate(task)
	if !decision.Admit || decision.ReasonCode != "ADMISSION_PASSED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func testLimits() Limits {
	return Limits{
		RuntimeClasses: []string{"oci", "wasm"}, MaxTokens: 1000, MaxCostMicroUSD: money.MustFromUSD(10),
		MaxToolCalls: 100, MaxWallSeconds: 3600, MaxCPU: 2000, MaxMemory: 4096,
		MaxLLMConcurrency: 4,
	}
}
