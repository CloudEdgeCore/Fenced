//go:build integration

package devops_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestScale100kTasks is the §Phase-8 scale gate: 100,000 plain tasks
// (tunable via FENCED_E2E_RESEARCH_RUNS) all reach a terminal state through
// the kernel admission → scheduling → runtime pipeline with zero failures.
// Gated by FENCED_RESEARCH_SCALE_100K=1.
func TestScale100kTasks(t *testing.T) {
	if os.Getenv("FENCED_RESEARCH_SCALE_100K") != "1" {
		t.Skip("FENCED_RESEARCH_SCALE_100K is not set")
	}
	count := 100000
	if value := os.Getenv("FENCED_E2E_RESEARCH_RUNS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			t.Fatalf("FENCED_E2E_RESEARCH_RUNS must be a positive integer, got %q", value)
		}
		count = parsed
	}
	h := newHarness(t, "scale100k", false)
	ctx := context.Background()

	// Create N plain third-party tasks (no workflow overhead).
	startedAt := time.Now()
	for index := 0; index < count; index++ {
		if _, err := h.store.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: uuid.New(), TenantID: devopsTenant, Namespace: "default",
			AgentVersionRef: "hello-agent@1.0.0", Goal: fmt.Sprintf("scale task %d", index),
			Spec: []byte(`{"priority":50,"budget":{"tokens":2000,"costUsd":0.10,"toolCalls":8,"wallSeconds":120},
				"placement":{"runtimeClasses":["research-network"],"preferredClass":"research-network",
					"region":"cn-east","cpuMillis":250,"memoryMiB":256,"workspaceBytes":8388608,"llmConcurrency":2},
				"retryPolicy":{"maxAttempts":3}}`),
			IdempotencyKey: fmt.Sprintf("scale/%d", index),
		}); err != nil {
			t.Fatalf("create task %d: %v", index, err)
		}
		if index > 0 && index%20000 == 0 {
			t.Logf("created %d/%d tasks in %s", index, count, time.Since(startedAt).Round(time.Second))
		}
	}
	createDuration := time.Since(startedAt)
	t.Logf("created %d tasks in %s; draining", count, createDuration.Round(time.Second))

	// Drain: wait until every task reaches a terminal phase.
	deadline := time.Now().Add(60 * time.Minute)
	var pending int
	for {
		if err := h.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM tasks WHERE tenant_id = $1 AND phase NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT','REJECTED')`,
			devopsTenant).Scan(&pending); err != nil {
			t.Fatalf("count pending: %v", err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scale-100k: %d tasks still pending after %s", pending, time.Since(startedAt).Round(time.Second))
		}
		time.Sleep(5 * time.Second)
	}
	total := time.Since(startedAt)

	var succeeded, failed int
	if err := h.pool.QueryRow(ctx,
		`SELECT
			(SELECT COUNT(*) FROM tasks WHERE tenant_id = $1 AND phase = 'SUCCEEDED'),
			(SELECT COUNT(*) FROM tasks WHERE tenant_id = $1 AND phase IN ('FAILED','CANCELLED','TIMED_OUT','REJECTED'))`,
		devopsTenant).Scan(&succeeded, &failed); err != nil {
		t.Fatalf("count outcomes: %v", err)
	}
	if failed != 0 {
		t.Fatalf("scale-100k: %d of %d tasks failed", failed, count)
	}
	if succeeded != count {
		t.Fatalf("scale-100k: %d succeeded, want %d", succeeded, count)
	}
	t.Logf("scale-100k: %d tasks succeeded in %s (create %s, drain %s)",
		count, total.Round(time.Second), createDuration.Round(time.Second), (total - createDuration).Round(time.Second))
}

// TestMetricsEndpoint proves the Control API observability surface: after a
// real execution, GET /v1/metrics returns the aggregated core metrics.
func TestMetricsEndpoint(t *testing.T) {
	h := newHarness(t, "obs-endpoint", false)
	id, err := h.createWorkflow("Metrics endpoint: checkout incident")
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	waitForApproval(t, h, id)
	h.approveStep(id, "execute", true)
	h.requireCompleted(id, settleTimeout+3*time.Minute)

	response, err := http.Get(h.controlURL + "/v1/metrics?since=" + time.Now().Add(-time.Hour).UTC().Format("2006-01-02T15:04:05Z"))
	if err != nil {
		t.Fatalf("GET /v1/metrics: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("GET /v1/metrics status = %d: %s", response.StatusCode, body)
	}
	var metrics struct {
		WorkflowCount         int     `json:"workflowCount"`
		WorkflowSuccessRate   float64 `json:"workflowSuccessRate"`
		ToolCalls             int     `json:"toolCalls"`
		BudgetDrift           bool    `json:"budgetDrift"`
		CrossTenantViolations int     `json:"crossTenantViolations"`
	}
	if err := json.NewDecoder(response.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if metrics.WorkflowCount < 1 {
		t.Fatalf("metrics workflowCount = %d, want >= 1", metrics.WorkflowCount)
	}
	if metrics.WorkflowSuccessRate != 1.0 {
		t.Fatalf("metrics workflowSuccessRate = %v, want 1.0", metrics.WorkflowSuccessRate)
	}
	if metrics.ToolCalls < 1 {
		t.Fatalf("metrics toolCalls = %d, want >= 1", metrics.ToolCalls)
	}
	if metrics.BudgetDrift || metrics.CrossTenantViolations != 0 {
		t.Fatalf("metrics drift/violations: budgetDrift=%v crossTenant=%d", metrics.BudgetDrift, metrics.CrossTenantViolations)
	}
	t.Logf("metrics endpoint OK: workflows=%d success=%.0f%% toolCalls=%d",
		metrics.WorkflowCount, metrics.WorkflowSuccessRate*100, metrics.ToolCalls)
}
