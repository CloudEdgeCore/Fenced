package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// runMetrics is the fenced metrics command (Phase 7 observability): fetch
// the aggregated platform metrics from the Control API and print the core
// indicators.
func runMetrics(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("metrics", flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	since := flags.Duration("since", 24*time.Hour, "aggregation window (e.g. 1h, 24h)")
	jsonOutput := flags.Bool("json", false, "emit machine-readable JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}

	client := &http.Client{Timeout: 30 * time.Second}
	target := *endpoint + "/v1/metrics?since=" + url.QueryEscape(time.Now().UTC().Add(-*since).Format(time.RFC3339))
	response, err := client.Get(target)
	if err != nil {
		return fmt.Errorf("fetch metrics: %w", err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("metrics endpoint returned HTTP %d: %.200s", response.StatusCode, encoded)
	}
	var metrics struct {
		WorkflowCount           int     `json:"workflowCount"`
		WorkflowSucceeded       int     `json:"workflowSucceeded"`
		TaskCount               int     `json:"taskCount"`
		TaskSucceeded           int     `json:"taskSucceeded"`
		ModelCalls              int     `json:"modelCalls"`
		ToolCalls               int     `json:"toolCalls"`
		MemoryRecords           int     `json:"memoryRecords"`
		AuditEvents             int     `json:"auditEvents"`
		WorkflowSuccessRate     float64 `json:"workflowSuccessRate"`
		TaskSuccessRate         float64 `json:"taskSuccessRate"`
		SchedulingLatencyMillis struct {
			P50 float64 `json:"p50"`
			P95 float64 `json:"p95"`
			P99 float64 `json:"p99"`
		} `json:"schedulingLatencyMillis"`
		RetryRate             float64 `json:"retryRate"`
		RecoveryRate          float64 `json:"recoveryRate"`
		BudgetDrift           bool    `json:"budgetDrift"`
		CapacityDrift         int     `json:"capacityDrift"`
		DuplicateSideEffects  int     `json:"duplicateSideEffects"`
		CrossTenantViolations int     `json:"crossTenantViolations"`
	}
	if err := json.Unmarshal(encoded, &metrics); err != nil {
		return fmt.Errorf("decode metrics: %w", err)
	}
	if *jsonOutput {
		_, err := fmt.Fprintln(stdout, string(encoded))
		return err
	}
	fmt.Fprintf(stdout, "Workflows             %d (%.0f%% succeeded)\n", metrics.WorkflowCount, metrics.WorkflowSuccessRate*100)
	fmt.Fprintf(stdout, "Tasks                 %d (%.0f%% succeeded)\n", metrics.TaskCount, metrics.TaskSuccessRate*100)
	fmt.Fprintf(stdout, "Model Calls           %d\n", metrics.ModelCalls)
	fmt.Fprintf(stdout, "Tool Calls            %d\n", metrics.ToolCalls)
	fmt.Fprintf(stdout, "Memory Records        %d\n", metrics.MemoryRecords)
	fmt.Fprintf(stdout, "Audit Events          %d\n", metrics.AuditEvents)
	fmt.Fprintf(stdout, "Scheduling P50/P95/P99 %.1f / %.1f / %.1f ms\n",
		metrics.SchedulingLatencyMillis.P50, metrics.SchedulingLatencyMillis.P95, metrics.SchedulingLatencyMillis.P99)
	fmt.Fprintf(stdout, "Retry Rate            %.3f\n", metrics.RetryRate)
	fmt.Fprintf(stdout, "Recovery Rate         %.3f\n", metrics.RecoveryRate)
	fmt.Fprintf(stdout, "Budget Drift          %v\n", metrics.BudgetDrift)
	fmt.Fprintf(stdout, "Capacity Drift        %d\n", metrics.CapacityDrift)
	fmt.Fprintf(stdout, "Duplicate Side Effects %d\n", metrics.DuplicateSideEffects)
	fmt.Fprintf(stdout, "Cross-Tenant Violations %d\n", metrics.CrossTenantViolations)
	return nil
}
