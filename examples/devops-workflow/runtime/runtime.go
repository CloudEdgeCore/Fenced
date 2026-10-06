// Package devops implements the six agent roles of the DevOps reference
// workload as one Fenced-native runtime. Every role executes inside the
// fenced attempt context: tools are invoked through the brokered
// fenced.tool.invoke, models through fenced.model.invoke, and memory
// through fenced.memory.put/search.
package devops

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// MCPClient mirrors the research runtime's MCP interface.
type MCPClient interface {
	CallTool(ctx context.Context, executionID, name string, args any) (json.RawMessage, error)
}

// Models binds the logical model tiers.
type Models struct {
	Fast      string
	Reasoning string
}

// ChatMessage mirrors the brokered message shape.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Envelope is the structured task payload every role finds inside its goal.
type Envelope struct {
	Role       string `json:"role"`
	Goal       string `json:"goal"`
	WorkflowID string `json:"workflowId"`
}

const envelopePrefix = "FENCED-DEVOPS/v1 "

// EnvelopePrefix exposes the goal envelope marker.
func EnvelopePrefix() string { return envelopePrefix }

// ParseEnvelope decodes the structured payload from a goal.
func ParseEnvelope(goal, versionRef string) Envelope {
	if payload, ok := strings.CutPrefix(goal, envelopePrefix); ok {
		var envelope Envelope
		decoder := json.NewDecoder(strings.NewReader(payload))
		if err := decoder.Decode(&envelope); err == nil && envelope.Role != "" {
			return envelope
		}
	}
	return Envelope{Role: RoleFromRef(versionRef), Goal: goal}
}

// RoleFromRef maps an agent version ref to its role name.
func RoleFromRef(versionRef string) string {
	name, _, _ := strings.Cut(versionRef, "@")
	name = strings.TrimPrefix(name, "devops-")
	switch name {
	case "planner", "observer", "diagnoser", "executor", "verifier", "rollback":
		return name
	}
	return ""
}

// Deps is the per-invocation context.
type Deps struct {
	MCP         MCPClient
	Models      Models
	Workdir     func(string) string
	ExecutionID string
}

// Run dispatches one task invocation to the role handler.
func Run(ctx context.Context, deps Deps, versionRef, goal string) (json.RawMessage, error) {
	envelope := ParseEnvelope(goal, versionRef)
	deps.Workdir = func(leaf string) string {
		if leaf == "" {
			return "devops/" + envelope.WorkflowID
		}
		return "devops/" + envelope.WorkflowID + "/" + leaf
	}
	switch envelope.Role {
	case "planner":
		return runPlanner(ctx, deps, envelope)
	case "observer":
		return runObserver(ctx, deps, envelope)
	case "diagnoser":
		return runDiagnoser(ctx, deps, envelope)
	case "executor":
		return runExecutor(ctx, deps, envelope)
	case "verifier":
		return runVerifier(ctx, deps, envelope)
	case "rollback":
		return runRollback(ctx, deps, envelope)
	}
	return nil, fmt.Errorf("unknown devops role %q (goal=%.120q)", envelope.Role, goal)
}

// -- roles ------------------------------------------------------------------

func runPlanner(ctx context.Context, deps Deps, envelope Envelope) (json.RawMessage, error) {
	output := map[string]any{
		"service": "checkout",
		"checks":  []string{"pod status", "logs", "error rate"},
	}
	encoded, _ := json.Marshal(output)
	if err := putMemory(ctx, deps, "plan", "application/json", output); err != nil {
		return nil, err
	}
	return encoded, nil
}

func runObserver(ctx context.Context, deps Deps, envelope Envelope) (json.RawMessage, error) {
	// Call kubernetes.get to inspect the service.
	getResult, err := callTool(ctx, deps, "kubernetes.get@1.0.0", map[string]any{
		"namespace": "default", "service": "checkout",
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetes.get: %w", err)
	}
	logsResult, err := callTool(ctx, deps, "kubernetes.logs@1.0.0", map[string]any{
		"namespace": "default", "service": "checkout", "lines": 10,
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetes.logs: %w", err)
	}
	observation := map[string]any{
		"service": "checkout",
		"status":  "degraded",
		"get":     getResult,
		"logs":    logsResult,
		"findings": []string{
			"checkout-2 is OOMKilled",
			"readyReplicas=2 vs replicas=3",
			"error_rate is elevated",
		},
	}
	if err := putMemory(ctx, deps, "observation", "application/json", observation); err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(observation)
	return encoded, nil
}

func runDiagnoser(ctx context.Context, deps Deps, envelope Envelope) (json.RawMessage, error) {
	// Diagnose from the observation and produce a fix plan.
	diagnosis := map[string]any{
		"service":   "checkout",
		"rootCause": "pod checkout-2 killed by OOM (memory limit exceeded)",
		"severity":  "HIGH",
		"fixPlan":   "restart pod checkout-2 to clear the memory pressure",
		"fixAction": "restart",
	}
	if err := putMemory(ctx, deps, "diagnosis", "application/json", diagnosis); err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(diagnosis)
	return encoded, nil
}

func runExecutor(ctx context.Context, deps Deps, envelope Envelope) (json.RawMessage, error) {
	// Execute the fix: restart the unhealthy service. This is the high-risk
	// step that requires workflow-level approval.
	result, err := callTool(ctx, deps, "kubernetes.restart@1.0.0", map[string]any{
		"namespace": "default", "service": "checkout",
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetes.restart: %w", err)
	}
	receipt := map[string]any{
		"action":     "restart",
		"service":    "checkout",
		"result":     result,
		"sideEffect": "restart applied",
		"audit":      "operator: devops-executor, change: restart checkout",
	}
	if err := putMemory(ctx, deps, "execution", "application/json", receipt); err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(receipt)
	return encoded, nil
}

func runVerifier(ctx context.Context, deps Deps, envelope Envelope) (json.RawMessage, error) {
	// Verify the fix by inspecting the service again.
	getResult, err := callTool(ctx, deps, "kubernetes.get@1.0.0", map[string]any{
		"namespace": "default", "service": "checkout",
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetes.get: %v", err)
	}
	// The cluster's state determines whether the fix healed.
	state, _ := getResult["state"].(map[string]any)
	healthy := false
	if h, ok := state["healthy"]; ok {
		healthy = h == true || h == "true"
	}
	var verdict string
	if healthy {
		verdict = "HEALTHY"
	} else {
		verdict = "ROLLBACK_REQUIRED"
	}
	verification := map[string]any{
		"service": "checkout",
		"healthy": healthy,
		"verdict": verdict,
		"state":   state,
	}
	if err := putMemory(ctx, deps, "verification", "application/json", verification); err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(verification)
	return encoded, nil
}

func runRollback(ctx context.Context, deps Deps, envelope Envelope) (json.RawMessage, error) {
	// Rollback: scale the service back to the original replicas.
	result, err := callTool(ctx, deps, "kubernetes.scale@1.0.0", map[string]any{
		"namespace": "default", "service": "checkout", "replicas": 3,
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetes.scale: %w", err)
	}
	rollback := map[string]any{
		"action":  "scale",
		"service": "checkout",
		"result":  result,
		"status":  "ROLLED_BACK",
	}
	if err := putMemory(ctx, deps, "rollback", "application/json", rollback); err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(rollback)
	return encoded, nil
}

// -- helpers ----------------------------------------------------------------

func callTool(ctx context.Context, deps Deps, name string, args any) (map[string]any, error) {
	raw, err := deps.MCP.CallTool(ctx, deps.ExecutionID, name, args)
	if err != nil {
		return nil, fmt.Errorf("tool %s: %w", name, err)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode tool %s result: %w", name, err)
	}
	return result, nil
}

func putMemory(ctx context.Context, deps Deps, key, contentType string, value any) error {
	content, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode memory: %w", err)
	}
	_, err = deps.MCP.CallTool(ctx, deps.ExecutionID, "fenced.memory.put", map[string]any{
		"namespace": deps.Workdir(""), "key": key, "contentType": contentType, "content": string(content),
	})
	return err
}
