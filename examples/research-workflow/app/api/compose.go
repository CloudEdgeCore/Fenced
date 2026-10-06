package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	research "github.com/CloudEdgeCore/Fenced/examples/research-workflow/runtime"
)

// ComposeWorkflowDocument renders the template with the concrete goal,
// deadline, workflow id, user budget override, and per-role/round envelopes.
// It mirrors the e2e test helpers' substitution pattern (helpers_test.go
// createResearch) at the object level so envelope strings stay properly
// JSON-escaped. The caller must validate the output with
// workflowkernel.DecodeWorkflowSpec before submission to the Control API.
func ComposeWorkflowDocument(template []byte, goal string, workflowID string, deadline time.Time, budgetOverride *BudgetOverride) ([]byte, error) {
	envelope := func(role string, round int) string {
		payload, _ := json.Marshal(map[string]any{
			"role": role, "goal": goal, "workflowId": workflowID, "round": round,
		})
		return research.EnvelopePrefix() + string(payload)
	}
	var document map[string]any
	if err := json.Unmarshal(template, &document); err != nil {
		return nil, fmt.Errorf("workflow template: %w", err)
	}
	document["deadline"] = deadline.UTC().Format(time.RFC3339)

	goalPlaceholders := map[string]string{
		"__ENVELOPE_PLANNER__":      envelope("planner", 0),
		"__ENVELOPE_ANALYST_R1__":   envelope("analyst", 1),
		"__ENVELOPE_CRITIC_R1__":    envelope("critic", 1),
		"__ENVELOPE_COLLECTOR_R2__": envelope("collector", 2),
		"__ENVELOPE_ANALYST_R2__":   envelope("analyst", 2),
		"__ENVELOPE_CRITIC_R2__":    envelope("critic", 2),
		"__ENVELOPE_COLLECTOR_R3__": envelope("collector", 3),
		"__ENVELOPE_ANALYST_R3__":   envelope("analyst", 3),
		"__ENVELOPE_CRITIC_R3__":    envelope("critic", 3),
		"__ENVELOPE_WRITER__":       envelope("writer", 3),
		"__ENVELOPE_VALIDATOR__":    envelope("validator", 3),
	}

	rawSteps, ok := document["steps"].([]any)
	if !ok {
		return nil, fmt.Errorf("workflow template has no steps array")
	}
	for _, rawStep := range rawSteps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if placeholder, ok := step["goal"].(string); ok {
			if envelopeValue, matched := goalPlaceholders[placeholder]; matched {
				step["goal"] = envelopeValue
			}
		}
	}

	// Apply user budget override when the caller supplied non-zero values.
	if budgetOverride != nil {
		budgetObj, ok := document["budget"].(map[string]any)
		if !ok {
			budgetObj = map[string]any{}
			document["budget"] = budgetObj
		}
		if budgetOverride.MaxTasks > 0 {
			budgetObj["maxTasks"] = budgetOverride.MaxTasks
		}
		if budgetOverride.MaxTokens > 0 {
			budgetObj["maxTokens"] = budgetOverride.MaxTokens
		}
	}

	rendered, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("marshal composed workflow: %w", err)
	}
	check := string(rendered)
	for _, placeholder := range []string{"__ENVELOPE_", "__DEADLINE__", "__MODEL_FAST__", "__MODEL_READER__", "__MODEL_REASONING__"} {
		if strings.Contains(check, placeholder) {
			return nil, fmt.Errorf("unresolved placeholder %q in composed workflow", placeholder)
		}
	}
	return rendered, nil
}
