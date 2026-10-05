package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func ScaffoldAgent(name string) error {
	targetDir := name
	if err := os.MkdirAll(filepath.Join(targetDir, "tools"), 0755); err != nil {
		return err
	}

	// 1. Generate agent.manifest.json
	manifest := map[string]any{
		"apiVersion": "fenced.dev/v1",
		"kind":       "AgentManifest",
		"metadata": map[string]string{
			"name":      name,
			"version":   "1.0.0",
			"namespace": "default",
		},
		"spec": map[string]any{
			"capabilities": map[string]any{
				"tools": []string{
					fmt.Sprintf("%s.action.query@1.0.0", name),
				},
				"models": []string{
					"stealth/space-bunny-alpha",
				},
			},
			"budget": map[string]any{
				"tokens":    20000,
				"costUsd":   1.00,
				"toolCalls": 10,
			},
			"systemPromptRef": "prompt.md",
		},
	}
	mBytes, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(targetDir, "agent.manifest.json"), mBytes, 0644); err != nil {
		return err
	}

	// 2. Generate prompt.md
	promptContent := fmt.Sprintf(`# %s System Prompt

You are an enterprise AI Agent governed by Fenced, responsible for data analysis and operational diagnostics.

## Role & Responsibilities
- Rigorous, professional, and strictly adherent to objective engineering facts.
- Prioritize invoking registered tools for real-time telemetry; do not fabricate assumptions.

## Safety Constraints
1. Hallucination guard: Any field without supporting evidence from tool execution receipts must be marked as "insufficient evidence".
2. Safety fail-closed: High-risk operational steps require explicit warnings and verification steps.

## Output Schema
Format output strictly into the following sections:
1. Problem Summary
2. Data Scope and Baseline
3. Detected Quantitative Anomalies
4. Candidate Root Causes and Confidence
5. Recommended Mitigation Steps
6. Data Sources and References
`, name)
	if err := os.WriteFile(filepath.Join(targetDir, "prompt.md"), []byte(promptContent), 0644); err != nil {
		return err
	}

	// 3. Generate project-level agent.yaml
	agentYamlContent := `version: "1.0"
environment: "development"

llm:
  default_provider: "openrouter"
  providers:
    openrouter:
      model: "stealth/space-bunny-alpha"
      base_url: "https://openrouter.ai/api/v1"
      timeout_sec: 180

kernel:
  budget:
    max_cost_usd: 1.00
    max_tokens: 30000
    max_tool_calls: 15

gateway:
  port: 18080
  host: "127.0.0.1"
`
	if err := os.WriteFile(filepath.Join(targetDir, "agent.yaml"), []byte(agentYamlContent), 0644); err != nil {
		return err
	}

	// 4. Generate tools/main.go
	toolContent := fmt.Sprintf(`package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Example custom tool service
func main() {
	http.HandleFunc("/api/v1/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"output": map[string]any{
				"status": "OK",
				"metric": "sample_telemetry",
				"value":  42.0,
			},
			"receiptId": fmt.Sprintf("rcpt_%%x", time.Now().UnixNano()),
		}
		json.NewEncoder(w).Encode(resp)
	})
	fmt.Printf("[%s Tool Server] listening on :18080...\n", "%s")
	_ = http.ListenAndServe(":18080", nil)
}
`, name, name)
	if err := os.WriteFile(filepath.Join(targetDir, "tools", "main.go"), []byte(toolContent), 0644); err != nil {
		return err
	}

	// 5. Generate README.md
	readmeContent := fmt.Sprintf(`# %s Agent Project

Scaffolded by Fenced CLI (agent init).

## Project Layout
- agent.yaml         : Project-level configuration (cascades over global settings)
- agent.manifest.json: Capability, tool permissions, and budget declaration
- prompt.md          : System prompt and output schema
- tools/             : Custom tool service source code

## Run
$ agent run %s/agent.manifest.json
`, name, name)
	return os.WriteFile(filepath.Join(targetDir, "README.md"), []byte(readmeContent), 0644)
}
