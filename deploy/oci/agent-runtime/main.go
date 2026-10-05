// Minimal agent runtime for the OCI/gVisor isolation drill. The container
// reads the workload spec from /fenced/input/workload.json (the task spec,
// which includes the goal + budget + runtime), writes the result to stdout,
// and exits. The OCI worker captures stdout as the attempt result.
//
// Build (static, no CGO):
//
//	CGO_ENABLED=0 GOOS=linux go build -o agent-runtime ./deploy/oci/agent-runtime/
package main

import (
	"encoding/json"
	"os"
)

func main() {
	inputPath := "/fenced/input/workload.json"
	if path := os.Getenv("FENCED_INPUT_PATH"); path != "" {
		inputPath = path
	}

	raw, err := os.ReadFile(inputPath)
	if err != nil {
		// If the input file doesn't exist (pre-created mode), just echo.
		output, _ := json.Marshal(map[string]any{
			"agent": "oci-agent", "goal": os.Getenv("FENCED_AGENT_VERSION_REF"),
			"status": "ok", "message": "OCI/gVisor isolation drill",
		})
		_, _ = os.Stdout.Write(output)
		os.Exit(0)
	}

	// Parse the task spec to extract the goal.
	var spec struct {
		Goal string `json:"goal"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		spec.Goal = string(raw)
	}

	output, _ := json.Marshal(map[string]any{
		"agent": "oci-agent", "goal": spec.Goal,
		"status": "ok", "message": "OCI/gVisor isolation verified",
	})
	_, _ = os.Stdout.Write(output)
}
