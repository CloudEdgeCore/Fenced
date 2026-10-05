//go:build integration

package research_test

import (
	"fmt"
	"testing"

	research "github.com/CloudEdgeCore/Fenced/examples/research-workflow/runtime"
)

func TestProbeParseEnvelope(t *testing.T) {
	goal := "FENCED-RESEARCH/v1 {\"role\":\"search\",\"goal\":\"\",\"workflowId\":\"40799ad9-5106-4817-93a1-79eeacb616ac\",\"question\":{\"id\":\"rq-002\",\"question\":\"How do production platforms sandbox untrusted agent workloads?\",\"priority\":2,\"searchQueries\":[\"sandboxing agents gvisor firecracker wasm\"]}}"
	envelope := research.ParseEnvelope(goal, "research-search@1.0.0")
	fmt.Printf("PROBE role=%q workflow=%q question=%v\n", envelope.Role, envelope.Workflow, envelope.Question != nil)
	if envelope.Workflow == "" {
		t.Fatalf("probe: workflow empty")
	}
}
