package workflow

import (
	"encoding/json"
	"testing"
)

func FuzzDecodeWorkflowSpec(f *testing.F) {
	f.Add([]byte(`{"apiVersion":"fenced.dev/workflow/v1","kind":"Workflow","steps":[]}`))
	f.Add([]byte(`{"apiVersion":"fenced.dev/workflow/v1","kind":"Workflow","steps":[{"name":"a","agentVersionRef":"a@1.0.0","goal":"go"}]}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		spec, err := DecodeWorkflowSpec(raw)
		if err != nil {
			return
		}
		canonical, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		if _, replayErr := DecodeWorkflowSpec(canonical); replayErr != nil {
			t.Fatalf("accepted workflow did not round trip: %v", replayErr)
		}
	})
}
