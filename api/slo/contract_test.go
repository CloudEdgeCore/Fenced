package slo_test

import (
	"encoding/json"
	"os"
	"testing"

	platformslo "github.com/CloudEdgeCore/Fenced/internal/platform/slo"
	"github.com/CloudEdgeCore/Fenced/internal/version"
)

func TestStableSLOContractIsComplete(t *testing.T) {
	encoded, err := os.ReadFile("v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Schema         string
		ProductVersion string
		WindowDays     int
		Objectives     map[string]struct {
			Operator string
			Target   float64
		}
	}
	if err := json.Unmarshal(encoded, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.Schema != platformslo.Schema || contract.ProductVersion != version.ProductVersion || contract.WindowDays != 30 || len(contract.Objectives) != 9 {
		t.Fatalf("incomplete SLO contract: %+v", contract)
	}
	for name, objective := range contract.Objectives {
		if objective.Operator == "" {
			t.Fatalf("objective %s has no operator", name)
		}
	}
}
