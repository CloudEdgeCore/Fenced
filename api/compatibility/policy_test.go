package compatibility_test

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

func TestV1PromotionPolicyIsMachineComplete(t *testing.T) {
	encoded, err := os.ReadFile("v1alpha1-to-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Kind    string
		Release struct {
			ProductVersion string
			SemVer         string
			Stage          string
		}
		Current struct {
			AgentManifest    string
			RuntimeInterface string
		}
		Legacy struct {
			AgentManifest    string
			RuntimeInterface string
			RemoveNotBefore  string
			ReadCompatible   bool
		}
		Policy struct {
			MinimumDeprecationDays         int
			NMinusOneReadCompatibility     bool
			StableChangesRequireNewVersion bool
			UnknownFieldsFailClosed        bool
		}
		FrozenRuntimeLifecycle []string
		PromotionGates         []string
	}
	if err := json.Unmarshal(encoded, &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Kind != "CompatibilityPromotion" ||
		policy.Current.AgentManifest != agentversion.ManifestAPIVersion ||
		policy.Current.RuntimeInterface != agent.ProtocolVersion ||
		agentversion.RuntimeInterfaceV1 != agent.ProtocolVersion ||
		policy.Legacy.AgentManifest != agentversion.LegacyManifestAPIVersion ||
		policy.Legacy.RuntimeInterface != agent.LegacyProtocolVersion || !policy.Legacy.ReadCompatible {
		t.Fatalf("policy drifted from code constants: %+v", policy)
	}
	if policy.Release.ProductVersion != "1.0.0.0" || policy.Release.SemVer != "1.0.0" || policy.Release.Stage != "GA" ||
		policy.Legacy.RemoveNotBefore != "2027-02-17" {
		t.Fatalf("release identity is incomplete: %+v", policy)
	}
	if policy.Policy.MinimumDeprecationDays < 180 || !policy.Policy.NMinusOneReadCompatibility ||
		!policy.Policy.StableChangesRequireNewVersion || !policy.Policy.UnknownFieldsFailClosed {
		t.Fatalf("unsafe promotion policy: %+v", policy.Policy)
	}
	for _, lifecycle := range []string{"health", "start", "stop", "event", "result", "checkpoint", "restore"} {
		if !slices.Contains(policy.FrozenRuntimeLifecycle, lifecycle) {
			t.Fatalf("runtime lifecycle %q is not frozen", lifecycle)
		}
	}
	if len(policy.PromotionGates) < 7 {
		t.Fatalf("promotion gates are incomplete: %v", policy.PromotionGates)
	}
}
