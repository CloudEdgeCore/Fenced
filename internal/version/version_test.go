package version_test

import (
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/version"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

func TestReleaseVersionIsSingleSourceOfTruth(t *testing.T) {
	if version.ProductVersion != "1.3.0.0" || version.SemVer != "1.3.0" || version.ReleaseStage != "GA" ||
		version.Manifest != agentversion.ManifestAPIVersion || version.RuntimeInterface != agent.ProtocolVersion ||
		version.SyscallABI != "1.0.0" || version.IPCProtocol != "fenced.ipc.v1" ||
		version.ServiceProtocol != "fenced.service.v1" || version.EffectProtocol != "fenced.effect.v1" ||
		version.LegacyRemovalBefore != "2027-02-17" {
		t.Fatalf("release constants drifted: %+v", version.Current())
	}
}
