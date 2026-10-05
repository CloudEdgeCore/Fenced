package agentversion

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
)

func validManifest() Manifest {
	return Manifest{
		APIVersion: ManifestAPIVersion,
		Kind:       ManifestKind,
		Metadata: Metadata{
			Name: "research-agent", Version: "1.0.0", Namespace: "default",
		},
		Spec: Spec{
			RuntimeClassPolicy: RuntimeClassPolicy{Allowed: []string{"oci"}, Preferred: "oci"},
			Runtimes: []RuntimeTarget{{
				Class: "oci", Interface: RuntimeInterfaceV1,
				RuntimeABI: "fenced.oci/v1", Entrypoint: []string{"/agent/bin/research", "serve"},
			}},
			Capabilities: &Capabilities{
				Tools: []string{"search@v1"}, Models: []string{"quality"},
				Memory: []string{"project/*"}, Secrets: []string{},
			},
			Resources:  &ResourceLimits{CPUMillis: 500, MemoryMiB: 512, WorkspaceBytes: 64 << 20},
			Budget:     &Budget{Tokens: 100_000, CostMicroUSD: money.MustFromUSD(10), ToolCalls: 100, WallSeconds: 3600},
			Checkpoint: &CheckpointPolicy{Mode: CheckpointLogical, SchemaVersion: "research-state/v1", IntervalSeconds: 30},
			Lifecycle:  Lifecycle{MaxAttempts: 3},
			Image: &Image{
				Ref:    "registry.example/research-agent",
				Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
		},
	}
}

func TestDecodeManifestCanonicalRoundTrip(t *testing.T) {
	raw, err := json.MarshalIndent(validManifest(), "", "  ")
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	manifest, canonical, digest, err := DecodeManifest(raw)
	if err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.Ref() != "research-agent@1.0.0" {
		t.Fatalf("manifest ref = %q", manifest.Ref())
	}
	_, replay, replayDigest, err := DecodeManifest(canonical)
	if err != nil {
		t.Fatalf("decode canonical manifest: %v", err)
	}
	if string(canonical) != string(replay) || digest != replayDigest {
		t.Fatalf("canonical replay changed: %s vs %s", canonical, replay)
	}
}

func TestLegacyAgentOSIdentifiersAreNormalizedOnDecode(t *testing.T) {
	legacy := validManifest()
	legacy.APIVersion = "agentos.dev/v1"
	legacy.Spec.Runtimes[0].Interface = "agentos.runtime.interface/v1"
	legacy.Spec.Runtimes[0].RuntimeABI = "agentos.oci/v1"

	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy manifest: %v", err)
	}
	decoded, canonical, _, err := DecodeManifest(encoded)
	if err != nil {
		t.Fatalf("a manifest published under the former name must remain readable: %v", err)
	}
	if decoded.APIVersion != ManifestAPIVersion {
		t.Fatalf("apiVersion = %q, want %q", decoded.APIVersion, ManifestAPIVersion)
	}
	if got := decoded.Spec.Runtimes[0].Interface; got != RuntimeInterfaceV1 {
		t.Fatalf("interface = %q, want %q", got, RuntimeInterfaceV1)
	}
	if got := decoded.Spec.Runtimes[0].RuntimeABI; got != "fenced.oci/v1" {
		t.Fatalf("runtimeABI = %q, want %q", got, "fenced.oci/v1")
	}
	if strings.Contains(string(canonical), "agentos") {
		t.Fatalf("canonical manifest still carries a legacy identifier: %s", canonical)
	}
}

func TestDecodeManifestRejectsAmbiguousAndUnknownJSON(t *testing.T) {
	raw, err := json.Marshal(validManifest())
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	duplicate := strings.Replace(string(raw), `"kind":"AgentManifest"`, `"kind":"AgentManifest","kind":"AgentManifest"`, 1)
	unknown := strings.Replace(string(raw), `"metadata":{`, `"unexpected":true,"metadata":{`, 1)
	for name, encoded := range map[string]string{
		"duplicate key":  duplicate,
		"unknown field":  unknown,
		"trailing value": string(raw) + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := DecodeManifest([]byte(encoded)); err == nil {
				t.Fatalf("DecodeManifest unexpectedly accepted %s", encoded)
			}
		})
	}
}

func TestManifestRequiresExplicitDefaultDenyAndLimits(t *testing.T) {
	for name, mutate := range map[string]func(*Manifest){
		"capabilities": func(manifest *Manifest) { manifest.Spec.Capabilities = nil },
		"resources":    func(manifest *Manifest) { manifest.Spec.Resources = nil },
		"budget":       func(manifest *Manifest) { manifest.Spec.Budget = nil },
		"checkpoint":   func(manifest *Manifest) { manifest.Spec.Checkpoint = nil },
	} {
		t.Run(name, func(t *testing.T) {
			manifest := validManifest()
			mutate(&manifest)
			if err := manifest.Validate(); err == nil {
				t.Fatalf("manifest without %s was accepted", name)
			}
		})
	}
}

func TestManifestRuntimeAndCapabilityValidation(t *testing.T) {
	for name, mutate := range map[string]func(*Manifest){
		"runtime class mismatch": func(manifest *Manifest) {
			manifest.Spec.RuntimeClassPolicy.Allowed = []string{"wasmtime"}
		},
		"duplicate runtime": func(manifest *Manifest) {
			manifest.Spec.Runtimes = append(manifest.Spec.Runtimes, manifest.Spec.Runtimes[0])
			manifest.Spec.RuntimeClassPolicy.Allowed = []string{"oci", "oci"}
		},
		"wrong interface": func(manifest *Manifest) {
			manifest.Spec.Runtimes[0].Interface = "fenced.runtime/v2"
		},
		"empty entrypoint": func(manifest *Manifest) {
			manifest.Spec.Runtimes[0].Entrypoint = nil
		},
		"duplicate capability": func(manifest *Manifest) {
			manifest.Spec.Capabilities.Tools = []string{"search@v1", "search@v1"}
		},
		"unsupported tool wildcard": func(manifest *Manifest) {
			// A version-floating grant must be exactly "name@*"; a name wildcard
			// carrying a version suffix is not a supported form (P1-08).
			manifest.Spec.Capabilities.Tools = []string{"weather.*@1"}
		},
		"omitted capability class": func(manifest *Manifest) {
			manifest.Spec.Capabilities.Secrets = nil
		},
		"spawn without child allowlist": func(manifest *Manifest) {
			manifest.Spec.Capabilities.SpawnTasks = true
		},
		"duplicate child allowlist": func(manifest *Manifest) {
			manifest.Spec.Capabilities.SpawnTasks = true
			manifest.Spec.Capabilities.ChildAgents = []string{"worker@1", "worker@1"}
		},
		"duplicate peer grant": func(manifest *Manifest) {
			manifest.Spec.Capabilities.Peers = []string{"worker@1", "worker@1"}
		},
		"empty peer grant": func(manifest *Manifest) {
			manifest.Spec.Capabilities.Peers = []string{""}
		},
		"embedded peer wildcard": func(manifest *Manifest) {
			manifest.Spec.Capabilities.Peers = []string{"wo*rker@1"}
		},
		"zero container workspace": func(manifest *Manifest) {
			manifest.Spec.Resources.WorkspaceBytes = 0
		},
		"invalid checkpoint": func(manifest *Manifest) {
			manifest.Spec.Checkpoint.Mode = CheckpointNone
		},
	} {
		t.Run(name, func(t *testing.T) {
			manifest := validManifest()
			mutate(&manifest)
			if err := manifest.Validate(); err == nil {
				t.Fatalf("invalid manifest %q was accepted", name)
			}
		})
	}
}

func TestManifestAcceptsAbsentAndPresentPeerGrants(t *testing.T) {
	// Absent peers must stay valid. validateCapabilitySet rejects a nil slice
	// ("use [] for default deny"), so an optional grant cannot go through the
	// required-class list the way tools/models/memory/secrets do: doing that
	// would reject every manifest written before peers existed.
	if err := validManifest().Validate(); err != nil {
		t.Fatalf("manifest without peers rejected: %v", err)
	}
	manifest := validManifest()
	manifest.Spec.Capabilities.Peers = []string{"worker@1", "team/*", "reporter@*"}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("valid peer grants rejected: %v", err)
	}
	// An explicit empty list is the opt-out form and must also be accepted.
	manifest = validManifest()
	manifest.Spec.Capabilities.Peers = []string{}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("explicitly empty peers rejected: %v", err)
	}
}

func TestManifestAcceptsSupportedCapabilityWildcards(t *testing.T) {
	// P1-08: exact pins, the version-floating "name@*" form and name wildcards
	// are all valid grant syntaxes.
	manifest := validManifest()
	manifest.Spec.Capabilities.Tools = []string{"weather@1.0.0", "search@*", "billing.*"}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("supported capability wildcards rejected: %v", err)
	}
}

func TestValidateSpecChecksPlatformFieldsWhenPresent(t *testing.T) {
	manifest := validManifest()
	encoded, err := json.Marshal(manifest.Spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	if err := ValidateSpec(encoded); err != nil {
		t.Fatalf("full platform spec rejected: %v", err)
	}

	manifest.Spec.Budget.WallSeconds = 0
	encoded, err = json.Marshal(manifest.Spec)
	if err != nil {
		t.Fatalf("marshal invalid spec: %v", err)
	}
	if err := ValidateSpec(encoded); err == nil {
		t.Fatal("invalid platform budget was accepted")
	}
}

func TestLegacyManifestPromotesDeterministicallyToV1(t *testing.T) {
	legacy := validManifest()
	legacy.APIVersion = LegacyManifestAPIVersion
	legacy.Spec.Runtimes[0].Interface = RuntimeInterfaceV1Alpha1
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, legacyDigest, err := DecodeManifest(encoded)
	if err != nil {
		t.Fatalf("legacy manifest must remain readable: %v", err)
	}
	promoted, err := decoded.PromoteToV1()
	if err != nil {
		t.Fatal(err)
	}
	if promoted.APIVersion != ManifestAPIVersion || promoted.Spec.Runtimes[0].Interface != RuntimeInterfaceV1 {
		t.Fatalf("manifest was not promoted: %+v", promoted)
	}
	stable, _, stableDigest, err := DecodeManifest(mustJSON(t, promoted))
	if err != nil || stable.Ref() != legacy.Ref() || stableDigest == legacyDigest {
		t.Fatalf("stable=%+v legacyDigest=%x stableDigest=%x err=%v", stable, legacyDigest, stableDigest, err)
	}
}

func TestAgentVersionSpecRejectsMixedRuntimeInterfaceVersions(t *testing.T) {
	manifest := validManifest()
	legacy := manifest.Spec.Runtimes[0]
	legacy.Class = "remote"
	legacy.Interface = RuntimeInterfaceV1Alpha1
	manifest.Spec.Runtimes = append(manifest.Spec.Runtimes, legacy)
	manifest.Spec.RuntimeClassPolicy.Allowed = []string{"oci", "remote"}
	if err := ValidateSpec(mustJSON(t, manifest.Spec)); err == nil {
		t.Fatal("mixed stable and legacy runtime interfaces were accepted")
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
