package admission

import (
	"encoding/json"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

const pinnedDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func versionWithImage(imageJSON string) store.AgentVersion {
	spec := `{"runtimeClassPolicy":{"allowed":["oci"]}}`
	if imageJSON != "" {
		spec = `{"runtimeClassPolicy":{"allowed":["oci"]},"image":` + imageJSON + `}`
	}
	return store.AgentVersion{
		ID: uuid.New(), TenantID: "tenant-a", Name: "agent", Version: "1",
		Spec: json.RawMessage(spec),
	}
}

func pinnedImageJSON(ref, digest string) string {
	return `{"ref":"` + ref + `","digest":"` + digest + `"}`
}

func taskSpecWithImage(image string) json.RawMessage {
	body := `{"placement":{"runtimeClasses":["oci"]}}`
	if image != "" {
		body = `{"placement":{"runtimeClasses":["oci"]},"image":` + image + `}`
	}
	return json.RawMessage(body)
}

func reasonCodes(reasons []store.AdmissionReason) []string {
	codes := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		codes = append(codes, reason.Code)
	}
	return codes
}

func TestImagePinPolicyAdmitsMatchingPin(t *testing.T) {
	version := versionWithImage(pinnedImageJSON("example.com/agent", pinnedDigest))
	reasons := checkVersionPolicy(taskSpecWithImage(pinnedImageJSON("example.com/agent", pinnedDigest)), version)
	if len(reasons) != 0 {
		t.Fatalf("matching pin rejected: %v", reasonCodes(reasons))
	}
}

func TestImagePinPolicyRequiresPinWhenVersionPins(t *testing.T) {
	version := versionWithImage(pinnedImageJSON("example.com/agent", pinnedDigest))
	reasons := checkVersionPolicy(taskSpecWithImage(""), version)
	if !hasReason(reasons, "RUNTIME_IMAGE_REQUIRED") {
		t.Fatalf("missing pin not rejected: %v", reasonCodes(reasons))
	}
}

func TestImagePinPolicyRejectsMismatchedPin(t *testing.T) {
	version := versionWithImage(pinnedImageJSON("example.com/agent", pinnedDigest))
	reasons := checkVersionPolicy(taskSpecWithImage(pinnedImageJSON("example.com/evil", pinnedDigest)), version)
	if !hasReason(reasons, "RUNTIME_IMAGE_MISMATCH") {
		t.Fatalf("mismatched pin not rejected: %v", reasonCodes(reasons))
	}
	// Same ref, different digest is also a mismatch (content pin).
	otherDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	reasons = checkVersionPolicy(taskSpecWithImage(pinnedImageJSON("example.com/agent", otherDigest)), version)
	if !hasReason(reasons, "RUNTIME_IMAGE_MISMATCH") {
		t.Fatalf("digest mismatch not rejected: %v", reasonCodes(reasons))
	}
}

func TestImagePinPolicyRejectsMalformedTaskPin(t *testing.T) {
	// No version pin, but the task pin is malformed: fail closed.
	version := versionWithImage("")
	reasons := checkVersionPolicy(taskSpecWithImage(pinnedImageJSON("example.com/agent", "md5:abc")), version)
	if !hasReason(reasons, "RUNTIME_IMAGE_INVALID") {
		t.Fatalf("malformed pin not rejected: %v", reasonCodes(reasons))
	}
}

func hasReason(reasons []store.AdmissionReason, code string) bool {
	for _, reason := range reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}

func containerEngine() *Engine {
	return New(Limits{
		RuntimeClasses: []string{"oci", "wasm"},
		MaxCPU:         64000, MaxMemory: 262144, MaxLLMConcurrency: 128,
		ContainerClasses: []string{"oci"},
	})
}

func containerTask(spec string) store.Task {
	return store.Task{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "limits", Spec: json.RawMessage(spec), ResourceVersion: 1,
	}
}

func TestContainerClassRequiresExplicitLimits(t *testing.T) {
	engine := containerEngine()
	for name, test := range map[string]struct {
		spec  string
		codes []string
	}{
		"zero cpu": {
			spec:  `{"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":0,"memoryMiB":128,"workspaceBytes":1048576,"llmConcurrency":1}}`,
			codes: []string{"CONTAINER_CPU_REQUIRED"},
		},
		"zero memory": {
			spec:  `{"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":0,"workspaceBytes":1048576,"llmConcurrency":1}}`,
			codes: []string{"CONTAINER_MEMORY_REQUIRED"},
		},
		"zero workspace": {
			spec:  `{"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":0,"llmConcurrency":1}}`,
			codes: []string{"CONTAINER_WORKSPACE_REQUIRED"},
		},
		"fully explicit admitted": {
			spec: `{"budget":{"tokens":100,"costUsd":1,"toolCalls":10,"wallSeconds":60},
				"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":1048576,"llmConcurrency":1}}`,
			codes: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			decision := engine.Evaluate(containerTask(test.spec))
			if len(test.codes) == 0 {
				if !decision.Admit {
					t.Fatalf("explicit limits rejected: %+v", decision.Reasons)
				}
				return
			}
			if decision.Admit {
				t.Fatalf("zero-limit container task admitted")
			}
			for _, code := range test.codes {
				if !hasReason(decision.Reasons, code) {
					t.Fatalf("missing reason %s in %+v", code, reasonCodes(decision.Reasons))
				}
			}
		})
	}
}

func TestContainerLimitsDoNotApplyToOtherClasses(t *testing.T) {
	engine := containerEngine()
	// A wasm task with zero workspace is fine: the requirement is per-class.
	decision := engine.Evaluate(containerTask(`{"budget":{"tokens":100,"costUsd":1,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["wasm"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":0,"llmConcurrency":1}}`))
	if !decision.Admit {
		t.Fatalf("wasm task without workspace was rejected: %+v", decision.Reasons)
	}
}

func TestRuntimeCommandValidation(t *testing.T) {
	engine := containerEngine()
	decision := engine.Evaluate(containerTask(`{"budget":{"tokens":100,"costUsd":1,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":1048576,"llmConcurrency":1},
		"runtime":{"command":[""]}}`))
	if decision.Admit || !hasReason(decision.Reasons, "RUNTIME_COMMAND_INVALID") {
		t.Fatalf("invalid runtime command decision = %+v", decision)
	}
}
