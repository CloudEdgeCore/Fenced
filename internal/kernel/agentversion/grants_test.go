package agentversion

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSplitToolGrantsSeparatesTheThreeVocabularies(t *testing.T) {
	set := SplitToolGrants([]string{
		"echo.dev@1.0.0", "fs.read", "search.*", "echo.dev@*", "echo.dev@1.0.0",
	})
	if want := []string{"echo.dev", "fs.read", "search.*"}; !reflect.DeepEqual(set.Capability, want) {
		t.Fatalf("capability grants = %v, want %v", set.Capability, want)
	}
	if want := []string{"fs.read", "search.*"}; !reflect.DeepEqual(set.NameLevel, want) {
		t.Fatalf("name-level grants = %v, want %v", set.NameLevel, want)
	}
	if want := []string{"echo.dev@1.0.0", "echo.dev@*"}; !reflect.DeepEqual(set.Versioned, want) {
		t.Fatalf("version grants = %v, want %v", set.Versioned, want)
	}
}

func TestPublicationToolGrantsMergesTheDerivedField(t *testing.T) {
	// A normalized publication: the capability vocabulary in tools, the
	// pinned form in toolVersions.
	set := PublicationToolGrants([]string{"echo.dev"}, []string{"echo.dev@1.0.0"})
	if !reflect.DeepEqual(set.Capability, []string{"echo.dev"}) {
		t.Fatalf("capability grants = %v", set.Capability)
	}
	if !reflect.DeepEqual(set.NameLevel, []string{"echo.dev"}) {
		t.Fatalf("name-level grants = %v", set.NameLevel)
	}
	if !reflect.DeepEqual(set.Versioned, []string{"echo.dev@1.0.0"}) {
		t.Fatalf("version grants = %v", set.Versioned)
	}
	// A legacy publication: both forms inside tools.
	set = PublicationToolGrants([]string{"echo.dev@1.0.0"}, nil)
	if !reflect.DeepEqual(set.Capability, []string{"echo.dev"}) {
		t.Fatalf("legacy capability grants = %v", set.Capability)
	}
	if len(set.NameLevel) != 0 {
		t.Fatalf("a pinned grant must not also grant the name level: %v", set.NameLevel)
	}
	if !reflect.DeepEqual(set.Versioned, []string{"echo.dev@1.0.0"}) {
		t.Fatalf("legacy version grants = %v", set.Versioned)
	}
}

func TestCanonicalizeSpecNormalizesToolGrants(t *testing.T) {
	spec := json.RawMessage(`{
		"runtimes":[{"class":"remote","interface":"fenced.runtime.interface/v1","runtimeABI":"fenced.adapter-http/v1","entrypoint":["internal"]}],
		"capabilities":{"tools":["echo.dev@1.0.0","fs.read"],"models":[],"memory":[],"secrets":[]}
	}`)
	canonical, digest, err := CanonicalizeSpec(spec)
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(canonical, &document); err != nil {
		t.Fatal(err)
	}
	capabilities := document["capabilities"].(map[string]any)
	tools := capabilities["tools"].([]any)
	if len(tools) != 2 || tools[0] != "echo.dev" || tools[1] != "fs.read" {
		t.Fatalf("capabilities.tools = %v, want the bare tool names", tools)
	}
	versions := capabilities["toolVersions"].([]any)
	if len(versions) != 1 || versions[0] != "echo.dev@1.0.0" {
		t.Fatalf("capabilities.toolVersions = %v, want the pinned grant", versions)
	}

	// The canonical form is stable: the same input yields the same digest,
	// and canonicalizing the canonical form reproduces it.
	again, secondDigest, err := CanonicalizeSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(canonical) || secondDigest != digest {
		t.Fatal("canonicalization must be deterministic")
	}
	third, thirdDigest, err := CanonicalizeSpec(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if string(third) != string(canonical) || thirdDigest != digest {
		t.Fatal("canonicalization must be idempotent: a pinned version must survive a second pass")
	}

	// A different grant spelling that means the same thing yields the same
	// publication, while a pin that differs stays a different publication.
	equivalent := json.RawMessage(`{
		"runtimes":[{"class":"remote","interface":"fenced.runtime.interface/v1","runtimeABI":"fenced.adapter-http/v1","entrypoint":["internal"]}],
		"capabilities":{"tools":["echo.dev","fs.read"],"toolVersions":["echo.dev@1.0.0"],"models":[],"memory":[],"secrets":[]}
	}`)
	_, equivalentDigest, err := CanonicalizeSpec(equivalent)
	if err != nil {
		t.Fatal(err)
	}
	if equivalentDigest != digest {
		t.Fatal("an equivalent grant spelling must produce the same publication digest")
	}
	otherPin := json.RawMessage(`{
		"runtimes":[{"class":"remote","interface":"fenced.runtime.interface/v1","runtimeABI":"fenced.adapter-http/v1","entrypoint":["internal"]}],
		"capabilities":{"tools":["echo.dev@2.0.0","fs.read"],"models":[],"memory":[],"secrets":[]}
	}`)
	_, otherDigest, err := CanonicalizeSpec(otherPin)
	if err != nil {
		t.Fatal(err)
	}
	if otherDigest == digest {
		t.Fatal("a different pinned version must produce a different publication digest")
	}
}

func TestCanonicalizeSpecDropsAnEmptyVersionVocabulary(t *testing.T) {
	spec := json.RawMessage(`{"capabilities":{"tools":["fs.read"],"models":[],"memory":[],"secrets":[]}}`)
	canonical, _, err := CanonicalizeSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(canonical, &document); err != nil {
		t.Fatal(err)
	}
	capabilities := document["capabilities"].(map[string]any)
	if _, present := capabilities["toolVersions"]; present {
		t.Fatalf("an empty version vocabulary must not be published: %v", capabilities)
	}
}
