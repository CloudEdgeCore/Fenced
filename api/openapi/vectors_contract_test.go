package openapi_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

// The golden wire vectors and the OpenAPI contract are two descriptions of the
// same interface. This test checks that they agree, so the vectors, the SDKs
// and the published contract cannot drift apart.
const vectorFile = "../../sdk/conformance/fixtures/runtime-interface-v1.json"

type vectors struct {
	Protocol  string              `json:"protocol"`
	Enums     map[string][]string `json:"enums"`
	Endpoints []struct {
		Name           string         `json:"name"`
		Method         string         `json:"method"`
		Path           string         `json:"path"`
		SpecPath       string         `json:"specPath"`
		Status         int            `json:"status"`
		InContract     *bool          `json:"inContract"`
		RequiredFields []string       `json:"requiredFields"`
		OptionalFields []string       `json:"optionalFields"`
		Values         map[string]any `json:"values"`
	} `json:"endpoints"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(vectorFile))
	if err != nil {
		t.Fatalf("read wire vectors: %v", err)
	}
	var document vectors
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode wire vectors: %v", err)
	}
	return document
}

// specPath maps a vector path onto the templated path of the contract.
func specPath(vectorPath string) string {
	if vectorPath == "" {
		return ""
	}
	parts := strings.Split(vectorPath, "/")
	for index, part := range parts {
		if part == "vector-execution-1" || part == "vector-unknown-execution" {
			parts[index] = "{executionId}"
		}
	}
	return strings.Join(parts, "/")
}

func TestWireVectorsAgreeWithTheRuntimeInterfaceContract(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("runtime-interface-v1.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI contract: %v", err)
	}
	vectors := loadVectors(t)

	for _, vector := range vectors.Endpoints {
		if vector.InContract != nil && !*vector.InContract {
			// The vector exercises a path the contract does not describe,
			// such as the unknown-route fallback.
			continue
		}
		t.Run(vector.Name, func(t *testing.T) {
			path := vector.SpecPath
			if path == "" {
				path = specPath(vector.Path)
			}
			item := document.Paths.Find(path)
			if item == nil {
				t.Fatalf("the contract has no path %q", path)
			}
			operation := item.Get
			if operation == nil {
				operation = item.Post
			}
			if operation == nil {
				t.Fatalf("the contract has no operation for %q", path)
			}
			response := operation.Responses.Status(vector.Status)
			if response == nil {
				t.Fatalf("the contract has no %d response for %q", vector.Status, path)
			}
			if response.Value == nil || response.Value.Content == nil {
				t.Fatalf("the %d response of %q has no content", vector.Status, path)
			}
			media := response.Value.Content.Get("application/json")
			if media == nil || media.Schema == nil || media.Schema.Value == nil {
				t.Fatalf("the %d response of %q has no JSON schema", vector.Status, path)
			}
			schema := media.Schema.Value
			required := append([]string(nil), schema.Required...)
			sort.Strings(required)
			want := append([]string(nil), vector.RequiredFields...)
			sort.Strings(want)
			if strings.Join(required, ",") != strings.Join(want, ",") {
				t.Errorf("required fields = %v, the vectors say %v", required, want)
			}
			known := map[string]bool{}
			for _, field := range want {
				known[field] = true
			}
			for _, field := range vector.OptionalFields {
				known[field] = true
			}
			for name := range schema.Properties {
				if !known[name] {
					t.Errorf("the contract knows the field %q, the vectors do not", name)
				}
			}
		})
	}
}

// The status vocabulary is part of the contract, not of one implementation.
func TestWireVectorEnumsAgreeWithTheContract(t *testing.T) {
	loader := openapi3.NewLoader()
	document, err := loader.LoadFromFile("runtime-interface-v1.yaml")
	if err != nil {
		t.Fatalf("load OpenAPI contract: %v", err)
	}
	vectors := loadVectors(t)

	result := document.Components.Schemas["Result"]
	if result == nil || result.Value == nil {
		t.Fatal("the contract has no Result schema")
	}
	status := result.Value.Properties["status"]
	if status == nil || status.Value == nil {
		t.Fatal("the Result schema has no status property")
	}
	var contract []string
	for _, value := range status.Value.Enum {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("the status enum holds %v, which is not a string", value)
		}
		contract = append(contract, text)
	}
	sort.Strings(contract)
	want := append([]string(nil), vectors.Enums["executionStatus"]...)
	sort.Strings(want)
	if strings.Join(contract, ",") != strings.Join(want, ",") {
		t.Errorf("the contract statuses = %v, the vectors say %v", contract, want)
	}
}
