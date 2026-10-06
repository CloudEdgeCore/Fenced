package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentpkg"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
)

func TestVersionAndManifestMigration(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"version", "-json"}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"productVersion":"1.3.0.0"`) || !strings.Contains(output.String(), `"releaseStage":"GA"`) {
		t.Fatalf("unexpected version output: %s", output.String())
	}

	legacy := agentversion.Manifest{
		APIVersion: agentversion.LegacyManifestAPIVersion, Kind: agentversion.ManifestKind,
		Metadata: agentversion.Metadata{Name: "legacy", Version: "0.9.0", Namespace: "default"},
		Spec: agentversion.Spec{
			RuntimeClassPolicy: agentversion.RuntimeClassPolicy{Allowed: []string{"remote"}, Preferred: "remote"},
			Runtimes:           []agentversion.RuntimeTarget{{Class: "remote", Interface: agentversion.RuntimeInterfaceV1Alpha1, RuntimeABI: "fenced.remote/v1", Entrypoint: []string{"http://127.0.0.1:8088"}}},
			Capabilities:       &agentversion.Capabilities{Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{}},
			Resources:          &agentversion.ResourceLimits{CPUMillis: 100, MemoryMiB: 128},
			Budget:             &agentversion.Budget{WallSeconds: 60},
			Checkpoint:         &agentversion.CheckpointPolicy{Mode: agentversion.CheckpointNone},
		},
	}
	directory := t.TempDir()
	legacyPath, stablePath := filepath.Join(directory, "legacy.json"), filepath.Join(directory, "stable.json")
	if err := os.WriteFile(legacyPath, mustMarshal(t, legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"migrate", "-manifest", legacyPath, "-out", stablePath}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	stable, _, _, err := loadManifest(stablePath)
	if err != nil || stable.APIVersion != agentversion.ManifestAPIVersion || stable.Spec.Runtimes[0].Interface != agentversion.RuntimeInterfaceV1 {
		t.Fatalf("stable=%+v err=%v", stable, err)
	}
}

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestInitValidatePackageAndSignWorkflow(t *testing.T) {
	for _, adapter := range []string{"go", "python", "langgraph", "a2a"} {
		t.Run(adapter, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "sample-"+adapter)
			var output bytes.Buffer
			if err := run([]string{"init", "-dir", directory, "-name", "sample-" + adapter, "-adapter", adapter}, &output, io.Discard); err != nil {
				t.Fatal(err)
			}
			manifestPath := filepath.Join(directory, "agent.json")
			if err := run([]string{"validate", "-manifest", manifestPath}, &output, io.Discard); err != nil {
				t.Fatal(err)
			}
			manifest, _, _, err := loadManifest(manifestPath)
			if err != nil || manifest.Spec.Runtimes[0].Interface != agentversion.RuntimeInterfaceV1 {
				t.Fatalf("manifest=%+v err=%v", manifest, err)
			}
			if err := run([]string{"init", "-dir", directory, "-name", "sample-" + adapter, "-adapter", adapter}, &output, io.Discard); err == nil {
				t.Fatal("init overwrote an existing project")
			}
		})
	}

	directory := filepath.Join(t.TempDir(), "package")
	if err := run([]string{"init", "-dir", directory, "-name", "packaged-agent"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	unsignedPath := filepath.Join(directory, "package-manifest.json")
	if err := run([]string{
		"package", "-manifest", filepath.Join(directory, "agent.json"), "-out", unsignedPath,
		"-builder", "ci", "-workflow", "build.yml", "-git-commit", "abc",
		"-built-at", "2026-08-21T08:00:00Z",
	}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	signing, _, err := agentpkg.GenerateSigningKey("ci")
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(directory, "private.key")
	if err := os.WriteFile(keyPath, []byte(base64.RawStdEncoding.EncodeToString(signing.PrivateKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	packagePath := filepath.Join(directory, "package.json")
	if err := run([]string{
		"sign", "-package-manifest", unsignedPath, "-key-id", "ci",
		"-private-key-file", keyPath, "-out", packagePath,
	}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	var pkg agentpkg.Package
	if err := decodeFileStrict(packagePath, &pkg); err != nil {
		t.Fatal(err)
	}
	if pkg.Signature.KeyID != "ci" || pkg.Manifest.AgentVersionRef != "packaged-agent@0.1.0" {
		t.Fatalf("unexpected package: %+v", pkg)
	}
}

func TestPublishRunAndLogsUseControlContract(t *testing.T) {
	t.Setenv("FENCED_TOKEN", "test-token")
	directory := filepath.Join(t.TempDir(), "agent")
	if err := run([]string{"init", "-dir", directory, "-name", "api-agent"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("missing bearer token")
		}
		switch request.URL.Path {
		case "/v1/agent-versions":
			calls = append(calls, "publish")
			var body map[string]json.RawMessage
			err := json.NewDecoder(request.Body).Decode(&body)
			var manifest agentversion.Manifest
			decodeErr := json.Unmarshal(body["manifest"], &manifest)
			if err != nil || decodeErr != nil || manifest.Ref() != "api-agent@0.1.0" {
				t.Errorf("publish body=%+v errors=%v/%v", body, err, decodeErr)
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusCreated)
			_, _ = writer.Write([]byte("{\"ref\":\"api-agent@0.1.0\"}"))
		case "/v1/tasks":
			calls = append(calls, "run")
			if request.Header.Get("Idempotency-Key") != "task-fixed" {
				t.Errorf("unexpected idempotency key")
			}
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte("{\"id\":\"task-1\"}"))
		case "/v1/tasks/33333333-3333-3333-3333-333333333333/events":
			calls = append(calls, "logs")
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = writer.Write([]byte("event: task.terminal\ndata: {\"phase\":\"SUCCEEDED\"}\n\n"))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	var output bytes.Buffer
	if err := run([]string{
		"publish", "-endpoint", server.URL, "-manifest", filepath.Join(directory, "agent.json"),
		"-idempotency-key", "publish-fixed",
	}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{
		"run", "-endpoint", server.URL, "-agent", "api-agent@0.1.0", "-goal", "execute",
		"-idempotency-key", "task-fixed",
	}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"logs", "-endpoint", server.URL, "-task", "33333333-3333-3333-3333-333333333333"}, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ",") != "publish,run,logs" || !strings.Contains(output.String(), "task.terminal") {
		t.Fatalf("calls=%v output=%s", calls, output.String())
	}
}

func TestWorkflowCommandsUseCASAndRenderTree(t *testing.T) {
	workflowID := "44444444-4444-4444-4444-444444444444"
	stepPath := "/v1/workflows/" + workflowID + "/steps/review/approval"
	directory := t.TempDir()
	workflowFile := filepath.Join(directory, "workflow.json")
	if err := os.WriteFile(workflowFile, []byte(`{"apiVersion":"fenced.dev/workflow/v1","kind":"Workflow","steps":[{"name":"root","agentVersionRef":"api-agent@0.1.0","goal":"work"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls = append(calls, request.Method+" "+request.URL.Path)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v1/workflows":
			if request.Header.Get("Idempotency-Key") != "wf-fixed" {
				t.Errorf("missing workflow idempotency key")
			}
			writer.WriteHeader(http.StatusAccepted)
		case "/v1/workflows/" + workflowID:
		case "/v1/workflows/" + workflowID + "/cancel", stepPath:
			if request.Header.Get("If-Match") != `W/"7"` {
				t.Errorf("If-Match = %q", request.Header.Get("If-Match"))
			}
			writer.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(writer, request)
			return
		}
		_, _ = writer.Write([]byte(`{"id":"` + workflowID + `","status":"RUNNING","steps":[{"name":"root","status":"SUCCEEDED"},{"name":"review","status":"WAITING_APPROVAL","parentStepName":"root","isDynamic":true,"spawnDepth":1}]}`))
	}))
	defer server.Close()

	var output bytes.Buffer
	commands := [][]string{
		{"workflow", "create", "-endpoint", server.URL, "-file", workflowFile, "-goal", "ship", "-idempotency-key", "wf-fixed"},
		{"workflow", "get", "-endpoint", server.URL, "-id", workflowID},
		{"workflow", "cancel", "-endpoint", server.URL, "-id", workflowID, "-version", "7"},
		{"workflow", "approve", "-endpoint", server.URL, "-id", workflowID, "-step", "review", "-version", "7"},
		{"workflow", "reject", "-endpoint", server.URL, "-id", workflowID, "-step", "review", "-version", "7"},
		{"workflow", "tree", "-endpoint", server.URL, "-id", workflowID},
	}
	for _, command := range commands {
		if err := run(command, &output, io.Discard); err != nil {
			t.Fatalf("%v: %v", command, err)
		}
	}
	if !strings.Contains(output.String(), "review [WAITING_APPROVAL] <- root") {
		t.Fatalf("tree output:\n%s", output.String())
	}
	if len(calls) != len(commands) {
		t.Fatalf("calls=%v", calls)
	}
}
