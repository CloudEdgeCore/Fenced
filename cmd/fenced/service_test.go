package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestServiceCreateSubmitsPublishedVersionAndWorkload(t *testing.T) {
	for _, runtimeClass := range []string{"", "research-network"} {
		t.Run("runtime-class="+runtimeClass, func(t *testing.T) {
			const workload = `{"budget":{"tokens":2000,"costUsd":0.1,"toolCalls":8,"wallSeconds":120},"placement":{"runtimeClasses":["research-network"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"workspaceBytes":1048576,"llmConcurrency":1},"sequence":9007199254740993}`
			specPath := filepath.Join(t.TempDir(), "task-spec.json")
			if err := os.WriteFile(specPath, []byte(workload), 0600); err != nil {
				t.Fatal(err)
			}
			var received struct {
				Namespace string `json:"namespace"`
				Name      string `json:"name"`
				AgentID   string `json:"agentId"`
				Spec      struct {
					Replicas        int             `json:"replicas"`
					RestartPolicy   string          `json:"restartPolicy"`
					AgentVersionRef string          `json:"agentVersionRef"`
					RuntimeClass    string          `json:"runtimeClass"`
					WorkloadSpec    json.RawMessage `json:"workloadSpec"`
				} `json:"spec"`
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/services" {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
				}
				if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
					t.Errorf("decode create request: %v", err)
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, `{"id":"svc-1"}`)
			}))
			defer server.Close()
			args := []string{"service", "create", "-endpoint", server.URL, "-name", "customer-bot", "-agent", "support/customer-bot@1.0.0", "-namespace", "support", "-replicas", "2", "-restart-policy", "OnFailure", "-spec", specPath}
			if runtimeClass != "" {
				args = append(args, "-runtime-class", runtimeClass)
			}
			var stdout bytes.Buffer
			if err := run(args, &stdout, io.Discard); err != nil {
				t.Fatal(err)
			}
			if received.Namespace != "support" || received.Name != "customer-bot" || received.AgentID != "support/customer-bot@1.0.0" || received.Spec.AgentVersionRef != received.AgentID {
				t.Fatalf("service identity = %+v", received)
			}
			if received.Spec.Replicas != 2 || received.Spec.RestartPolicy != "OnFailure" || received.Spec.RuntimeClass != runtimeClass {
				t.Fatalf("service spec = %+v", received.Spec)
			}
			var compact bytes.Buffer
			if err := json.Compact(&compact, received.Spec.WorkloadSpec); err != nil {
				t.Fatal(err)
			}
			if compact.String() != workload {
				t.Fatalf("workload changed: %s", compact.String())
			}
			if !strings.Contains(stdout.String(), "svc-1") {
				t.Fatalf("create response = %s", stdout.String())
			}
		})
	}
}

func TestServiceCreateRejectsInvalidInputBeforeHTTP(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	for _, tc := range []struct {
		name         string
		agent        string
		namespace    string
		workload     string
		skipSpec     bool
		missingFile  bool
		runtimeClass string
		want         string
	}{
		{name: "spec required", agent: "customer-bot@1.0.0", namespace: "default", skipSpec: true, want: "-spec is required"},
		{name: "missing file", agent: "customer-bot@1.0.0", namespace: "default", missingFile: true, want: "read workload spec"},
		{name: "array", agent: "customer-bot@1.0.0", namespace: "default", workload: `[]`, want: "JSON object"},
		{name: "null", agent: "customer-bot@1.0.0", namespace: "default", workload: `null`, want: "JSON object"},
		{name: "scalar", agent: "customer-bot@1.0.0", namespace: "default", workload: `"task"`, want: "JSON object"},
		{name: "malformed", agent: "customer-bot@1.0.0", namespace: "default", workload: `{`, want: "JSON object"},
		{name: "trailing json", agent: "customer-bot@1.0.0", namespace: "default", workload: `{} {}`, want: "JSON object"},
		{name: "unversioned identity", agent: "customer-bot", namespace: "default", workload: `{}`, want: "-agent"},
		{name: "noncanonical default namespace", agent: "default/customer-bot@1.0.0", namespace: "default", workload: `{}`, want: "canonical"},
		{name: "namespace mismatch", agent: "support/customer-bot@1.0.0", namespace: "default", workload: `{}`, want: "namespace must match"},
		{name: "invalid runtime class", agent: "customer-bot@1.0.0", namespace: "default", workload: `{}`, runtimeClass: "runtime/remote", want: "-runtime-class"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"service", "create", "-endpoint", server.URL, "-name", "customer-bot", "-agent", tc.agent, "-namespace", tc.namespace}
			if !tc.skipSpec {
				specPath := filepath.Join(t.TempDir(), "task-spec.json")
				if !tc.missingFile {
					if err := os.WriteFile(specPath, []byte(tc.workload), 0600); err != nil {
						t.Fatal(err)
					}
				}
				args = append(args, "-spec", specPath)
			}
			if tc.runtimeClass != "" {
				args = append(args, "-runtime-class", tc.runtimeClass)
			}
			if err := run(args, io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid input sent %d HTTP requests", requests.Load())
	}
}

func TestServiceCommandsForwardBearerToken(t *testing.T) {
	t.Setenv("FENCED_TOKEN", "service-test-token")
	specPath := filepath.Join(t.TempDir(), "task-spec.json")
	if err := os.WriteFile(specPath, []byte(`{"budget":{},"placement":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		command string
		args    []string
		method  string
		path    string
		status  int
	}{
		{"create", []string{"-name", "worker", "-agent", "worker@1", "-spec", specPath}, http.MethodPost, "/v1/services", http.StatusCreated},
		{"list", nil, http.MethodGet, "/v1/services", http.StatusOK},
		{"get", []string{"svc-1"}, http.MethodGet, "/v1/services/svc-1", http.StatusOK},
		{"scale", []string{"-replicas", "2", "svc-1"}, http.MethodPost, "/v1/services/svc-1/scale", http.StatusOK},
		{"restart", []string{"svc-1"}, http.MethodPost, "/v1/services/svc-1/restart", http.StatusOK},
		{"stop", []string{"svc-1"}, http.MethodPost, "/v1/services/svc-1/stop", http.StatusOK},
		{"delete", []string{"svc-1"}, http.MethodDelete, "/v1/services/svc-1", http.StatusNoContent},
		{"instances", []string{"svc-1"}, http.MethodGet, "/v1/services/svc-1/instances", http.StatusOK},
	} {
		t.Run(tc.command, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer service-test-token" {
					t.Error("service command did not forward bearer authentication")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.Method != tc.method || r.URL.Path != tc.path {
					t.Errorf("request = %s %s, want %s %s", r.Method, r.URL.Path, tc.method, tc.path)
				}
				w.WriteHeader(tc.status)
				if tc.status != http.StatusNoContent {
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			defer server.Close()
			args := append([]string{"service", tc.command, "-endpoint", server.URL}, tc.args...)
			if err := run(args, io.Discard, io.Discard); err != nil {
				t.Fatal(err)
			}
		})
	}
}
