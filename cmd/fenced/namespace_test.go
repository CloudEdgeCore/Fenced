package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
)

func TestNamespaceCLI(t *testing.T) {
	// Mock server that implements /v1/namespaces endpoints
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		w.Header().Set("Content-Type", "application/json")

		if path == "/v1/namespaces" {
			if r.Method == http.MethodPost {
				var req map[string]any
				_ = json.NewDecoder(r.Body).Decode(&req)
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"name":        req["name"],
					"displayName": req["displayName"],
					"phase":       "Active",
					"createdAt":   time.Now().UTC(),
				})
				return
			}
			if r.Method == http.MethodGet {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"items": []any{
						map[string]any{
							"name":        "default",
							"displayName": "Default Namespace",
							"phase":       "Active",
						},
						map[string]any{
							"name":        "test-ns",
							"displayName": "Test Namespace",
							"phase":       "Active",
						},
					},
				})
				return
			}
		}

		if path == "/v1/namespaces/test-ns" {
			if r.Method == http.MethodGet {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"name":        "test-ns",
					"displayName": "Test Namespace",
					"phase":       "Active",
				})
				return
			}
			if r.Method == http.MethodPut {
				var req map[string]any
				_ = json.NewDecoder(r.Body).Decode(&req)
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"name":        "test-ns",
					"displayName": req["displayName"],
					"phase":       "Active",
				})
				return
			}
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}

		if path == "/v1/namespaces/test-ns/usage" {
			if r.Method == http.MethodGet {
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"namespace": "test-ns",
					"usage": namespace.ResourceUsage{
						Namespace:      "test-ns",
						ActiveServices: 2,
						ActiveTasks:    5,
					},
				})
				return
			}
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// 1. Create namespace
	var out bytes.Buffer
	err := run([]string{
		"namespace", "create",
		"-endpoint", server.URL,
		"-name", "test-ns",
		"-display-name", "Test Namespace",
		"-max-services", "3",
		"-allow-cross-ipc",
	}, &out, io.Discard)
	if err != nil {
		t.Fatalf("namespace create failed: %v", err)
	}
	if !strings.Contains(out.String(), "test-ns") {
		t.Fatalf("expected test-ns in output, got: %s", out.String())
	}

	// 2. List namespaces
	out.Reset()
	err = run([]string{
		"namespace", "list",
		"-endpoint", server.URL,
	}, &out, io.Discard)
	if err != nil {
		t.Fatalf("namespace list failed: %v", err)
	}
	if !strings.Contains(out.String(), "default") || !strings.Contains(out.String(), "test-ns") {
		t.Fatalf("expected namespaces listed, got: %s", out.String())
	}

	// List as JSON
	out.Reset()
	err = run([]string{
		"namespace", "list",
		"-endpoint", server.URL,
		"-json",
	}, &out, io.Discard)
	if err != nil {
		t.Fatalf("namespace list -json failed: %v", err)
	}
	if !strings.Contains(out.String(), `"items"`) {
		t.Fatalf("expected JSON items, got: %s", out.String())
	}

	// 3. Get namespace
	out.Reset()
	err = run([]string{
		"namespace", "get", "test-ns",
		"-endpoint", server.URL,
	}, &out, io.Discard)
	if err != nil {
		t.Fatalf("namespace get failed: %v", err)
	}
	if !strings.Contains(out.String(), "test-ns") {
		t.Fatalf("expected test-ns in get output, got: %s", out.String())
	}

	// 4. Update namespace
	out.Reset()
	err = run([]string{
		"namespace", "update", "test-ns",
		"-endpoint", server.URL,
		"-display-name", "Updated Test Namespace",
	}, &out, io.Discard)
	if err != nil {
		t.Fatalf("namespace update failed: %v", err)
	}
	if !strings.Contains(out.String(), "Updated Test Namespace") {
		t.Fatalf("expected updated displayName, got: %s", out.String())
	}

	// 5. Usage
	out.Reset()
	err = run([]string{
		"namespace", "usage", "test-ns",
		"-endpoint", server.URL,
	}, &out, io.Discard)
	if err != nil {
		t.Fatalf("namespace usage failed: %v", err)
	}
	if !strings.Contains(out.String(), "active_services") || !strings.Contains(out.String(), "active_tasks") {
		t.Fatalf("expected usage metrics in output, got: %s", out.String())
	}

	// 6. Delete namespace
	out.Reset()
	err = run([]string{
		"namespace", "delete", "test-ns",
		"-endpoint", server.URL,
	}, &out, io.Discard)
	if err != nil {
		t.Fatalf("namespace delete failed: %v", err)
	}
	if !strings.Contains(out.String(), "deleted") {
		t.Fatalf("expected deleted confirmation, got: %s", out.String())
	}
}
