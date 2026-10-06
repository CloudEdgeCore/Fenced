package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	controlapi "github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
)

func TestNamespaceEndpointsLifecycle(t *testing.T) {
	backend := newMemoryStore()
	nsStore := namespace.NewMemoryStore()

	handler := controlapi.NewHandler(backend, backend, backend, backend,
		controlapi.WithNamespaceStore(nsStore),
	)
	authed := auth.StaticMiddleware(auth.Principal{Subject: "admin-1", TenantID: "tenant-ns-test"}, handler)

	// 1. List namespaces initially: default namespace should be present
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	resp := httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on list, got %d: %s", resp.Code, resp.Body.String())
	}
	var listResp map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	items, ok := listResp["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected 1 initial namespace (default), got %d", len(items))
	}

	// 2. Create a custom namespace
	createBody := []byte(`{
		"name": "data-pipeline",
		"displayName": "Data Processing Pipeline",
		"description": "ETL and data aggregation",
		"labels": {"team": "data"},
		"quota": {
			"max_services": 5,
			"max_tasks": 25,
			"allow_cross_namespace_ipc": true
		}
	}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/namespaces", bytes.NewReader(createBody))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", resp.Code, resp.Body.String())
	}

	// 3. Duplicate creation should return 409 Conflict
	req = httptest.NewRequest(http.MethodPost, "/v1/namespaces", bytes.NewReader(createBody))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for duplicate, got %d: %s", resp.Code, resp.Body.String())
	}

	// 4. Invalid name format should return 400 Bad Request
	invalidBody := []byte(`{"name": "Invalid_Uppercase_Name"}`)
	req = httptest.NewRequest(http.MethodPost, "/v1/namespaces", bytes.NewReader(invalidBody))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for invalid name, got %d: %s", resp.Code, resp.Body.String())
	}

	// 5. Get created namespace
	req = httptest.NewRequest(http.MethodGet, "/v1/namespaces/data-pipeline", nil)
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on get, got %d: %s", resp.Code, resp.Body.String())
	}
	var getResp map[string]any
	if err := json.Unmarshal(resp.Body.Bytes(), &getResp); err != nil {
		t.Fatalf("unmarshal get response: %v", err)
	}
	if getResp["displayName"] != "Data Processing Pipeline" {
		t.Fatalf("unexpected displayName: %v", getResp["displayName"])
	}

	// 6. Update namespace
	updateBody := []byte(`{
		"displayName": "Data Pipeline V2",
		"quota": {
			"max_services": 10
		}
	}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/namespaces/data-pipeline", bytes.NewReader(updateBody))
	req.Header.Set("Content-Type", "application/json")
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on update, got %d: %s", resp.Code, resp.Body.String())
	}

	// 7. Get usage
	req = httptest.NewRequest(http.MethodGet, "/v1/namespaces/data-pipeline/usage", nil)
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on usage, got %d: %s", resp.Code, resp.Body.String())
	}

	// 8. Delete default namespace is rejected with 422
	req = httptest.NewRequest(http.MethodDelete, "/v1/namespaces/default", nil)
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 Unprocessable Entity when deleting default ns, got %d: %s", resp.Code, resp.Body.String())
	}

	// 9. Delete custom namespace succeeds
	req = httptest.NewRequest(http.MethodDelete, "/v1/namespaces/data-pipeline", nil)
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d: %s", resp.Code, resp.Body.String())
	}

	// 10. Get deleted namespace returns 404
	req = httptest.NewRequest(http.MethodGet, "/v1/namespaces/data-pipeline", nil)
	resp = httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 Not Found after deletion, got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestNamespaceEndpointsDisabled(t *testing.T) {
	backend := newMemoryStore()
	// Handler without WithNamespaceStore
	handler := controlapi.NewHandler(backend, backend, backend, backend)
	authed := auth.StaticMiddleware(auth.Principal{Subject: "admin-1", TenantID: "tenant-ns-test"}, handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces", nil)
	resp := httptest.NewRecorder()
	authed.ServeHTTP(resp, req)
	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when namespace store is disabled, got %d", resp.Code)
	}
}
