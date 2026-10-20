package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	controlapi "github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
)

// The rollout surface is the operator's way to move a service to another
// agent version. It has to be reachable, observable, and it has to refuse a
// second movement while one is running.
func TestServiceRolloutEndpoints(t *testing.T) {
	backend := newMemoryStore()
	serviceStore := supervisor.NewMemoryStore()
	sup := supervisor.NewSupervisor(serviceStore)

	handler := controlapi.NewHandler(backend, backend, backend, backend,
		controlapi.WithSupervisor(sup, serviceStore),
	)
	authed := auth.StaticMiddleware(auth.Principal{Subject: "user-1", TenantID: "tenant-a"}, handler)

	createBody := []byte(`{
		"namespace": "default",
		"name": "rollout-svc",
		"agentId": "summarizer",
		"spec": {"replicas": 2, "restartPolicy": "Always", "agentVersion": "v1"}
	}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/services", bytes.NewReader(createBody))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	authed.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", response.Code, response.Body.String())
	}
	var created map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created service: %v", err)
	}
	serviceID, _ := created["id"].(string)
	if serviceID == "" {
		t.Fatal("missing service ID in the response")
	}

	// 1. Start a rollout.
	rolloutBody := []byte(`{"newVersion": "v2", "config": {"maxSurge": 1, "maxUnavailable": 1, "autoRollback": true}}`)
	request = httptest.NewRequest(http.MethodPost, "/v1/services/"+serviceID+"/rollout", bytes.NewReader(rolloutBody))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	authed.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", response.Code, response.Body.String())
	}
	var rolled map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &rolled); err != nil {
		t.Fatalf("unmarshal rollout response: %v", err)
	}
	status, _ := rolled["status"].(map[string]any)
	if status["rolloutPhase"] != "PROGRESSING" {
		t.Fatalf("expected the rollout to progress, got %v", status["rolloutPhase"])
	}
	if status["rolloutTargetVersion"] != "v2" {
		t.Fatalf("expected the target version to be v2, got %v", status["rolloutTargetVersion"])
	}

	// 2. Read the rollout.
	request = httptest.NewRequest(http.MethodGet, "/v1/services/"+serviceID+"/rollout", nil)
	response = httptest.NewRecorder()
	authed.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", response.Code, response.Body.String())
	}
	var rollout map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &rollout); err != nil {
		t.Fatalf("unmarshal rollout: %v", err)
	}
	if rollout["kind"] != "ServiceRollout" || rollout["targetVersion"] != "v2" {
		t.Fatalf("unexpected rollout document: %v", rollout)
	}

	// 3. A second movement while one runs is refused.
	request = httptest.NewRequest(http.MethodPost, "/v1/services/"+serviceID+"/rollout", bytes.NewReader(rolloutBody))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	authed.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d: %s", response.Code, response.Body.String())
	}

	// 4. The operator can roll the service back.
	request = httptest.NewRequest(http.MethodPost, "/v1/services/"+serviceID+"/rollback", nil)
	response = httptest.NewRecorder()
	authed.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", response.Code, response.Body.String())
	}
	var rolledBack map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &rolledBack); err != nil {
		t.Fatalf("unmarshal rollback response: %v", err)
	}
	backStatus, _ := rolledBack["status"].(map[string]any)
	// The rollback is recorded as a movement; it closes in the same step
	// when the previous replicas never left.
	if phase, _ := backStatus["rolloutPhase"].(string); phase != "ROLLING_BACK" && phase != "ROLLED_BACK" {
		t.Fatalf("expected the rollback to be recorded, got %v", backStatus["rolloutPhase"])
	}
	if backStatus["activeVersion"] != "v1" {
		t.Fatalf("expected the service to run v1 again, got %v", backStatus["activeVersion"])
	}
}

// The rollout surface is tenant scoped: another tenant neither sees the
// rollout nor moves the service.
func TestServiceRolloutIsTenantScoped(t *testing.T) {
	backend := newMemoryStore()
	serviceStore := supervisor.NewMemoryStore()
	sup := supervisor.NewSupervisor(serviceStore)

	handler := controlapi.NewHandler(backend, backend, backend, backend,
		controlapi.WithSupervisor(sup, serviceStore),
	)
	owner := auth.StaticMiddleware(auth.Principal{Subject: "user-1", TenantID: "tenant-a"}, handler)
	other := auth.StaticMiddleware(auth.Principal{Subject: "user-2", TenantID: "tenant-b"}, handler)

	createBody := []byte(`{"namespace": "default", "name": "scoped-svc", "agentId": "summarizer", "spec": {"replicas": 1}}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/services", bytes.NewReader(createBody))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	owner.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", response.Code, response.Body.String())
	}
	var created map[string]any
	_ = json.Unmarshal(response.Body.Bytes(), &created)
	serviceID, _ := created["id"].(string)

	rolloutBody := []byte(`{"newVersion": "v2"}`)
	request = httptest.NewRequest(http.MethodPost, "/v1/services/"+serviceID+"/rollout", bytes.NewReader(rolloutBody))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	other.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for another tenant, got %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/services/"+serviceID+"/rollout", nil)
	response = httptest.NewRecorder()
	other.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for another tenant, got %d: %s", response.Code, response.Body.String())
	}
}
