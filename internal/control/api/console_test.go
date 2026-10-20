package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// The operator read surface must never cross a tenant boundary: the list
// endpoints are the ones a console reads, and a console that showed another
// tenant's tasks would be a data leak with a friendly UI.
func TestListEndpointsAreTenantScoped(t *testing.T) {
	memory := newMemoryStore()
	own := uuid.New()
	other := uuid.New()
	memory.byID[own] = store.Task{ID: own, TenantID: "tenant-a", Namespace: "default", Phase: "RUNNING", Goal: "mine"}
	memory.byID[other] = store.Task{ID: other, TenantID: "tenant-b", Namespace: "default", Phase: "RUNNING", Goal: "theirs"}
	memory.versions = append(memory.versions, store.AgentVersion{ID: uuid.New(), TenantID: "tenant-a", Name: "mine", Version: "1"})
	memory.versions = append(memory.versions, store.AgentVersion{ID: uuid.New(), TenantID: "tenant-b", Name: "theirs", Version: "1"})

	handler := auth.StaticMiddleware(auth.Principal{Subject: "operator", TenantID: "tenant-a"},
		api.NewHandler(memory, memory, memory, nil))

	for _, path := range []string{"/v1/tasks", "/v1/agent-versions"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s: status = %d body = %s", path, response.Code, response.Body.String())
		}
		if body := response.Body.String(); containsString(body, "theirs") {
			t.Fatalf("%s: the response leaked another tenant: %s", path, body)
		}
	}
}

func TestListTasksFiltersByNamespace(t *testing.T) {
	memory := newMemoryStore()
	first := uuid.New()
	second := uuid.New()
	memory.byID[first] = store.Task{ID: first, TenantID: "tenant-a", Namespace: "default"}
	memory.byID[second] = store.Task{ID: second, TenantID: "tenant-a", Namespace: "other"}

	handler := auth.StaticMiddleware(auth.Principal{Subject: "operator", TenantID: "tenant-a"},
		api.NewHandler(memory, memory, memory, nil))
	request := httptest.NewRequest(http.MethodGet, "/v1/tasks?namespace=other", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", response.Code, response.Body.String())
	}
	var document struct {
		Tasks []struct {
			Namespace string `json:"namespace"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if len(document.Tasks) != 1 || document.Tasks[0].Namespace != "other" {
		t.Fatalf("tasks = %+v, want only the other namespace", document.Tasks)
	}
}

func TestListEndpointsRequireAuthentication(t *testing.T) {
	handler := api.NewHandler(newMemoryStore(), newMemoryStore(), newMemoryStore(), nil)
	for _, path := range []string{"/v1/tasks", "/v1/agent-versions", "/v1/approvals", "/v1/workflows", "/v1/runtime-pools"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, want 401 without a principal", path, response.Code)
		}
	}
}

// containsString reports whether the text holds the needle.
func containsString(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
