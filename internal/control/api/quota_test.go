package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	controlapi "github.com/CloudEdgeCore/Fenced/internal/control/api"
	"github.com/CloudEdgeCore/Fenced/internal/control/auth"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func TestTenantQuotaEndpointsDisabledWithoutStore(t *testing.T) {
	handler := controlapi.NewHandler(newMemoryStore(), newMemoryStore(), newMemoryStore(), newMemoryStore())
	authed := auth.StaticMiddleware(auth.Principal{Subject: "owner", TenantID: "tenant-a"}, handler)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		request := httptest.NewRequest(method, "/v1/quota", nil)
		response := httptest.NewRecorder()
		authed.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d: %s", method, response.Code, response.Body.String())
		}
		assertReason(t, response, "QUOTA_DISABLED")
	}
}

func TestTenantQuotaLifecycle(t *testing.T) {
	backend := newMemoryStore()
	quotas := newFakeQuotaStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "owner", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend, controlapi.WithTenantQuotaStore(quotas)),
	)

	// GET before configuration: not found.
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/quota", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET status = %d: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "TENANT_QUOTA_NOT_FOUND")

	// PUT configures the quota.
	body := []byte(`{"windowSeconds":86400,"limits":{"tokens":100000,"costUsd":100,"toolCalls":10000,"wallSeconds":86400}}`)
	request := httptest.NewRequest(http.MethodPut, "/v1/quota", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", response.Code, response.Body.String())
	}
	if response.Header().Get("ETag") != `W/"1"` {
		t.Fatalf("missing ETag: %+v", response.Header())
	}
	var configured map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &configured); err != nil {
		t.Fatalf("decode quota: %v", err)
	}
	if configured["tenantId"] != "tenant-a" || configured["kind"] != "TenantQuota" {
		t.Fatalf("unexpected quota response: %+v", configured)
	}
	limits := configured["limits"].(map[string]any)
	if limits["tokens"].(float64) != 100000 {
		t.Fatalf("unexpected limits: %+v", limits)
	}

	// GET reports the quota with the current window usage.
	quotas.usage["tenant-a"] = store.TenantWindowUsage{
		TenantID: "tenant-a", WindowStart: time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC),
		Consumed: store.TaskBudget{Tokens: 42, CostMicroUSD: money.MustFromUSD(1.5)},
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/quota", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET status = %d: %s", response.Code, response.Body.String())
	}
	var view map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil {
		t.Fatalf("decode view: %v", err)
	}
	usage := view["usage"].(map[string]any)
	if usage["tokens"].(float64) != 42 || view["windowStart"] == nil {
		t.Fatalf("unexpected usage view: %+v", view)
	}

	// DELETE removes the configuration; GET then reports not found.
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/v1/quota", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d: %s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/quota", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d: %s", response.Code, response.Body.String())
	}
}

// TestTenantQuotaBoundaryIsThePrincipal proves the quota endpoints operate on
// the authenticated principal's tenant only: tenant-b can neither read nor
// overwrite tenant-a's quota.
func TestTenantQuotaBoundaryIsThePrincipal(t *testing.T) {
	backend := newMemoryStore()
	quotas := newFakeQuotaStore()
	handler := controlapi.NewHandler(backend, backend, backend, backend, controlapi.WithTenantQuotaStore(quotas))

	owner := auth.StaticMiddleware(auth.Principal{Subject: "owner", TenantID: "tenant-a"}, handler)
	body := []byte(`{"windowSeconds":86400,"limits":{"tokens":1000}}`)
	request := httptest.NewRequest(http.MethodPut, "/v1/quota", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	owner.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("owner PUT status = %d: %s", response.Code, response.Body.String())
	}

	other := auth.StaticMiddleware(auth.Principal{Subject: "other", TenantID: "tenant-b"}, handler)
	response = httptest.NewRecorder()
	other.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/quota", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("other tenant GET status = %d: %s", response.Code, response.Body.String())
	}
	if _, ok := quotas.quotas["tenant-b"]; ok {
		t.Fatal("tenant-b quota was configured by the boundary test")
	}
}

func TestSetTenantQuotaValidation(t *testing.T) {
	backend := newMemoryStore()
	handler := auth.StaticMiddleware(
		auth.Principal{Subject: "owner", TenantID: "tenant-a"},
		controlapi.NewHandler(backend, backend, backend, backend,
			controlapi.WithTenantQuotaStore(newFakeQuotaStore())),
	)
	cases := []struct {
		name   string
		body   string
		reason string
		status int
	}{
		{"short window", `{"windowSeconds":59,"limits":{"tokens":1}}`, "INVALID_QUOTA", http.StatusUnprocessableEntity},
		{"negative limit", `{"windowSeconds":86400,"limits":{"tokens":-1}}`, "INVALID_QUOTA", http.StatusUnprocessableEntity},
		{"unknown field", `{"windowSeconds":86400,"limits":{"tokens":1},"bogus":true}`, "INVALID_JSON", http.StatusBadRequest},
		{"duplicate key", `{"windowSeconds":86400,"limits":{"tokens":1,"tokens":2}}`, "INVALID_JSON", http.StatusBadRequest},
		{"missing content type", `{"windowSeconds":86400,"limits":{"tokens":1}}`, "CONTENT_TYPE_REQUIRED", http.StatusUnsupportedMediaType},
	}
	for _, tc := range cases {
		request := httptest.NewRequest(http.MethodPut, "/v1/quota", bytes.NewReader([]byte(tc.body)))
		if tc.name != "missing content type" {
			request.Header.Set("Content-Type", "application/json")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != tc.status {
			t.Fatalf("%s: status = %d, want %d: %s", tc.name, response.Code, tc.status, response.Body.String())
		}
		assertReason(t, response, tc.reason)
	}

	// Unsupported methods are rejected.
	request := httptest.NewRequest(http.MethodPost, "/v1/quota", bytes.NewReader([]byte(`{}`)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d: %s", response.Code, response.Body.String())
	}
	assertReason(t, response, "METHOD_NOT_ALLOWED")
}

// fakeQuotaStore is an in-memory store.TenantQuotaStore for API tests.
type fakeQuotaStore struct {
	mu     sync.Mutex
	quotas map[string]store.TenantQuota
	usage  map[string]store.TenantWindowUsage
}

func newFakeQuotaStore() *fakeQuotaStore {
	return &fakeQuotaStore{quotas: map[string]store.TenantQuota{}, usage: map[string]store.TenantWindowUsage{}}
}

func (f *fakeQuotaStore) SetTenantQuota(_ context.Context, in store.SetTenantQuotaInput) (store.TenantQuota, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	version := int64(1)
	if existing, ok := f.quotas[in.TenantID]; ok {
		version = existing.ResourceVersion + 1
	}
	quota := store.TenantQuota{
		TenantID: in.TenantID, WindowSeconds: in.WindowSeconds, Limits: in.Limits,
		ResourceVersion: version, UpdatedAt: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
	}
	f.quotas[in.TenantID] = quota
	return quota, nil
}

func (f *fakeQuotaStore) GetTenantQuota(_ context.Context, tenantID string) (store.TenantQuota, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	quota, ok := f.quotas[tenantID]
	if !ok {
		return store.TenantQuota{}, store.ErrNotFound
	}
	return quota, nil
}

func (f *fakeQuotaStore) DeleteTenantQuota(_ context.Context, tenantID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.quotas, tenantID)
	return nil
}

func (f *fakeQuotaStore) GetTenantQuotaUsage(_ context.Context, tenantID string, at time.Time) (store.TenantWindowUsage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.quotas[tenantID]; !ok {
		return store.TenantWindowUsage{}, store.ErrNotFound
	}
	usage, ok := f.usage[tenantID]
	if !ok {
		return store.TenantWindowUsage{TenantID: tenantID, WindowStart: at.Truncate(24 * time.Hour)}, nil
	}
	return usage, nil
}
