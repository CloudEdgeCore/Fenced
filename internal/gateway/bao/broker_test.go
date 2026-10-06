package bao

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
)

// fakeBao simulates an OpenBao KV v2 endpoint.
type fakeBao struct {
	secrets  map[string]map[string]any
	requests atomic.Int64
	token    string
}

func (f *fakeBao) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sys/health", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Vault-Token") != f.token {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/", func(writer http.ResponseWriter, request *http.Request) {
		f.requests.Add(1)
		if request.Header.Get("X-Vault-Token") != f.token {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		path := strings.TrimPrefix(request.URL.EscapedPath(), "/v1/secret/data/")
		secret, ok := f.secrets[path]
		if !ok {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"data": map[string]any{"data": secret}})
	})
	return mux
}

func newBroker(t *testing.T, fake *fakeBao) (*Broker, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(fake.handler())
	t.Cleanup(server.Close)
	broker, err := NewBroker(server.URL, fake.token, WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	return broker, server
}

func scope() tool.SecretScope {
	return tool.SecretScope{TenantID: "tenant-a", AttemptID: "attempt-1", ToolName: "fs.read", Resource: "fs:/tmp/notes"}
}

func TestIssueReadsValueKey(t *testing.T) {
	fake := &fakeBao{token: "root", secrets: map[string]map[string]any{
		"fenced/tenant-a/fs.read/fs:%2Ftmp%2Fnotes": {"value": "scoped-credential"},
	}}
	broker, _ := newBroker(t, fake)
	handle, err := broker.Issue(context.Background(), scope())
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if handle != "scoped-credential" {
		t.Fatalf("handle = %q, want the scoped credential", handle)
	}
}

func TestIssueRendersJSONDataWithoutValueKey(t *testing.T) {
	fake := &fakeBao{token: "root", secrets: map[string]map[string]any{
		"fenced/tenant-a/fs.read/fs:%2Ftmp%2Fnotes": {"username": "svc", "host": "db.internal"},
	}}
	broker, _ := newBroker(t, fake)
	handle, err := broker.Issue(context.Background(), scope())
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	var rendered map[string]string
	if err := json.Unmarshal([]byte(handle), &rendered); err != nil {
		t.Fatalf("handle is not the rendered secret JSON: %v", err)
	}
	if rendered["username"] != "svc" || rendered["host"] != "db.internal" {
		t.Fatalf("rendered = %+v", rendered)
	}
}

func TestIssueFailClosed(t *testing.T) {
	broker, _ := newBroker(t, &fakeBao{token: "root"})

	if _, err := broker.Issue(context.Background(), scope()); !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("missing secret error = %v, want ErrSecretNotFound", err)
	}
	denied := &fakeBao{token: "root"}
	denied.secrets = map[string]map[string]any{"fenced/tenant-a/fs.read/fs:%2Ftmp%2Fnotes": {"value": "x"}}
	broker, _ = newBroker(t, denied)
	denied.token = "different"
	if _, err := broker.Issue(context.Background(), scope()); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("forbidden read error = %v, want ErrSecretUnavailable", err)
	}
	// A dead endpoint surfaces as unavailable, not a panic.
	dead, err := NewBroker("http://127.0.0.1:1", "root", WithHTTPClient(&http.Client{Timeout: 200 * time.Millisecond}))
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	if _, err := dead.Issue(context.Background(), scope()); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("dead endpoint error = %v, want ErrSecretUnavailable", err)
	}
}

func TestIssueCachesWithinTTL(t *testing.T) {
	fake := &fakeBao{token: "root", secrets: map[string]map[string]any{
		"fenced/tenant-a/fs.read/fs:%2Ftmp%2Fnotes": {"value": "scoped-credential"},
	}}
	broker, _ := newBroker(t, fake)
	clock := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	broker.now = func() time.Time { return clock }
	broker.cacheTTL = time.Minute

	for i := 0; i < 3; i++ {
		handle, err := broker.Issue(context.Background(), scope())
		if err != nil || handle != "scoped-credential" {
			t.Fatalf("issue %d: handle=%q err=%v", i, handle, err)
		}
	}
	if fake.requests.Load() != 1 {
		t.Fatalf("broker was contacted %d times within the TTL, want 1", fake.requests.Load())
	}

	// After the TTL the broker is consulted again.
	clock = clock.Add(2 * time.Minute)
	if _, err := broker.Issue(context.Background(), scope()); err != nil {
		t.Fatalf("issue after TTL: %v", err)
	}
	if fake.requests.Load() != 2 {
		t.Fatalf("broker was contacted %d times after TTL, want 2", fake.requests.Load())
	}
}

func TestIssueScopesAreIsolated(t *testing.T) {
	fake := &fakeBao{token: "root", secrets: map[string]map[string]any{
		"fenced/tenant-a/fs.read/fs:%2Ftmp%2Fnotes": {"value": "tenant-a-secret"},
		"fenced/tenant-b/fs.read/fs:%2Ftmp%2Fnotes": {"value": "tenant-b-secret"},
	}}
	broker, _ := newBroker(t, fake)
	tenantA, err := broker.Issue(context.Background(), tool.SecretScope{TenantID: "tenant-a", ToolName: "fs.read", Resource: "fs:/tmp/notes"})
	if err != nil {
		t.Fatalf("issue tenant-a: %v", err)
	}
	tenantB, err := broker.Issue(context.Background(), tool.SecretScope{TenantID: "tenant-b", ToolName: "fs.read", Resource: "fs:/tmp/notes"})
	if err != nil {
		t.Fatalf("issue tenant-b: %v", err)
	}
	if tenantA == tenantB {
		t.Fatal("different tenants must never share a secret handle")
	}
}

func TestPing(t *testing.T) {
	broker, _ := newBroker(t, &fakeBao{token: "root"})
	if err := broker.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	denied := &fakeBao{token: "root"}
	broker, _ = newBroker(t, denied)
	denied.token = "wrong"
	if err := broker.Ping(context.Background()); err == nil {
		t.Fatal("ping with a rejected token must fail")
	}
}

func TestScopePathEscapesResource(t *testing.T) {
	broker, err := NewBroker("http://bao:8200", "root", WithNamespace("team-a"), WithMount("/secret/"))
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	path := broker.scopePath(tool.SecretScope{TenantID: "tenant-a", ToolName: "db.query", Resource: "postgres://prod"})
	if path != "fenced/tenant-a/db.query/postgres:%2F%2Fprod" {
		t.Fatalf("scope path = %q", path)
	}
}
