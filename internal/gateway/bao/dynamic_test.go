package bao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
)

// fakeDatabaseEngine simulates the OpenBao database secrets engine and the
// sys/leases lifecycle endpoints.
type fakeDatabaseEngine struct {
	token    string
	role     string
	renewals atomic.Int64
	revokes  atomic.Int64
	creds    atomic.Int64
	mu       sync.Mutex
	revoked  map[string]bool
}

func (f *fakeDatabaseEngine) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/database/creds/", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Vault-Token") != f.token {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		role := request.URL.Path[len("/v1/database/creds/"):]
		if role != f.role {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		f.creds.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"lease_id":       "database/creds/" + role + "/lease-" + itoa(f.creds.Load()),
			"lease_duration": 300, "renewable": true,
			"data": map[string]any{"username": "v-agent-" + itoa(f.creds.Load()), "password": "p-" + itoa(f.creds.Load())},
		})
	})
	mux.HandleFunc("/v1/sys/leases/renew", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Vault-Token") != f.token {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		f.renewals.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"lease_id": "renewed", "lease_duration": 300, "renewable": true})
	})
	mux.HandleFunc("/v1/sys/leases/revoke", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Vault-Token") != f.token {
			writer.WriteHeader(http.StatusForbidden)
			return
		}
		f.revokes.Add(1)
		var payload struct {
			LeaseID string `json:"lease_id"`
		}
		_ = json.NewDecoder(request.Body).Decode(&payload)
		f.mu.Lock()
		if f.revoked == nil {
			f.revoked = map[string]bool{}
		}
		f.revoked[payload.LeaseID] = true
		f.mu.Unlock()
		writer.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func itoa(value int64) string {
	return fmt.Sprintf("%d", value)
}

func newDynamicBroker(t *testing.T, fake *fakeDatabaseEngine) (*DynamicBroker, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(fake.handler())
	t.Cleanup(server.Close)
	broker, err := NewDynamicBroker(server.URL, fake.token, fake.role, WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	return broker, server
}

func TestDynamicIssueReturnsCredentials(t *testing.T) {
	fake := &fakeDatabaseEngine{token: "root", role: "dev-db"}
	broker, _ := newDynamicBroker(t, fake)
	handle, err := broker.Issue(context.Background(), tool.SecretScope{TenantID: "tenant-a", ToolName: "db.query", Resource: "postgres://prod"})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	var credentials DynamicCredentials
	if err := json.Unmarshal([]byte(handle), &credentials); err != nil {
		t.Fatalf("handle is not credentials JSON: %v", err)
	}
	if credentials.Username == "" || credentials.Password == "" {
		t.Fatalf("credentials = %+v", credentials)
	}
	if fake.creds.Load() != 1 {
		t.Fatalf("engine contacted %d times, want 1", fake.creds.Load())
	}
	if broker.OutstandingLeases() != 1 {
		t.Fatalf("outstanding leases = %d, want 1", broker.OutstandingLeases())
	}

	// Cached scope reuses the same credential without a new issuance.
	if _, err := broker.Issue(context.Background(), tool.SecretScope{TenantID: "tenant-a", ToolName: "db.query", Resource: "postgres://prod"}); err != nil {
		t.Fatalf("cached issue: %v", err)
	}
	if fake.creds.Load() != 1 {
		t.Fatalf("cached issue contacted the engine %d times", fake.creds.Load())
	}
}

func TestDynamicIssueFailClosed(t *testing.T) {
	fake := &fakeDatabaseEngine{token: "root", role: "dev-db"}
	broker, _ := newDynamicBroker(t, fake)
	// Unknown role: not found.
	other, err := NewDynamicBroker(broker.addr, "root", "ghost", WithHTTPClient(broker.client))
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	if _, err := other.Issue(context.Background(), tool.SecretScope{TenantID: "t", ToolName: "x", Resource: "y"}); !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("unknown role error = %v, want ErrSecretNotFound", err)
	}
	// Forbidden token: unavailable.
	denied := &fakeDatabaseEngine{token: "root", role: "dev-db"}
	server := httptest.NewServer(denied.handler())
	defer server.Close()
	deniedBroker, err := NewDynamicBroker(server.URL, "wrong-token", "dev-db", WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}
	if _, err := deniedBroker.Issue(context.Background(), tool.SecretScope{TenantID: "t", ToolName: "x", Resource: "y"}); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("forbidden error = %v, want ErrSecretUnavailable", err)
	}
}

func TestDynamicJanitorRenewsAndRevokes(t *testing.T) {
	fake := &fakeDatabaseEngine{token: "root", role: "dev-db"}
	broker, _ := newDynamicBroker(t, fake)
	clock := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	broker.now = func() time.Time { return clock }
	// 300s TTL at 70% → renew threshold at ~210s; renewAt=2min makes leases
	// due for renewal when <120s remain, i.e. after ~180s of the 210s window.
	broker.renewAt = 2 * time.Minute

	scope := tool.SecretScope{TenantID: "tenant-a", ToolName: "db.query", Resource: "r"}
	if _, err := broker.Issue(context.Background(), scope); err != nil {
		t.Fatalf("issue: %v", err)
	}

	// Advance into the renewal window and run the janitor.
	clock = clock.Add(3 * time.Minute)
	if err := broker.Janitor(context.Background()); err != nil {
		t.Fatalf("janitor: %v", err)
	}
	if fake.renewals.Load() != 1 {
		t.Fatalf("renewals = %d, want 1", fake.renewals.Load())
	}
	// Renewal extended the lease: it stays tracked.
	if broker.OutstandingLeases() != 1 {
		t.Fatalf("outstanding leases after renewal = %d, want 1", broker.OutstandingLeases())
	}

	// Close revokes everything.
	if err := broker.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	if fake.revokes.Load() != 1 {
		t.Fatalf("revokes = %d, want 1", fake.revokes.Load())
	}
	if broker.OutstandingLeases() != 0 {
		t.Fatalf("outstanding leases after close = %d, want 0", broker.OutstandingLeases())
	}
	// Issuing after close fails closed.
	if _, err := broker.Issue(context.Background(), scope); !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("issue after close error = %v, want ErrSecretUnavailable", err)
	}
}

func TestDynamicLeaseExpiryRevokes(t *testing.T) {
	fake := &fakeDatabaseEngine{token: "root", role: "dev-db"}
	broker, _ := newDynamicBroker(t, fake)
	clock := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	broker.now = func() time.Time { return clock }

	scope := tool.SecretScope{TenantID: "tenant-a", ToolName: "db.query", Resource: "r"}
	if _, err := broker.Issue(context.Background(), scope); err != nil {
		t.Fatalf("issue: %v", err)
	}
	// A non-renewable lease (renewal fails) is revoked once past its window.
	broker.mu.Lock()
	for _, lease := range broker.leases {
		lease.renewable = false
		lease.expiresAt = clock.Add(-time.Second)
	}
	broker.mu.Unlock()
	if err := broker.Janitor(context.Background()); err != nil {
		t.Fatalf("janitor: %v", err)
	}
	if fake.revokes.Load() != 1 {
		t.Fatalf("revokes = %d, want 1 (expired lease)", fake.revokes.Load())
	}
	if broker.OutstandingLeases() != 0 {
		t.Fatalf("outstanding leases = %d, want 0", broker.OutstandingLeases())
	}
}
