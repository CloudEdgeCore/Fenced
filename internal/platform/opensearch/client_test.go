package opensearch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeCluster simulates the OpenSearch REST surface the client uses.
type fakeCluster struct {
	mu      sync.Mutex
	docs    map[string][]byte
	indexed []string
	deleted []string
	fail    string // set to a status code to fail everything, e.g. "503"
}

func (f *fakeCluster) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/_cluster/health", func(writer http.ResponseWriter, request *http.Request) {
		if f.fail != "" {
			writer.WriteHeader(503)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"green"}`))
	})
	mux.HandleFunc("/fenced-memory", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/fenced-memory/_doc/", func(writer http.ResponseWriter, request *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.fail != "" {
			writer.WriteHeader(503)
			return
		}
		id := strings.TrimPrefix(request.URL.EscapedPath(), "/fenced-memory/_doc/")
		switch request.Method {
		case http.MethodGet:
			doc, ok := f.docs[id]
			if !ok {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write(doc)
		case http.MethodPut:
			body := make([]byte, 0, 1<<16)
			buffer := make([]byte, 1<<10)
			for {
				n, err := request.Body.Read(buffer)
				body = append(body, buffer[:n]...)
				if err != nil {
					break
				}
			}
			f.docs[id] = body
			f.indexed = append(f.indexed, id)
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"result":"created"}`))
		case http.MethodDelete:
			delete(f.docs, id)
			f.deleted = append(f.deleted, id)
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"result":"deleted"}`))
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return mux
}

func newClient(t *testing.T, fake *fakeCluster) *Client {
	t.Helper()
	server := httptest.NewServer(fake.handler())
	t.Cleanup(server.Close)
	client, err := New(server.URL, "fenced-memory", WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	return client
}

func TestIndexGetDeleteRoundTrip(t *testing.T) {
	fake := &fakeCluster{docs: map[string][]byte{}}
	client := newClient(t, fake)
	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := client.EnsureIndex(ctx, []byte(`{"mappings":{"properties":{}}}`)); err != nil {
		t.Fatalf("ensure index: %v", err)
	}
	if err := client.Index(ctx, "tenant-a/1", map[string]any{"tenantId": "tenant-a", "content": "hello", "resourceVersion": int64(3)}); err != nil {
		t.Fatalf("index: %v", err)
	}
	document, err := client.Get(ctx, "tenant-a/1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(document, &parsed); err != nil {
		t.Fatalf("decode stored document: %v", err)
	}
	if parsed["tenantId"] != "tenant-a" || parsed["resourceVersion"] != float64(3) {
		t.Fatalf("stored document = %+v", parsed)
	}
	if err := client.Delete(ctx, "tenant-a/1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.Get(ctx, "tenant-a/1"); err != ErrNotFound {
		t.Fatalf("get after delete error = %v, want ErrNotFound", err)
	}
	// Deleting a missing document is idempotent success.
	if err := client.Delete(ctx, "tenant-a/1"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}
	if _, err := client.Get(ctx, "missing"); err != ErrNotFound {
		t.Fatalf("get missing error = %v, want ErrNotFound", err)
	}
}

func TestTenantScopedDocumentIDRoundTrip(t *testing.T) {
	fake := &fakeCluster{docs: map[string][]byte{}}
	client := newClient(t, fake)
	ctx := context.Background()
	id := "tenant-a/3fa85f64-5717-4562-b3fc-2c963f66afa6"
	if err := client.Index(ctx, id, map[string]any{"tenantId": "tenant-a"}); err != nil {
		t.Fatalf("index tenant-scoped id: %v", err)
	}
	document, err := client.Get(ctx, id)
	if err != nil {
		t.Fatalf("get tenant-scoped id: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(document, &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if parsed["tenantId"] != "tenant-a" {
		t.Fatalf("document = %+v", parsed)
	}
	if err := client.Delete(ctx, id); err != nil {
		t.Fatalf("delete tenant-scoped id: %v", err)
	}
}

func TestUnavailableCluster(t *testing.T) {
	fake := &fakeCluster{fail: "503", docs: map[string][]byte{}}
	client := newClient(t, fake)
	ctx := context.Background()
	if err := client.Ping(ctx); err == nil {
		t.Fatal("ping must fail on an unavailable cluster")
	}
	if err := client.Index(ctx, "id", map[string]any{"x": 1}); err == nil {
		t.Fatal("index must fail on an unavailable cluster")
	}
	if _, err := client.Get(ctx, "id"); err == nil {
		t.Fatal("get must fail on an unavailable cluster")
	}
}
