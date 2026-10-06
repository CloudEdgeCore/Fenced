package memory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func TestHTTPEmbedderRequiresHTTPSAndValidatesVector(t *testing.T) {
	if _, err := NewHTTPEmbedder("http://example.com/embed", "", nil); err == nil {
		t.Fatal("plaintext embedding endpoint was accepted")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer secret" {
			t.Error("embedding bearer credential was not injected")
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"embedding": make([]float32, store.MemoryEmbeddingDimension)})
	}))
	defer server.Close()
	embedder, err := NewHTTPEmbedder(server.URL, "secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	vector, err := embedder.Embed(context.Background(), "content")
	if err != nil || len(vector) != store.MemoryEmbeddingDimension {
		t.Fatalf("dimensions=%d err=%v", len(vector), err)
	}
}
