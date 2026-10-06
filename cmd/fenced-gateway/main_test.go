package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProductionEndpointConfigurationFailsClosed(t *testing.T) {
	for _, endpoint := range []string{"https://bao.example", "https://bao.example/v1"} {
		if !isHTTPSURL(endpoint) {
			t.Fatalf("valid HTTPS endpoint %q rejected", endpoint)
		}
	}
	for _, endpoint := range []string{"", "http://bao.example", "https://user@bao.example", "https://bao.example#fragment"} {
		if isHTTPSURL(endpoint) {
			t.Fatalf("unsafe endpoint %q accepted", endpoint)
		}
	}

	path := filepath.Join(t.TempDir(), "endpoints.json")
	if err := os.WriteFile(path, []byte(`{"endpoints":{"search@1.0.0":"https://tools.example/search"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	endpoints, err := loadToolEndpoints(path)
	if err != nil || endpoints["search@1.0.0"] == "" {
		t.Fatalf("endpoints=%v err=%v", endpoints, err)
	}
	if err := os.WriteFile(path, []byte(`{"endpoints":{},"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadToolEndpoints(path); err == nil {
		t.Fatal("unknown production configuration was accepted")
	}
}
