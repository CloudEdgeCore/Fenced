package main

import "testing"

// The sandbox MCP listener is a high-privilege brokered entry point; its
// loopback-only rule is enforced unconditionally in main. These cases pin
// the predicate for both the listener and the plaintext RPC endpoints.
func TestLoopbackEndpointPredicate(t *testing.T) {
	allowed := []string{
		"127.0.0.1:9090",
		"[::1]:9090",
		"localhost:9090",
	}
	for _, address := range allowed {
		if !loopbackEndpoint(address) {
			t.Fatalf("loopback address %q must be accepted", address)
		}
	}
	denied := []string{
		"0.0.0.0:9090",
		"::9090",
		":9090",
		"192.168.1.5:9090",
		"8.8.8.8:9090",
		"[fe80::1]:9090",
		"example.com:9090",
		"127.0.0.1",
	}
	for _, address := range denied {
		if loopbackEndpoint(address) {
			t.Fatalf("non-loopback address %q must be rejected", address)
		}
	}
}
