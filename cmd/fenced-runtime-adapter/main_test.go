package main

import "testing"

// The sandbox MCP listener brokers model, tool, memory and spawn access for
// sandboxed agents. Until it has its own SPIFFE/mTLS identity it must be
// loopback-only in every mode, including fully configured production mTLS.
func TestSandboxMCPListenerMustBeLoopbackInEveryMode(t *testing.T) {
	allowed := []string{
		"",
		"127.0.0.1:9093",
		"[::1]:9093",
		"localhost:9093",
	}
	for _, listen := range allowed {
		if err := validateSandboxMCPListen(listen); err != nil {
			t.Fatalf("loopback listen %q must be accepted: %v", listen, err)
		}
	}
	denied := []string{
		"0.0.0.0:9093",
		"::9093",
		":9093",
		"192.168.1.5:9093",
		"10.0.0.1:9093",
		"8.8.8.8:9093",
		"[fe80::1]:9093",
		"example.com:9093",
		"127.0.0.1",
		"mcp-listen",
	}
	for _, listen := range denied {
		if err := validateSandboxMCPListen(listen); err == nil {
			t.Fatalf("non-loopback listen %q must be rejected", listen)
		}
	}
}
