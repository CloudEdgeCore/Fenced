package main

import "testing"

// The command line must not carry its own definition of the loopback rule:
// the adapter package owns the transport policy, and the command delegates
// to it. A second implementation would be a second security boundary.
func TestLoopbackRPCAddressUsesTheAdapterPolicy(t *testing.T) {
	for _, address := range []string{
		"127.0.0.1:9090",
		"localhost:9090",
		"LocalHost:9090",
		"[::1]:9090",
	} {
		if !loopbackRPCAddress(address) {
			t.Fatalf("%q must be a loopback address", address)
		}
	}
	for _, address := range []string{
		"10.0.0.5:9090",
		"192.168.1.10:9090",
		"example.com:9090",
		// 0.0.0.0 is a wildcard bind, not a loopback address.
		"0.0.0.0:9090",
		// An address without a port is not a dialable endpoint, and an
		// unbracketed IPv6 literal is malformed.
		"127.0.0.1",
		"::1:9090",
		"",
		"not-an-address",
	} {
		if loopbackRPCAddress(address) {
			t.Fatalf("%q must not be treated as a loopback address", address)
		}
	}
}

// The sandbox MCP listener brokers model, tool, memory and spawn access for
// sandboxed agents, so it must never bind a non-loopback address - in any
// mode.
func TestValidateSandboxMCPListenRequiresLoopback(t *testing.T) {
	if err := validateSandboxMCPListen(""); err != nil {
		t.Fatalf("an empty listener is allowed: %v", err)
	}
	if err := validateSandboxMCPListen("127.0.0.1:9095"); err != nil {
		t.Fatalf("a loopback listener is allowed: %v", err)
	}
	if err := validateSandboxMCPListen("0.0.0.0:9095"); err == nil {
		t.Fatal("a wildcard listener must be rejected")
	}
	if err := validateSandboxMCPListen("10.0.0.5:9095"); err == nil {
		t.Fatal("a non-loopback listener must be rejected")
	}
}
