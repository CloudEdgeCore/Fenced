package ipc

import (
	"testing"
)

// The address carries the agent version: the frozen IPC contract names it
// agent_version_ref, and without it an address cannot say which version of
// an agent a message belongs to.
func TestAddressCarriesTheAgentVersion(t *testing.T) {
	address := NewVersionedAddress("tenant-a", "default", "worker", "worker@2.0.0")
	if address.AgentVersionRef != "worker@2.0.0" {
		t.Fatalf("version = %q", address.AgentVersionRef)
	}
	if got := address.String(); got != "agent://tenant-a/default/worker@2.0.0" {
		t.Fatalf("canonical form = %q", got)
	}

	instance := NewInstanceAddressForVersion("tenant-a", "default", "worker", "worker@2.0.0", "run-1")
	if got := instance.String(); got != "agent://tenant-a/default/worker@2.0.0/run-1" {
		t.Fatalf("canonical form = %q", got)
	}
	parsed, err := ParseAddress(instance.String())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.AgentVersionRef != "worker@2.0.0" || parsed.InstanceID != "run-1" {
		t.Fatalf("parsed = %+v", parsed)
	}
	if parsed.AgentID != "worker" {
		t.Fatalf("the version leaked into the agent name: %+v", parsed)
	}
}

// Two versions are the same when they name the same publication. The
// namespace is part of that identity: prod/reports@1 and default/reports@1
// are two different publications.
func TestVersionComparison(t *testing.T) {
	if !SameVersion("reports@1.0.0", "default/reports@1.0.0") {
		t.Fatal("the default namespace is the same version")
	}
	if SameVersion("prod/reports@1.0.0", "default/reports@1.0.0") {
		t.Fatal("two namespaces are two versions")
	}
	if !SameVersion("", "") {
		t.Fatal("two empty versions are the same")
	}
	if SameVersion("reports@1.0.0", "") {
		t.Fatal("a version and nothing are not the same")
	}
}

// A message for one version does not match an address of another version.
func TestAddressMatchingRespectsTheVersion(t *testing.T) {
	first := NewVersionedAddress("tenant-a", "default", "worker", "worker@1.0.0")
	second := NewVersionedAddress("tenant-a", "default", "worker", "worker@2.0.0")
	if first.Matches(second) {
		t.Fatal("two versions must not match")
	}
	if !first.Matches(first) {
		t.Fatal("an address must match itself")
	}
	// An address without a version names the agent, and matches any version
	// of it: that is what a logical lookup does.
	logical := NewAddress("tenant-a", "default", "worker")
	if !logical.Matches(first) || !logical.Matches(second) {
		t.Fatal("a logical address must match every version")
	}
	// The logical form keeps the version, so the versions of an agent stay
	// apart.
	if first.Logical().AgentVersionRef != "worker@1.0.0" {
		t.Fatalf("the logical form lost the version: %+v", first.Logical())
	}
}

// The canonical form of an address is stable and reversible.
func TestAddressRoundTrip(t *testing.T) {
	for _, raw := range []string{
		"agent://tenant-a/default/worker",
		"agent://tenant-a/default/worker@2.0.0",
		"agent://tenant-a/default/worker@2.0.0/run-1",
	} {
		parsed, err := ParseAddress(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if parsed.String() != raw {
			t.Fatalf("round trip: %q became %q", raw, parsed.String())
		}
	}
}
