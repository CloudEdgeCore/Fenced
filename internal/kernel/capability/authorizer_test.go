package capability

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

type versionStore struct {
	version store.AgentVersion
}

func (s versionStore) CreateAgentVersion(context.Context, store.CreateAgentVersionInput) (store.CreateAgentVersionResult, error) {
	return store.CreateAgentVersionResult{}, errors.New("not implemented")
}
func (s versionStore) GetAgentVersion(context.Context, string, uuid.UUID) (store.AgentVersion, error) {
	return store.AgentVersion{}, errors.New("not implemented")
}
func (s versionStore) GetAgentVersionByRef(_ context.Context, tenantID, ref string) (store.AgentVersion, error) {
	if tenantID != s.version.TenantID || ref != s.version.Ref() {
		return store.AgentVersion{}, store.ErrNotFound
	}
	return s.version, nil
}

func TestAuthorizerEnforcesExactAndWildcardGrants(t *testing.T) {
	spec := agentversion.Spec{
		Runtimes: []agentversion.RuntimeTarget{{Class: "remote"}},
		Capabilities: &agentversion.Capabilities{
			Tools: []string{"fs.read", "search.*"}, Models: []string{"model/quality"},
			Memory: []string{"project/*"}, Secrets: []string{},
		},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewAuthorizer(versionStore{version: store.AgentVersion{
		TenantID: "tenant-a", Name: "agent", Version: "1", Spec: encoded,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]struct {
		kind       Kind
		candidates []string
	}{
		"exact tool":     {Tool, []string{"fs.read"}},
		"versioned tool": {Tool, []string{"missing", "fs.read"}},
		"wildcard tool":  {Tool, []string{"search.web"}},
		"model":          {Model, []string{"model/quality"}},
		"memory":         {Memory, []string{"project/session"}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := authorizer.Authorize(context.Background(), "tenant-a", "agent@1", check.kind, check.candidates...); err != nil {
				t.Fatal(err)
			}
		})
	}
	if err := authorizer.Authorize(context.Background(), "tenant-a", "agent@1", Secret, "database/prod"); !errors.Is(err, ErrDenied) {
		t.Fatalf("undeclared secret error=%v", err)
	}
	if err := authorizer.Authorize(context.Background(), "tenant-b", "agent@1", Tool, "fs.read"); !errors.Is(err, ErrDenied) {
		t.Fatalf("cross-tenant error=%v", err)
	}
}

func TestAuthorizerPreservesLegacyV1Alpha1Publications(t *testing.T) {
	authorizer, err := NewAuthorizer(versionStore{version: store.AgentVersion{
		TenantID: "tenant-a", Name: "legacy", Version: "1",
		Spec: json.RawMessage("{\"runtimeClassPolicy\":{\"allowed\":[\"oci\"]}}"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(context.Background(), "tenant-a", "legacy@1", Tool, "legacy.tool"); err != nil {
		t.Fatalf("legacy compatibility was broken: %v", err)
	}
	// The same compatibility window means a legacy publication is not confined
	// by peer grants either: it may address any peer in its tenant. This is a
	// known gap, not an oversight, and it is asserted so that closing it is a
	// deliberate change rather than an accident.
	if err := authorizer.Authorize(context.Background(), "tenant-a", "legacy@1", IPC, "anyone@9"); err != nil {
		t.Fatalf("legacy peer compatibility was broken: %v", err)
	}
}

func TestAuthorizerEnforcesIPCPeerGrants(t *testing.T) {
	spec := agentversion.Spec{
		Runtimes: []agentversion.RuntimeTarget{{Class: "remote"}},
		Capabilities: &agentversion.Capabilities{
			Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{},
			Peers: []string{"worker@1", "team/*", "reporter@*"},
		},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewAuthorizer(versionStore{version: store.AgentVersion{
		TenantID: "tenant-a", Name: "planner", Version: "1", Spec: encoded,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for name, candidates := range map[string][]string{
		"exact peer":       {"worker@1"},
		"namespace peer":   {"team/analyst@1"},
		"version float":    {"reporter@9"},
		"one of several":   {"stranger@1", "worker@1"},
		"granted on retry": {"missing", "worker@1"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := authorizer.Authorize(context.Background(), "tenant-a", "planner@1", IPC, candidates...); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, candidates := range map[string][]string{
		"ungranted peer": {"stranger@1"},
		"empty address":  {""},
		// A version-float grant must not leak into a name-adjacent peer.
		"name adjacent": {"reporter2@1"},
		// A namespace wildcard is a suffix match, not a substring one.
		"substring": {"xteam/analyst@1"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := authorizer.Authorize(context.Background(), "tenant-a", "planner@1", IPC, candidates...); !errors.Is(err, ErrDenied) {
				t.Fatalf("expected denial, error=%v", err)
			}
		})
	}
}

func TestAuthorizerDeniesPeersWhenTheGrantIsAbsent(t *testing.T) {
	spec := agentversion.Spec{
		Runtimes: []agentversion.RuntimeTarget{{Class: "remote"}},
		Capabilities: &agentversion.Capabilities{
			Tools: []string{}, Models: []string{}, Memory: []string{}, Secrets: []string{},
		},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewAuthorizer(versionStore{version: store.AgentVersion{
		TenantID: "tenant-a", Name: "planner", Version: "1", Spec: encoded,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := authorizer.Authorize(context.Background(), "tenant-a", "planner@1", IPC, "worker@1"); !errors.Is(err, ErrDenied) {
		t.Fatalf("absent peer grant must deny, error=%v", err)
	}
}
