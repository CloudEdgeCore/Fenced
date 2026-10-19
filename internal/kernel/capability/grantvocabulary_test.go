package capability

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

func toolVersionStore(t *testing.T, tools, toolVersions []string, createdAt time.Time) *Authorizer {
	t.Helper()
	spec := agentversion.Spec{
		Runtimes: []agentversion.RuntimeTarget{{Class: "remote"}},
		Capabilities: &agentversion.Capabilities{
			Tools: tools, ToolVersions: toolVersions,
			Models: []string{}, Memory: []string{}, Secrets: []string{},
		},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewAuthorizer(versionStore{version: store.AgentVersion{
		TenantID: "tenant-a", Name: "agent", Version: "1", Spec: encoded, CreatedAt: createdAt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return authorizer
}

// A publication that pins a tool version must still authorize the tool at
// runtime: the capability check compares bare tool names, the pinned version
// belongs to the freeze. Before the two vocabularies were separated, such a
// publication was denied every agent-facing tool call.
func TestAuthorizerAllowsPinnedToolGrant(t *testing.T) {
	published := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	authorizer := toolVersionStore(t, []string{"echo.dev@1.0.0"}, nil, published)

	if err := authorizer.Authorize(context.Background(), "tenant-a", "agent@1", Tool, "echo.dev"); err != nil {
		t.Fatalf("pinned tool grant must authorize the tool: %v", err)
	}
	if err := authorizer.Authorize(context.Background(), "tenant-a", "agent@1", Tool, "other.dev"); !errors.Is(err, ErrDenied) {
		t.Fatalf("ungranted tool error=%v", err)
	}
}

// A normalized publication keeps the capability vocabulary in tools and the
// pinned form in toolVersions; both must authorize the same tool.
func TestAuthorizerAllowsNormalizedToolGrant(t *testing.T) {
	published := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	authorizer := toolVersionStore(t, []string{"echo.dev"}, []string{"echo.dev@1.0.0"}, published)

	if err := authorizer.Authorize(context.Background(), "tenant-a", "agent@1", Tool, "echo.dev"); err != nil {
		t.Fatalf("normalized tool grant must authorize the tool: %v", err)
	}
}

// A floating grant accepts any version of the tool and opts out of the
// publish-time freeze.
func TestAuthorizerAllowsFloatingToolGrant(t *testing.T) {
	published := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	authorizer := toolVersionStore(t, []string{"echo.dev@*"}, nil, published)

	if err := authorizer.Authorize(context.Background(), "tenant-a", "agent@1", Tool, "echo.dev"); err != nil {
		t.Fatalf("floating tool grant must authorize the tool: %v", err)
	}
}

// The freeze keeps its meaning for both vocabularies: a pinned grant allows
// exactly its version even when it was registered after the publication, and
// a bare name stays frozen to the publish-time version set.
func TestToolFreezeKeepsPinnedAndFrozenSemantics(t *testing.T) {
	published := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	beforePublish := published.Add(-time.Hour)
	afterPublish := published.Add(time.Hour)

	pinned := toolVersionStore(t, []string{"echo.dev@1.0.0"}, nil, published)
	freeze, err := pinned.ToolFreeze(context.Background(), "tenant-a", "agent@1")
	if err != nil {
		t.Fatal(err)
	}
	if !freeze.Enforced() {
		t.Fatal("the freeze must apply to a portable publication")
	}
	if !freeze.Allow("echo.dev", "1.0.0", afterPublish) {
		t.Fatal("a pinned version must be allowed even when it was registered after the publication")
	}
	if freeze.Allow("echo.dev", "1.0.1", afterPublish) {
		t.Fatal("a pinned version must not allow a different version")
	}

	bare := toolVersionStore(t, []string{"echo.dev"}, nil, published)
	freeze, err = bare.ToolFreeze(context.Background(), "tenant-a", "agent@1")
	if err != nil {
		t.Fatal(err)
	}
	if !freeze.Allow("echo.dev", "1.0.0", beforePublish) {
		t.Fatal("a bare name must allow the versions that existed at publication")
	}
	if freeze.Allow("echo.dev", "1.0.1", afterPublish) {
		t.Fatal("a bare name must stay frozen to the publish-time version set")
	}

	floating := toolVersionStore(t, []string{"echo.dev@*"}, nil, published)
	freeze, err = floating.ToolFreeze(context.Background(), "tenant-a", "agent@1")
	if err != nil {
		t.Fatal(err)
	}
	if !freeze.Allow("echo.dev", "9.9.9", afterPublish) {
		t.Fatal("a floating grant must accept any version of the tool")
	}
}
