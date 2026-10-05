package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToolServiceEnforcesSecretCapability(t *testing.T) {
	authorizer := testAuthorizer(t, agentversion.Capabilities{
		Tools: []string{"fs.read"}, Models: []string{}, Memory: []string{}, Secrets: []string{"database/read"},
	})
	invoker := &fakeInvoker{result: tool.InvokeResult{Outcome: tool.OutcomeExecuted}}
	service := NewService(invoker, "tenant-a", authorizer)

	denied := invokeRequest(t, "")
	denied.ToolVersion = "1.0.0"
	denied.SecretRef = "database/admin"
	if _, err := service.InvokeTool(context.Background(), denied); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("undeclared secret: %v, want PermissionDenied", err)
	}
	if invoker.input.ToolName != "" {
		t.Fatal("denied secret reached the tool decision chain")
	}

	allowed := invokeRequest(t, "")
	allowed.ToolVersion = "1.0.0"
	allowed.SecretRef = "database/read"
	if _, err := service.InvokeTool(context.Background(), allowed); err != nil {
		t.Fatalf("declared secret: %v", err)
	}
	if invoker.input.SecretRef != "database/read" {
		t.Fatalf("secret grant not propagated: %+v", invoker.input)
	}
}

func TestMemoryServiceFencesAndAuthorizesReadWrite(t *testing.T) {
	authorizer := testAuthorizer(t, agentversion.Capabilities{
		Tools: []string{}, Models: []string{}, Memory: []string{"project:read", "project:write"}, Secrets: []string{},
	})
	taskID, runID := uuid.New(), uuid.New()
	fence := &fakeMemoryFence{assignment: store.RuntimeAssignment{
		Task: store.Task{ID: taskID, TenantID: "tenant-a", AgentVersionRef: "agent@1"},
		Run:  store.Run{ID: runID}, Attempt: store.Attempt{ID: attemptID},
	}}
	memories := &fakeMemoryInvoker{}
	service := NewMemoryService(memories, fence, "tenant-a", authorizer)
	identity := &gatewayv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: attemptID.String(), FencingToken: 7}

	put, err := service.PutMemory(context.Background(), &gatewayv1.PutMemoryRequest{
		Identity: identity, AgentVersionRef: "agent@1", Namespace: "project", Key: "status",
		ContentType: "text/plain", Content: "ready", Sensitivity: "internal", ProvenanceJson: []byte(`{"source":"test"}`),
	})
	if err != nil {
		t.Fatalf("PutMemory: %v", err)
	}
	if put.GetRecord().GetContent() != "ready" || memories.put.SourceAttemptID == nil || *memories.put.SourceAttemptID != attemptID {
		t.Fatalf("write was not provenance-bound: response=%+v input=%+v", put, memories.put)
	}

	search, err := service.SearchMemory(context.Background(), &gatewayv1.SearchMemoryRequest{
		Identity: identity, AgentVersionRef: "agent@1", Namespace: "project", Query: "ready", Limit: 5,
	})
	if err != nil || len(search.GetRecords()) != 1 {
		t.Fatalf("SearchMemory: response=%+v error=%v", search, err)
	}

	_, err = service.SearchMemory(context.Background(), &gatewayv1.SearchMemoryRequest{
		Identity: identity, AgentVersionRef: "agent@1", Namespace: "private", Query: "ready", Limit: 5,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("undeclared namespace: %v, want PermissionDenied", err)
	}
	if memories.searchCalls != 1 {
		t.Fatal("denied namespace reached canonical memory")
	}
}

func TestMemoryServiceRejectsStaleAndMismatchedAttempt(t *testing.T) {
	authorizer := testAuthorizer(t, agentversion.Capabilities{
		Tools: []string{}, Models: []string{}, Memory: []string{"*"}, Secrets: []string{},
	})
	identity := &gatewayv1.AttemptIdentity{TenantId: "tenant-a", AttemptId: attemptID.String(), FencingToken: 1}
	service := NewMemoryService(&fakeMemoryInvoker{}, &fakeMemoryFence{err: store.ErrFenced}, "tenant-a", authorizer)
	_, err := service.SearchMemory(context.Background(), &gatewayv1.SearchMemoryRequest{
		Identity: identity, AgentVersionRef: "agent@1", Namespace: "project", Query: "x", Limit: 1,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale fence: %v", err)
	}

	service = NewMemoryService(&fakeMemoryInvoker{}, &fakeMemoryFence{assignment: store.RuntimeAssignment{
		Task: store.Task{AgentVersionRef: "other@1"}, Attempt: store.Attempt{ID: attemptID},
	}}, "tenant-a", authorizer)
	_, err = service.SearchMemory(context.Background(), &gatewayv1.SearchMemoryRequest{
		Identity: identity, AgentVersionRef: "agent@1", Namespace: "project", Query: "x", Limit: 1,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("mismatched publication: %v", err)
	}
}

type gatewayVersionStore struct{ version store.AgentVersion }

func (s gatewayVersionStore) CreateAgentVersion(context.Context, store.CreateAgentVersionInput) (store.CreateAgentVersionResult, error) {
	return store.CreateAgentVersionResult{}, errors.New("not implemented")
}
func (s gatewayVersionStore) GetAgentVersion(context.Context, string, uuid.UUID) (store.AgentVersion, error) {
	return store.AgentVersion{}, errors.New("not implemented")
}
func (s gatewayVersionStore) GetAgentVersionByRef(_ context.Context, tenant, ref string) (store.AgentVersion, error) {
	if tenant != s.version.TenantID || ref != s.version.Ref() {
		return store.AgentVersion{}, store.ErrNotFound
	}
	return s.version, nil
}

func testAuthorizer(t *testing.T, capabilities agentversion.Capabilities) *capability.Authorizer {
	t.Helper()
	spec, err := json.Marshal(agentversion.Spec{
		Runtimes: []agentversion.RuntimeTarget{{Class: "remote"}}, Capabilities: &capabilities,
	})
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := capability.NewAuthorizer(gatewayVersionStore{version: store.AgentVersion{
		TenantID: "tenant-a", Name: "agent", Version: "1", Spec: spec,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return authorizer
}

type fakeMemoryFence struct {
	assignment store.RuntimeAssignment
	err        error
}

func (f *fakeMemoryFence) GetRuntimeAssignment(context.Context, string, uuid.UUID, int64) (store.RuntimeAssignment, error) {
	return f.assignment, f.err
}

type fakeMemoryInvoker struct {
	put         memory.PutInput
	searchCalls int
}

func (f *fakeMemoryInvoker) Put(_ context.Context, input memory.PutInput) (store.MemoryRecord, bool, error) {
	f.put = input
	now := time.Now()
	return store.MemoryRecord{
		ID: uuid.New(), TenantID: input.TenantID, Namespace: input.Namespace, Key: input.Key,
		ContentType: input.ContentType, Content: input.Content, Sensitivity: input.Sensitivity,
		ResourceVersion: 1, CreatedAt: now, UpdatedAt: now,
	}, false, nil
}

func (f *fakeMemoryInvoker) Search(_ context.Context, input memory.SearchInput) ([]store.MemoryRecord, error) {
	f.searchCalls++
	now := time.Now()
	return []store.MemoryRecord{{
		ID: uuid.New(), TenantID: input.TenantID, Namespace: input.Namespace, Key: "status",
		ContentType: "text/plain", Content: "ready", Sensitivity: "internal", ResourceVersion: 1,
		CreatedAt: now, UpdatedAt: now,
	}}, nil
}

// freezeAuthorizer publishes one AgentVersion "agent@1" at publishedAt with the
// given tool grants, so a test can exercise the P1-08 publish-time freeze.
func freezeAuthorizer(t *testing.T, tools []string, publishedAt time.Time) *capability.Authorizer {
	t.Helper()
	spec, err := json.Marshal(agentversion.Spec{
		Runtimes:     []agentversion.RuntimeTarget{{Class: "remote"}},
		Capabilities: &agentversion.Capabilities{Tools: tools, Models: []string{}, Memory: []string{}, Secrets: []string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := capability.NewAuthorizer(gatewayVersionStore{version: store.AgentVersion{
		TenantID: "tenant-a", Name: "agent", Version: "1", Spec: spec, CreatedAt: publishedAt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return authorizer
}

// TestToolServiceFreezesToolVersionsAtPublish is the P1-08 acceptance: an
// AgentVersion published before a newer tool version is registered keeps
// resolving the version that existed at publication, unless its grant floats.
func TestToolServiceFreezesToolVersionsAtPublish(t *testing.T) {
	publishedAt := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	descriptors := []store.ToolDescriptor{
		{TenantID: "tenant-a", Name: "weather", Version: "1.0.0", SideEffectRisk: store.ToolRiskLow,
			Actions: []string{"read"}, ParamsSchema: json.RawMessage(`{"type":"object"}`),
			CreatedAt: publishedAt.Add(-time.Hour)},
		{TenantID: "tenant-a", Name: "weather", Version: "2.0.0", SideEffectRisk: store.ToolRiskLow,
			Actions: []string{"read"}, ParamsSchema: json.RawMessage(`{"type":"object"}`),
			CreatedAt: publishedAt.Add(time.Hour)}, // registered AFTER the AgentVersion
	}
	visible := func(grant string) map[string]bool {
		service := NewService(&fakeInvoker{descriptors: descriptors}, "tenant-a", freezeAuthorizer(t, []string{grant}, publishedAt))
		resp, err := service.ListTools(context.Background(), &gatewayv1.ListToolsRequest{TenantId: "tenant-a", AgentVersionRef: "agent@1"})
		if err != nil {
			t.Fatalf("ListTools(%q): %v", grant, err)
		}
		seen := map[string]bool{}
		for _, descriptor := range resp.GetTools() {
			seen[descriptor.GetVersion()] = true
		}
		return seen
	}
	invoke := func(grant, version string) error {
		service := NewService(&fakeInvoker{descriptors: descriptors, result: tool.InvokeResult{Outcome: tool.OutcomeExecuted}},
			"tenant-a", freezeAuthorizer(t, []string{grant}, publishedAt))
		request := invokeRequest(t, "")
		request.ToolName, request.ToolVersion = "weather", version
		_, err := service.InvokeTool(context.Background(), request)
		return err
	}

	// Bare grant: frozen to the publish-time version set.
	if seen := visible("weather"); !seen["1.0.0"] || seen["2.0.0"] {
		t.Fatalf("bare grant visibility = %v, want only 1.0.0", seen)
	}
	if err := invoke("weather", "1.0.0"); err != nil {
		t.Fatalf("bare grant invoke 1.0.0: %v", err)
	}
	if err := invoke("weather", "2.0.0"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("bare grant invoke 2.0.0: %v, want PermissionDenied (frozen out)", err)
	}
	// A freeze forces the caller to name the frozen version rather than let the
	// store resolve its own latest.
	if err := invoke("weather", ""); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("bare grant invoke unversioned: %v, want PermissionDenied", err)
	}

	// Floating "name@*" grant: the newer version drifts in by design, and
	// the listing still shows exactly one entry per name (P1-02) — the
	// latest granted version, which is what a bare-name call resolves to.
	if seen := visible("weather@*"); !seen["2.0.0"] || seen["1.0.0"] {
		t.Fatalf("floating grant visibility = %v, want only the resolved latest 2.0.0", seen)
	}
	if err := invoke("weather@*", "2.0.0"); err != nil {
		t.Fatalf("floating grant invoke 2.0.0: %v", err)
	}

	// Pinned "name@version" grant: exactly that version, at any registration time.
	if seen := visible("weather@2.0.0"); seen["1.0.0"] || !seen["2.0.0"] {
		t.Fatalf("pinned grant visibility = %v, want only 2.0.0", seen)
	}
	if err := invoke("weather@2.0.0", "1.0.0"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("pinned grant invoke 1.0.0: %v, want PermissionDenied", err)
	}
}
