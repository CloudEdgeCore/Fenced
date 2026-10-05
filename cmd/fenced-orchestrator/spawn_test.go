package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	runtimeadapter "github.com/CloudEdgeCore/Fenced/internal/runtime/adapter"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeSpawnWorkflowStore struct {
	kernelstore.WorkflowStore
	workflow kernelstore.Workflow
	input    kernelstore.SpawnWorkflowStepInput
	spawns   int
}

func (f *fakeSpawnWorkflowStore) GetWorkflow(_ context.Context, tenant string, id uuid.UUID) (kernelstore.Workflow, error) {
	if tenant != f.workflow.TenantID || id != f.workflow.ID {
		return kernelstore.Workflow{}, kernelstore.ErrWorkflowNotFound
	}
	return f.workflow, nil
}

func (f *fakeSpawnWorkflowStore) SpawnWorkflowStep(_ context.Context, input kernelstore.SpawnWorkflowStepInput) (kernelstore.SpawnWorkflowStepResult, error) {
	f.input = input
	f.spawns++
	return kernelstore.SpawnWorkflowStepResult{
		Created: true,
		Step:    kernelstore.WorkflowStep{Name: input.Name, SpawnDepth: 1},
	}, nil
}

type fakeSpawnRuntimeStore struct {
	assignment kernelstore.RuntimeAssignment
	workflowID uuid.UUID
	step       string
	err        error
}

type fakeSpawnIdentityResolver struct{ identity mcp.AttemptContext }

func (f fakeSpawnIdentityResolver) Resolve(context.Context) (mcp.AttemptContext, error) {
	return f.identity, nil
}

func (f *fakeSpawnRuntimeStore) GetRuntimeAssignment(_ context.Context, _ string, _ uuid.UUID, _ int64) (kernelstore.RuntimeAssignment, error) {
	return f.assignment, f.err
}

func (f *fakeSpawnRuntimeStore) WorkflowLineage(context.Context, string, uuid.UUID) (uuid.UUID, string, int64, bool, error) {
	return f.workflowID, f.step, 7, f.workflowID != uuid.Nil, nil
}

func validSpawnServiceFixture() (*spawnService, *fakeSpawnWorkflowStore, *runtimev1.SpawnStepRequest) {
	workflowID, attemptID := uuid.New(), uuid.New()
	workflows := &fakeSpawnWorkflowStore{workflow: kernelstore.Workflow{
		ID: workflowID, TenantID: "tenant-a", ResourceVersion: 7,
		Spec: []byte(`{
			"defaultTaskSpec":{"priority":50},
			"budget":{"maxTasks":16},
			"runtime":{"dynamic":{"enabled":true,"maxDynamicSteps":8,"maxChildrenPerStep":4,"maxSpawnDepth":3,"maxWorkflowSteps":16}},
			"deadline":"2099-01-01T00:00:00Z",
			"steps":[{"name":"planner","agentVersionRef":"planner@1","goal":"plan"}]
		}`),
	}}
	runtimeStore := &fakeSpawnRuntimeStore{
		workflowID: workflowID, step: "planner",
		assignment: kernelstore.RuntimeAssignment{
			Task: kernelstore.Task{ID: uuid.New(), TenantID: "tenant-a", WorkflowID: &workflowID,
				WorkflowStepName: "planner", WorkflowAttempt: 1, Phase: domain.TaskRunning},
			Attempt: kernelstore.Attempt{ID: attemptID, TenantID: "tenant-a", FencingToken: 9, Phase: domain.AttemptRunning},
			Lease:   kernelstore.Lease{ExpiresAt: time.Now().UTC().Add(time.Minute)},
			AgentVersion: &kernelstore.AgentVersion{Spec: []byte(`{
				"capabilities":{"tools":[],"models":[],"memory":[],"secrets":[],
				"spawnTasks":true,"childAgents":["worker@1"]}
			}`)},
		},
	}
	service := &spawnService{workflows: workflows, runtime: runtimeStore}
	request := &runtimev1.SpawnStepRequest{
		Identity:   &runtimev1.AttemptIdentity{TenantId: "tenant-a", AttemptId: attemptID.String(), FencingToken: 9},
		WorkflowId: workflowID.String(), ParentStep: "planner", Name: "child",
		Goal: "execute child", AgentVersionRef: "worker@1", SpecJson: `{}`,
		IdempotencyKey: "spawn-1", ArgumentsJson: `{}`,
	}
	return service, workflows, request
}

func TestSpawnServiceAuthorizesFencedAttemptAndImmutableCapabilities(t *testing.T) {
	service, workflows, request := validSpawnServiceFixture()
	response, err := service.SpawnStep(context.Background(), request)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if response.GetOutcome() != "created" || workflows.spawns != 1 {
		t.Fatalf("response=%+v spawns=%d", response, workflows.spawns)
	}
	if workflows.input.TenantID != "tenant-a" || workflows.input.WorkflowVersion != 7 ||
		workflows.input.ParentStepName != "planner" || workflows.input.AgentVersionRef != "worker@1" {
		t.Fatalf("authoritative spawn input = %+v", workflows.input)
	}
	if string(workflows.input.Spec) != `{"priority":50}` {
		t.Fatalf("dynamic child did not inherit workflow defaults: %s", workflows.input.Spec)
	}
}

func TestSpawnServiceDeniesStaleLineageAndUnauthorizedChildren(t *testing.T) {
	service, workflows, request := validSpawnServiceFixture()
	runtimeStore := service.runtime.(*fakeSpawnRuntimeStore)
	runtimeStore.err = kernelstore.ErrFenced
	if _, err := service.SpawnStep(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale attempt status = %v", err)
	}
	runtimeStore.err = nil
	runtimeStore.step = "other-parent"
	if _, err := service.SpawnStep(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("lineage mismatch status = %v", err)
	}
	runtimeStore.step = "planner"
	request.AgentVersionRef = "admin@1"
	if _, err := service.SpawnStep(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("child allowlist status = %v", err)
	}
	if workflows.spawns != 0 {
		t.Fatalf("denied requests reached store: %d", workflows.spawns)
	}
}

func TestSpawnServiceDeniesTerminalAndExpiredAttempts(t *testing.T) {
	service, workflows, request := validSpawnServiceFixture()
	runtimeStore := service.runtime.(*fakeSpawnRuntimeStore)
	runtimeStore.assignment.Attempt.Phase = domain.AttemptCompleted
	if _, err := service.SpawnStep(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("terminal attempt status = %v", err)
	}
	runtimeStore.assignment.Attempt.Phase = domain.AttemptRunning
	runtimeStore.assignment.Lease.ExpiresAt = time.Now().UTC().Add(-time.Second)
	if _, err := service.SpawnStep(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expired lease status = %v", err)
	}
	if workflows.spawns != 0 {
		t.Fatalf("inactive attempts reached store: %d", workflows.spawns)
	}
}

func TestSpawnVerticalSliceBrokerGrpcAndAuthoritativeService(t *testing.T) {
	service, workflows, request := validSpawnServiceFixture()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	runtimev1.RegisterWorkflowSpawnServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial spawn service: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	identity := mcp.AttemptContext{
		TenantID: "tenant-a", AttemptID: uuid.MustParse(request.GetIdentity().GetAttemptId()), FencingToken: 9,
		WorkflowID: uuid.MustParse(request.GetWorkflowId()), ParentStepName: "planner",
		AgentVersionRef: "planner@1", CanSpawnTasks: true, AllowedChildAgents: []string{"worker@1"},
	}
	broker := mcp.NewBroker(nil, nil, nil,
		runtimeadapter.NewGrpcWorkflowSpawner(runtimev1.NewWorkflowSpawnServiceClient(connection)),
		nil, fakeSpawnIdentityResolver{identity: identity})
	result, rpcErr := broker.CallTool(context.Background(), []byte(`{
		"name":"fenced.task.spawn",
		"arguments":{"name":"child","goal":"execute child","agentVersionRef":"worker@1"}
	}`))
	if rpcErr != nil {
		t.Fatalf("broker spawn RPC error: %+v", rpcErr)
	}
	if workflows.spawns != 1 || workflows.input.TenantID != identity.TenantID ||
		workflows.input.ParentStepName != identity.ParentStepName || workflows.input.AgentVersionRef != "worker@1" {
		t.Fatalf("vertical spawn did not preserve authoritative identity: result=%+v input=%+v", result, workflows.input)
	}
}

// TestSpawnServiceMTLSPeerTenantBinding proves the Phase 2 remote-runtime
// binding at the spawn boundary: with a SPIFFE trust domain configured, the
// verified peer SVID's tenant must match the request's tenant claim. A
// correct tenant passes, a cross-tenant SVID is denied, and a non-TLS peer
// is unauthenticated.
func TestSpawnServiceMTLSPeerTenantBinding(t *testing.T) {
	now := time.Now()
	ca, err := spiffe.NewCA("fenced.dev", now, 24*time.Hour)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	service, _, request := validSpawnServiceFixture()
	service.spiffeTrustDomain = "fenced.dev"

	peerContextFor := func(svid tls.Certificate) context.Context {
		leaf, err := x509.ParseCertificate(svid.Certificate[0])
		if err != nil {
			t.Fatalf("parse SVID leaf: %v", err)
		}
		return peer.NewContext(context.Background(), &peer.Peer{
			AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{leaf},
			}},
		})
	}

	// A tenant-a worker SVID (matching the request claim) passes the identity
	// boundary; the rest of the spawn flow runs (fixture is valid).
	tenantASVID, err := ca.IssueSVID("tenant-a", "remote-1", now, time.Hour)
	if err != nil {
		t.Fatalf("issue tenant-a SVID: %v", err)
	}
	if _, err := service.SpawnStep(peerContextFor(tenantASVID), request); err != nil {
		t.Fatalf("authorized remote runtime spawn: %v", err)
	}

	// A tenant-b SVID impersonating tenant-a is denied.
	tenantBSVID, err := ca.IssueSVID("tenant-b", "remote-1", now, time.Hour)
	if err != nil {
		t.Fatalf("issue tenant-b SVID: %v", err)
	}
	if _, err := service.SpawnStep(peerContextFor(tenantBSVID), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant remote runtime = %v, want PermissionDenied", err)
	}

	// An SVID from an untrusted CA is rejected at the identity check.
	rogueCA, err := spiffe.NewCA("evil.example", now, time.Hour)
	if err != nil {
		t.Fatalf("create rogue CA: %v", err)
	}
	rogueSVID, err := rogueCA.IssueSVID("tenant-a", "remote-1", now, time.Hour)
	if err != nil {
		t.Fatalf("issue rogue SVID: %v", err)
	}
	if _, err := service.SpawnStep(peerContextFor(rogueSVID), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("rogue-CA remote runtime = %v, want PermissionDenied", err)
	}

	// A non-TLS peer (no verified identity) is unauthenticated.
	if _, err := service.SpawnStep(context.Background(), request); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("non-TLS peer = %v, want Unauthenticated", err)
	}
}
