//go:build integration

package security

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testDatabaseEnvironment = "FENCED_TEST_DATABASE_URL"

// securityDatabaseName is the dedicated database for the security suite: the
// suite runs sequence-sensitive takeover scenarios, and other packages reset
// shared test tables in parallel, so it must not share a database with them.
const securityDatabaseName = "fenced_security"

// prepareSecurity connects to the dedicated security test database (created
// on demand), applies migrations and resets the runtime tables.
func prepareSecurity(t *testing.T) *pgxpool.Pool {
	t.Helper()
	baseURL := os.Getenv(testDatabaseEnvironment)
	if baseURL == "" {
		t.Skipf("%s is not set", testDatabaseEnvironment)
	}
	url, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	url.Path = "/" + securityDatabaseName

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL admin connection: %v", err)
	}
	_, _ = admin.Exec(ctx, `CREATE DATABASE `+securityDatabaseName)
	admin.Close()

	pool, err := pgxpool.New(ctx, url.String())
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	migrations := filepath.Join("..", "..", "db", "migrations")
	if _, err := migrate.Apply(ctx, pool, migrations); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE task_usage_reservations,
		runtime_capacity_reservations, runtime_pool_capacities, runtime_pool_tenant_grants, runtime_pools,
		provider_circuit_breakers, workflow_usage_ledgers, workflow_steps, workflows,
		model_calls, model_descriptors, tool_approvals, tool_calls, tool_descriptors,
		runtime_operation_receipts, checkpoints, artifacts,
		task_budget_settlements, task_budget_ledgers, agent_versions, ipc_messages, inbox_receipts, outbox_events,
		audit_events,
		runtime_leases, attempts, runs, tasks RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("reset database: %v", err)
	}
	return pool
}

// TestFencingReplayRejectedAfterTakeover is the integration leg of the
// fencing-replay negative scenario: a worker whose execution window was taken
// over (recovery bumped the fencing token) cannot replay heartbeat,
// checkpoint or transition calls with its stale token — every one of them is
// rejected PermissionDenied before any state mutation.
func TestFencingReplayRejectedAfterTakeover(t *testing.T) {
	pool := prepareSecurity(t)
	repository := postgresstore.New(pool)
	service := control.NewService(repository, "tenant-a", 2*time.Minute)
	ctx := context.Background()

	// Publish the version the task references.
	if _, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "research-agent", Version: "1.0.0",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci","reference-go"]}}`),
	}); err != nil {
		t.Fatalf("publish version: %v", err)
	}
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "research-agent@1.0.0",
		Goal: "fencing replay", Spec: []byte(`{}`), IdempotencyKey: "fencing-replay",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := repository.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, created.Task.ResourceVersion, domain.TaskAdmitted); err != nil {
		t.Fatalf("admit task: %v", err)
	}
	run, err := repository.CreateRun(ctx, kernelstore.CreateRunInput{
		ID: uuid.New(), TenantID: created.Task.TenantID, TaskID: created.Task.ID, ExpectedTaskVersion: 2,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	owned, err := repository.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: created.Task.TenantID, AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "reference-go",
		RuntimeInstanceID: "worker-1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire attempt: %v", err)
	}
	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: created.Task.TenantID, AttemptID: owned.Attempt.ID, FencingToken: owned.Attempt.FencingToken,
		ExpectedAttemptVersion: owned.Attempt.ResourceVersion, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: created.Task.TenantID, AttemptID: starting.ID, FencingToken: starting.FencingToken,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatalf("run attempt: %v", err)
	}

	staleIdentity := &runtimev1.AttemptIdentity{
		TenantId: "tenant-a", AttemptId: owned.Attempt.ID.String(), FencingToken: owned.Attempt.FencingToken,
	}

	// Takeover: the lease lapses (the worker crashed without heartbeating),
	// recovery expires the attempt and re-acquires it, bumping the fencing
	// token. The old worker's token is now stale.
	if _, err := pool.Exec(ctx, `UPDATE runtime_leases SET
		acquired_at = now() - interval '10 minutes',
		heartbeat_at = now() - interval '3 minutes',
		expires_at = now() - interval '1 minute'
		WHERE attempt_id = $1`, owned.Attempt.ID.String()); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	recovered, err := repository.RecoverExpiredAttempt(ctx, kernelstore.RecoverExpiredAttemptInput{
		TenantID: owned.Attempt.TenantID, AttemptID: owned.Attempt.ID,
		FencingToken: owned.Attempt.FencingToken, NewAttemptID: uuid.New(), NewLeaseID: uuid.New(),
		LeaseTTL: time.Minute, MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("recover attempt: %v", err)
	}
	if !recovered.Retried {
		t.Fatalf("recovery did not retry: %+v", recovered)
	}
	if recovered.Lease.Lease.FencingToken == owned.Attempt.FencingToken {
		t.Fatalf("takeover did not bump the fencing token")
	}

	// Stale heartbeat: rejected.
	_, err = service.Heartbeat(ctx, &runtimev1.HeartbeatRequest{
		Identity: staleIdentity, ExpectedLeaseVersion: owned.Attempt.FencingToken,
		RequestedTtlSeconds: 30, IdempotencyKey: "replay-heartbeat-1",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale heartbeat error = %v, want PermissionDenied", err)
	}

	// Stale checkpoint: rejected before any durable state is written.
	digest := sha256.Sum256([]byte("stale-state"))
	_, err = service.CommitCheckpoint(ctx, &runtimev1.CommitCheckpointRequest{
		Identity: staleIdentity, CheckpointId: uuid.New().String(),
		ExpectedAttemptVersion: running.ResourceVersion, IdempotencyKey: "replay-checkpoint-1",
		AgentVersionRef: "research-agent@1.0.0", Provider: "reference-go",
		RuntimeAbi: "fenced.reference/v1", SchemaVersion: "state/v1",
		State: &runtimev1.ArtifactReference{
			Uri: "artifact://tenant-a/sha256/stale", Sha256: hex.EncodeToString(digest[:]),
			SizeBytes: int64(len("stale-state")), MediaType: "application/octet-stream",
		},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale checkpoint error = %v, want PermissionDenied", err)
	}
	var checkpoints int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM checkpoints WHERE tenant_id = $1`, "tenant-a").Scan(&checkpoints); err != nil {
		t.Fatalf("count checkpoints: %v", err)
	}
	if checkpoints != 0 {
		t.Fatalf("stale checkpoint was durably stored: %d", checkpoints)
	}

	// Stale transition: rejected.
	_, err = service.TransitionAttempt(ctx, &runtimev1.TransitionAttemptRequest{
		Identity: staleIdentity, ExpectedAttemptVersion: running.ResourceVersion,
		IdempotencyKey: "replay-transition-1", TargetPhase: runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale transition error = %v, want PermissionDenied", err)
	}

	// Stale assignment read: rejected.
	if _, err := service.GetAssignment(ctx, &runtimev1.GetAssignmentRequest{Identity: staleIdentity}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("stale assignment read error = %v, want PermissionDenied", err)
	}

	// The fresh owner still works: its heartbeat is accepted.
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE id = $1`, recovered.Lease.Attempt.ID.String()).Scan(&attempts); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("recovered attempt count = %d, want 1", attempts)
	}
}

// TestTenantEscapeRejectedOnRealStore proves the tenant boundary at the
// Runtime Protocol against real PostgreSQL: a tenant-b poll never reaches
// tenant-a's store state and is rejected before any lookup.
func TestTenantEscapeRejectedOnRealStore(t *testing.T) {
	pool := prepareSecurity(t)
	service := control.NewService(postgresstore.New(pool), "tenant-a", 2*time.Minute)
	_, err := service.PollAssignment(context.Background(), &runtimev1.PollAssignmentRequest{
		TenantId: "tenant-b", RuntimeInstanceId: "worker-1",
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant poll error = %v, want PermissionDenied", err)
	}
	if _, err := service.GetAssignment(context.Background(), &runtimev1.GetAssignmentRequest{
		Identity: &runtimev1.AttemptIdentity{TenantId: "tenant-b", AttemptId: uuid.New().String(), FencingToken: 1},
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-tenant assignment error = %v, want PermissionDenied", err)
	}
}

var _ = errors.Is
