//go:build integration

package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestAgentVersionPublishResolveAndImmutability(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	created, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "research-agent", Version: "1.3.0",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["oci"],"preferred":"oci"},"lifecycle":{"maxAttempts":5}}`),
	})
	if err != nil {
		t.Fatalf("publish agent version: %v", err)
	}
	if created.Existing || created.AgentVersion.ResourceVersion != 1 || created.AgentVersion.Ref() != "research-agent@1.3.0" {
		t.Fatalf("unexpected publication: %+v", created)
	}

	byID, err := repository.GetAgentVersion(ctx, "tenant-a", created.AgentVersion.ID)
	if err != nil || byID.ID != created.AgentVersion.ID {
		t.Fatalf("get by id: %+v err=%v", byID, err)
	}
	byRef, err := repository.GetAgentVersionByRef(ctx, "tenant-a", "research-agent@1.3.0")
	if err != nil || byRef.ID != created.AgentVersion.ID {
		t.Fatalf("get by ref: %+v err=%v", byRef, err)
	}

	// Identical spec replay is idempotent even with a fresh identity.
	replayed, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "research-agent", Version: "1.3.0",
		Spec: []byte(`{"lifecycle":{"maxAttempts":5},"runtimeClassPolicy":{"allowed":["oci"],"preferred":"oci"}}`),
	})
	if err != nil || !replayed.Existing || replayed.AgentVersion.ID != created.AgentVersion.ID {
		t.Fatalf("idempotent replay: %+v err=%v", replayed, err)
	}

	// The same identity with a different spec conflicts.
	conflict, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "research-agent", Version: "1.3.0",
		Spec: []byte(`{"runtimeClassPolicy":{"allowed":["microvm"]}}`),
	})
	if !errors.Is(err, kernelstore.ErrAgentVersionConflict) {
		t.Fatalf("expected version conflict, got %+v err=%v", conflict, err)
	}

	// Tenant boundary: the same name/version may exist per tenant, and a
	// cross-tenant lookup must not leak existence.
	other, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-b", Namespace: "default",
		Name: "research-agent", Version: "1.3.0", Spec: []byte(`{}`),
	})
	if err != nil || other.Existing {
		t.Fatalf("tenant-b publish: %+v err=%v", other, err)
	}
	if _, err := repository.GetAgentVersion(ctx, "tenant-b", created.AgentVersion.ID); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("cross-tenant read leaked: %v", err)
	}

	// A published spec is immutable at the database level.
	if _, err := repository.GetAgentVersionByRef(ctx, "tenant-a", "not-a-ref"); !errors.Is(err, kernelstore.ErrAgentVersionRefInvalid) {
		t.Fatalf("expected ref-invalid error, got %v", err)
	}
}

func TestAgentVersionNamespaceIsPartOfIdentity(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	// P1-07: the namespace is a k8s-style isolation boundary that is part of the
	// publication identity. Two teams sharing one tenant may publish the same
	// name@version in different namespaces without colliding.
	spec := []byte(`{"runtimeClassPolicy":{"allowed":["oci"],"preferred":"oci"}}`)
	defaultVersion, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "shared-agent", Version: "1.0.0", Spec: spec,
	})
	if err != nil {
		t.Fatalf("publish default namespace: %v", err)
	}
	teamVersion, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "team-a",
		Name: "shared-agent", Version: "1.0.0", Spec: spec,
	})
	if err != nil {
		t.Fatalf("publish team-a namespace: %v", err)
	}
	if teamVersion.Existing || teamVersion.AgentVersion.ID == defaultVersion.AgentVersion.ID {
		t.Fatalf("second namespace collided with the first: %+v", teamVersion)
	}

	// The default namespace is elided in the canonical reference, so the two
	// identities render and resolve distinctly.
	if got := defaultVersion.AgentVersion.Ref(); got != "shared-agent@1.0.0" {
		t.Fatalf("default namespace ref = %q, want elided form", got)
	}
	if got := teamVersion.AgentVersion.Ref(); got != "team-a/shared-agent@1.0.0" {
		t.Fatalf("team-a ref = %q, want namespaced form", got)
	}

	byDefault, err := repository.GetAgentVersionByRef(ctx, "tenant-a", "shared-agent@1.0.0")
	if err != nil || byDefault.ID != defaultVersion.AgentVersion.ID {
		t.Fatalf("resolve default ref: %+v err=%v", byDefault, err)
	}
	byTeam, err := repository.GetAgentVersionByRef(ctx, "tenant-a", "team-a/shared-agent@1.0.0")
	if err != nil || byTeam.ID != teamVersion.AgentVersion.ID {
		t.Fatalf("resolve namespaced ref: %+v err=%v", byTeam, err)
	}

	// A namespace with no publication is not silently served from another.
	if _, err := repository.GetAgentVersionByRef(ctx, "tenant-a", "team-b/shared-agent@1.0.0"); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("empty namespace leaked a publication: %v", err)
	}

	// Re-publishing within one namespace with a different spec still conflicts:
	// widening the identity did not weaken per-namespace immutability.
	if _, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "team-a",
		Name: "shared-agent", Version: "1.0.0", Spec: []byte(`{"runtimeClassPolicy":{"allowed":["microvm"]}}`),
	}); !errors.Is(err, kernelstore.ErrAgentVersionConflict) {
		t.Fatalf("expected per-namespace conflict, got %v", err)
	}
}

func TestAgentVersionPersistsPackageSignatureEnvelope(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	envelope := &kernelstore.PackageSignature{
		KeyID: "ci-builder-1", Signature: "c2lnbmF0dXJl", ManifestDigest: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
	}
	created, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "research-agent", Version: "2.0.0",
		Spec:             []byte(`{"runtimeClassPolicy":{"allowed":["oci"]}}`),
		PackageSignature: envelope,
	})
	if err != nil {
		t.Fatalf("publish signed agent version: %v", err)
	}
	if created.AgentVersion.PackageSignature == nil || *created.AgentVersion.PackageSignature != *envelope {
		t.Fatalf("published signature = %+v, want %+v", created.AgentVersion.PackageSignature, envelope)
	}

	byID, err := repository.GetAgentVersion(ctx, "tenant-a", created.AgentVersion.ID)
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if byID.PackageSignature == nil || *byID.PackageSignature != *envelope {
		t.Fatalf("read-back signature = %+v, want %+v", byID.PackageSignature, envelope)
	}
	byRef, err := repository.GetAgentVersionByRef(ctx, "tenant-a", "research-agent@2.0.0")
	if err != nil {
		t.Fatalf("get by ref: %v", err)
	}
	if byRef.PackageSignature == nil || byRef.PackageSignature.KeyID != envelope.KeyID {
		t.Fatalf("ref read-back signature = %+v, want key id %s", byRef.PackageSignature, envelope.KeyID)
	}

	// An idempotent replay returns the stored envelope, not the new input.
	replayed, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "research-agent", Version: "2.0.0",
		Spec:             []byte(`{"runtimeClassPolicy":{"allowed":["oci"]}}`),
		PackageSignature: &kernelstore.PackageSignature{KeyID: "intruder", Signature: "x", ManifestDigest: "y"},
	})
	if err != nil || !replayed.Existing {
		t.Fatalf("idempotent replay: %+v err=%v", replayed, err)
	}
	if replayed.AgentVersion.PackageSignature == nil || replayed.AgentVersion.PackageSignature.KeyID != "ci-builder-1" {
		t.Fatalf("replay signature = %+v, want stored ci-builder-1 envelope", replayed.AgentVersion.PackageSignature)
	}

	// Unsigned publications round-trip with a nil envelope.
	unsigned, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-b", Namespace: "default",
		Name: "agent", Version: "1", Spec: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("publish unsigned: %v", err)
	}
	if unsigned.AgentVersion.PackageSignature != nil {
		t.Fatalf("unsigned publication carried a signature envelope: %+v", unsigned.AgentVersion.PackageSignature)
	}

	// The database itself rejects a partial envelope (all fields or none).
	if _, err := pool.Exec(ctx, `INSERT INTO agent_versions (
		id, tenant_id, namespace, name, version, spec, spec_digest,
		package_key_id, resource_version, created_at
	) VALUES ($1, 'tenant-c', 'default', 'agent', '2', '{}'::jsonb, decode(repeat('00', 32), 'hex'), 'lone-key', 1, now())`,
		uuid.New().String()); err == nil {
		t.Fatal("partial package signature envelope was accepted")
	}
}

func TestAgentVersionRowsRejectMutation(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	published := publishVersion(t, ctx, repository, "tenant-a", "agent", "1", `{"lifecycle":{"maxAttempts":3}}`)

	if _, err := pool.Exec(ctx, `UPDATE agent_versions SET spec = '{}'::jsonb WHERE id = $1`, published.AgentVersion.ID.String()); err == nil {
		t.Fatal("UPDATE of an immutable agent version succeeded")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM agent_versions WHERE id = $1`, published.AgentVersion.ID.String()); err == nil {
		t.Fatal("DELETE of an immutable agent version succeeded")
	}
	var stillThere int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_versions WHERE id = $1`, published.AgentVersion.ID.String()).Scan(&stillThere); err != nil {
		t.Fatalf("count agent versions: %v", err)
	}
	if stillThere != 1 {
		t.Fatalf("agent version disappeared: count=%d", stillThere)
	}
}

func TestCheckpointRejectsUnpublishedAgentVersion(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	// A task referencing a version that was never published. Admission would
	// reject it, but a faulty or compromised runtime must still be unable to
	// checkpoint against an unknown publication.
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "ghost@1",
		Goal: "unpublished version", Spec: []byte(`{}`), IdempotencyKey: "ghost-checkpoint",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	admitted, err := repository.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, created.Task.ResourceVersion, domain.TaskAdmitted)
	if err != nil {
		t.Fatalf("admit task: %v", err)
	}
	run, err := repository.CreateRun(ctx, kernelstore.CreateRunInput{
		ID: uuid.New(), TenantID: admitted.TenantID, TaskID: admitted.ID, ExpectedTaskVersion: admitted.ResourceVersion,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	owned, err := repository.AcquireAttempt(ctx, kernelstore.AcquireAttemptInput{
		TenantID: admitted.TenantID, AttemptID: uuid.New(), LeaseID: uuid.New(), RunID: run.ID,
		ExpectedRunVersion: run.ResourceVersion, RuntimeClass: "oci", RuntimeInstanceID: "worker-1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("acquire attempt: %v", err)
	}
	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: admitted.TenantID, AttemptID: owned.Attempt.ID, FencingToken: owned.Attempt.FencingToken,
		ExpectedAttemptVersion: owned.Attempt.ResourceVersion, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: admitted.TenantID, AttemptID: starting.ID, FencingToken: starting.FencingToken,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatalf("run attempt: %v", err)
	}

	digest := sha256.Sum256([]byte("ghost-state"))
	_, _, err = repository.CommitCheckpoint(ctx, kernelstore.CommitCheckpointInput{
		TenantID: "tenant-a", AttemptID: running.ID, FencingToken: running.FencingToken,
		ExpectedAttemptVersion: running.ResourceVersion, IdempotencyKey: "ghost-checkpoint-1", CheckpointID: uuid.New(),
		AgentVersionRef: "ghost@1", Provider: "reference-go", RuntimeABI: "fenced.reference/v1",
		SchemaVersion: "state/v1",
		State: kernelstore.ArtifactReference{
			URI: "artifact://tenant-a/sha256/ghost", SHA256: digest, SizeBytes: int64(len("ghost-state")), MediaType: "application/octet-stream",
		},
	})
	if !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("expected unpublished-version rejection, got %v", err)
	}
	var checkpoints int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM checkpoints WHERE tenant_id = $1`, "tenant-a").Scan(&checkpoints); err != nil {
		t.Fatalf("count checkpoints: %v", err)
	}
	if checkpoints != 0 {
		t.Fatalf("checkpoint was durably stored for an unpublished version: %d", checkpoints)
	}
}
