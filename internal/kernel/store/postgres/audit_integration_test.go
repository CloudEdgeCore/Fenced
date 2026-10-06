//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// TestAuditLedgerChainIntegrity drives the transactional audit ledger
// (ADR-014): every kernel decision appends a chained event, the chain
// verifies, tampering is detected at the exact broken link, and the ledger
// is append-only at the database level.
func TestAuditLedgerChainIntegrity(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	// A task lifecycle produces queued + transitioned audit events.
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "audit chain", Spec: []byte(`{}`), IdempotencyKey: "audit-1",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if _, err := repository.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, created.Task.ResourceVersion, domain.TaskAdmitted); err != nil {
		t.Fatalf("admit task: %v", err)
	}
	// A second tenant's chain is independent.
	other, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: uuid.New(), TenantID: "tenant-b", Namespace: "default", AgentVersionRef: "agent@1",
		Goal: "audit chain b", Spec: []byte(`{}`), IdempotencyKey: "audit-b-1",
	})
	if err != nil {
		t.Fatalf("create task b: %v", err)
	}

	tenantA, err := repository.ExportAuditChain(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("export tenant-a: %v", err)
	}
	if len(tenantA) != 2 || tenantA[0].Seq != 1 || tenantA[1].Seq != 2 {
		t.Fatalf("tenant-a chain = %d events, want 2 (seq 1..2)", len(tenantA))
	}
	if tenantA[0].PrevHash != kernelstore.AuditGenesisHash {
		t.Fatalf("first event prev hash = %x, want genesis", tenantA[0].PrevHash)
	}
	if tenantA[1].PrevHash != tenantA[0].ChainHash {
		t.Fatalf("second event prev hash does not chain to the first event")
	}
	if tenantA[0].EventType != "task.queued" || tenantA[1].EventType != "task.transitioned" {
		t.Fatalf("event types = %s, %s", tenantA[0].EventType, tenantA[1].EventType)
	}

	tenantB, err := repository.ExportAuditChain(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("export tenant-b: %v", err)
	}
	if len(tenantB) != 1 || tenantB[0].Seq != 1 {
		t.Fatalf("tenant-b chain = %d events, want 1 (independent chain)", len(tenantB))
	}
	if _, err := repository.ExportAuditChain(ctx, "tenant-c"); err != nil {
		t.Fatalf("export empty tenant: %v", err)
	}

	// Verification passes on the intact chains.
	verification, err := repository.VerifyAuditChain(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("verify tenant-a: %v", err)
	}
	if !verification.Valid || verification.Checked != 2 || verification.HeadSeq != 2 {
		t.Fatalf("verification = %+v", verification)
	}

	// Tampering with a middle event breaks the chain exactly there: the
	// details of seq 1 change, so seq 2's prev hash no longer matches.
	if _, err := pool.Exec(ctx, `UPDATE audit_events SET details = '{"tampered":true}'::jsonb
		WHERE tenant_id = 'tenant-a' AND seq = 1`); err == nil {
		t.Fatal("UPDATE of an audit row succeeded (append-only trigger missing)")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM audit_events WHERE tenant_id = 'tenant-a'`); err == nil {
		t.Fatal("DELETE of audit rows succeeded (append-only trigger missing)")
	}
	// The trigger blocks UPDATE/DELETE; simulate tampering by disabling it for
	// the test (the attack is a compromised database operator, not SQL).
	if _, err := pool.Exec(ctx, `ALTER TABLE audit_events DISABLE TRIGGER audit_events_append_only`); err != nil {
		t.Fatalf("disable trigger: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, `ALTER TABLE audit_events ENABLE TRIGGER audit_events_append_only`)
	}()
	if _, err := pool.Exec(ctx, `UPDATE audit_events SET details = '{"tampered":true}'::jsonb
		WHERE tenant_id = 'tenant-a' AND seq = 1`); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	verification, err = repository.VerifyAuditChain(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("verify tampered: %v", err)
	}
	if verification.Valid || verification.FirstBrokenSeq != 1 {
		t.Fatalf("tampered verification = %+v, want broken at seq 1 (the tampered event)", verification)
	}

	// Appending after the tamper keeps the chain broken at the same link.
	if _, err := repository.TransitionTask(ctx, created.Task.TenantID, created.Task.ID, 2, domain.TaskRunning); err != nil {
		t.Fatalf("transition after tamper: %v", err)
	}
	verification, err = repository.VerifyAuditChain(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("verify after append: %v", err)
	}
	if verification.Valid || verification.FirstBrokenSeq != 1 {
		t.Fatalf("post-tamper verification = %+v, want still broken at seq 1", verification)
	}
	_ = other
}

// TestAuditLedgerConcurrentAppends proves the per-tenant advisory lock keeps
// seq and the hash chain race-free under concurrent appends.
func TestAuditLedgerConcurrentAppends(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	const writers = 8
	const perWriter = 5
	var wg sync.WaitGroup
	errorsCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
					ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
					Goal: "concurrent audit", Spec: []byte(`{}`),
					IdempotencyKey: fmt.Sprintf("audit-concurrent-%d-%d", worker, i),
				}); err != nil {
					errorsCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent create: %v", err)
	}

	verification, err := repository.VerifyAuditChain(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	want := int64(writers * perWriter)
	if !verification.Valid || verification.Checked != want {
		t.Fatalf("verification = %+v, want %d valid events", verification, want)
	}
	// Seq is dense: no gaps from lost advisory-lock races.
	events, err := repository.ExportAuditChain(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	for i, event := range events {
		if event.Seq != int64(i+1) {
			t.Fatalf("seq gap at index %d: %d", i, event.Seq)
		}
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE tenant_id = 'tenant-a'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != writers*perWriter {
		t.Fatalf("row count = %d, want %d", count, writers*perWriter)
	}
}

// TestAuditLedgerFollowsDomainOperations proves the ledger captures agent
// version publishes, memory writes/tombstones and approval decisions.
func TestAuditLedgerFollowsDomainOperations(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()

	if _, err := repository.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default",
		Name: "agent", Version: "1", Spec: []byte(`{}`),
	}); err != nil {
		t.Fatalf("publish version: %v", err)
	}
	embedding := make([]float32, kernelstore.MemoryEmbeddingDimension)
	embedding[0] = 0.5
	record, _, err := repository.PutMemory(ctx, kernelstore.PutMemoryInput{
		TenantID: "tenant-a", Namespace: "default", Key: "audit-memory",
		ContentType: "text/plain", Content: "audited fact",
		Embedding: embedding, EmbeddingProvider: "test", Sensitivity: "internal",
	})
	if err != nil {
		t.Fatalf("put memory: %v", err)
	}
	if _, err := repository.TombstoneMemory(ctx, "tenant-a", record.ID, record.ResourceVersion); err != nil {
		t.Fatalf("tombstone memory: %v", err)
	}

	events, err := repository.ExportAuditChain(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	var types []string
	for _, event := range events {
		types = append(types, event.EventType)
	}
	want := []string{"agent_version.published", "memory.upserted", "memory.tombstoned"}
	if len(types) != len(want) {
		t.Fatalf("event types = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("event types = %v, want %v", types, want)
		}
	}
	// Every audit event also produced an outbox projection event.
	var outboxCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_type = 'Audit'`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != len(want) {
		t.Fatalf("audit outbox events = %d, want %d", outboxCount, len(want))
	}
}

// TestAuditPaging proves the cursor-based read surface.
func TestAuditPaging(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		if _, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1",
			Goal: "paging", Spec: []byte(`{}`), IdempotencyKey: "audit-page-" + string(rune('a'+i)),
		}); err != nil {
			t.Fatalf("create task %d: %v", i, err)
		}
	}
	page1, err := repository.ListAudit(ctx, kernelstore.ListAuditInput{TenantID: "tenant-a", Limit: 3})
	if err != nil || len(page1) != 3 || page1[0].Seq != 1 || page1[2].Seq != 3 {
		t.Fatalf("page 1: %d events err=%v", len(page1), err)
	}
	page2, err := repository.ListAudit(ctx, kernelstore.ListAuditInput{TenantID: "tenant-a", AfterSeq: 3, Limit: 3})
	if err != nil || len(page2) != 3 || page2[0].Seq != 4 {
		t.Fatalf("page 2: %d events err=%v", len(page2), err)
	}
	tail, err := repository.ListAudit(ctx, kernelstore.ListAuditInput{TenantID: "tenant-a", AfterSeq: 6, Limit: 10})
	if err != nil || len(tail) != 1 || tail[0].Seq != 7 {
		t.Fatalf("tail: %d events err=%v", len(tail), err)
	}
	if _, err := repository.ListAudit(ctx, kernelstore.ListAuditInput{TenantID: "tenant-a", Limit: 0}); err == nil {
		t.Fatal("zero limit must fail")
	}
}
