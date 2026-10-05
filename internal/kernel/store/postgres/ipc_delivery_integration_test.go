//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ipcOutboxCount counts the announcements one tenant's mailbox produced.
func ipcOutboxCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events
		WHERE tenant_id = $1 AND aggregate_type = $2`,
		tenantID, kernelstore.IPCMessageEventAggregateType).Scan(&count); err != nil {
		t.Fatalf("count ipc outbox events: %v", err)
	}
	return count
}

func ipcStoredMessageCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tenantID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ipc_messages WHERE tenant_id = $1`, tenantID).Scan(&count); err != nil {
		t.Fatalf("count ipc messages: %v", err)
	}
	return count
}

// TestIPCMessageStoredEventIsWrittenInItsOwnTransaction pins the delivery
// contract's first half: storing a message announces it exactly once, in the
// same transaction, under the subject grammar the outbox renders.
func TestIPCMessageStoredEventIsWrittenInItsOwnTransaction(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	id := uuid.New()
	sentAt := time.Date(2026, 9, 19, 8, 30, 0, 0, time.UTC)
	input := ipcAppendInput(ipcTenantA, id, "announce-1", mailbox, sentAt)
	input.CorrelationID = "corr-announce-1"
	input.Deadline = sentAt.Add(time.Hour)

	result, err := store.AppendIPCMessage(ctx, input)
	if err != nil {
		t.Fatalf("append message: %v", err)
	}
	if result.Replayed {
		t.Fatal("a first append reported a replay")
	}

	var (
		aggregateID, eventType, tenantID string
		aggregateVersion                 int64
		occurredAt                       time.Time
		publishedAt                      *time.Time
		payload                          []byte
	)
	err = pool.QueryRow(ctx, `SELECT aggregate_id::text, aggregate_version, event_type,
			payload, occurred_at, published_at, tenant_id
		FROM outbox_events
		WHERE tenant_id = $1 AND aggregate_type = $2`, ipcTenantA, kernelstore.IPCMessageEventAggregateType).
		Scan(&aggregateID, &aggregateVersion, &eventType, &payload, &occurredAt, &publishedAt, &tenantID)
	if err != nil {
		t.Fatalf("read the stored event: %v", err)
	}
	if aggregateID != id.String() {
		t.Fatalf("event aggregate id is %s, want the message id %s", aggregateID, id)
	}
	if aggregateVersion != result.Message.Sequence {
		t.Fatalf("event aggregate version is %d, want the mailbox sequence %d", aggregateVersion, result.Message.Sequence)
	}
	if eventType != kernelstore.IPCMessageStoredEventType {
		t.Fatalf("event type is %q, want %q", eventType, kernelstore.IPCMessageStoredEventType)
	}
	if !occurredAt.UTC().Equal(sentAt) {
		t.Fatalf("event occurred at %s, want the send time %s", occurredAt.UTC(), sentAt)
	}
	if publishedAt != nil {
		t.Fatalf("a freshly stored event is already published at %s", publishedAt)
	}

	var announcement map[string]any
	if err := json.Unmarshal(payload, &announcement); err != nil {
		t.Fatalf("decode event payload: %v", err)
	}
	if announcement["messageId"] != id.String() {
		t.Fatalf("announcement message id is %v, want %s", announcement["messageId"], id)
	}
	if announcement["toAddress"] != mailbox.Canonical() {
		t.Fatalf("announcement address is %v, want %s", announcement["toAddress"], mailbox.Canonical())
	}
	if announcement["kind"] != "handoff" || announcement["correlationId"] != "corr-announce-1" {
		t.Fatalf("announcement lost the message identity: %v", announcement)
	}
	if _, ok := announcement["deadline"]; !ok {
		t.Fatal("announcement dropped the deadline the message carries")
	}
	// The event announces a message; it does not carry one. The body stays in
	// the mailbox: embedding up to 256 KiB in the envelope would approach the
	// 1 MiB JetStream cap, and the event stream is not the place for contents.
	for _, forbidden := range []string{"payload", "attachments", "summarize"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("announcement carries %q, so it is not a notification only: %s", forbidden, payload)
		}
	}
}

// TestIPCMessageReplayDoesNotAnnounceTwice keeps a retry from becoming two
// events: the message a replay resolves to was announced by the attempt that
// stored it.
func TestIPCMessageReplayDoesNotAnnounceTwice(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	input := ipcAppendInput(ipcTenantA, uuid.New(), "announce-replay", mailbox,
		time.Date(2026, 9, 19, 9, 0, 0, 0, time.UTC))
	if _, err := store.AppendIPCMessage(ctx, input); err != nil {
		t.Fatalf("first append: %v", err)
	}
	replayed, err := store.AppendIPCMessage(ctx, input)
	if err != nil {
		t.Fatalf("replayed append: %v", err)
	}
	if !replayed.Replayed {
		t.Fatal("a retry of the same send did not report a replay")
	}
	if count := ipcOutboxCount(t, ctx, pool, ipcTenantA); count != 1 {
		t.Fatalf("mailbox holds %d announcements, want 1: a replay must not announce again", count)
	}
}

// TestIPCMessageFailedAppendLeavesNoAnnouncement is the other half of the
// same-transaction claim: a send that is refused creates neither a message nor
// an announcement.
func TestIPCMessageFailedAppendLeavesNoAnnouncement(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	id := uuid.New()
	sentAt := time.Date(2026, 9, 19, 9, 30, 0, 0, time.UTC)
	if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, id, "announce-fail", mailbox, sentAt)); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// The same message id under a different idempotency key is refused.
	conflict := ipcAppendInput(ipcTenantA, id, "announce-fail-other", mailbox, sentAt.Add(time.Second))
	if _, err := store.AppendIPCMessage(ctx, conflict); !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("reusing a message id under a new key returned %v", err)
	}
	if count := ipcOutboxCount(t, ctx, pool, ipcTenantA); count != 1 {
		t.Fatalf("a refused send left %d announcements, want only the 1 from the stored message", count)
	}
	if count := ipcStoredMessageCount(t, ctx, pool, ipcTenantA); count != 1 {
		t.Fatalf("mailbox holds %d messages, want 1", count)
	}
}

// TestGetRunMailboxScopeResolvesTheRunsTaskVersion covers the check a send makes
// before it stores anything: a run id alone does not name a mailbox, because the
// version reference comes from the run's task.
func TestGetRunMailboxScopeResolvesTheRunsTaskVersion(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	task, run := createAdmittedRun(t, ctx, store, "ipc-scope")
	scope, err := store.GetRunMailboxScope(ctx, task.TenantID, run.ID)
	if err != nil {
		t.Fatalf("resolve run scope: %v", err)
	}
	if scope.RunID != run.ID.String() || scope.TaskID != task.ID.String() {
		t.Fatalf("scope identifies run %s task %s, want run %s task %s", scope.RunID, scope.TaskID, run.ID, task.ID)
	}
	if scope.AgentVersionRef != task.AgentVersionRef {
		t.Fatalf("scope version is %q, want the task's %q", scope.AgentVersionRef, task.AgentVersionRef)
	}

	if _, err := store.GetRunMailboxScope(ctx, task.TenantID, uuid.New()); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("resolving an unknown run returned %v", err)
	}
	// Another tenant sees a run that does not exist, not one it may not read:
	// tenant scoping is not a filter the caller can observe.
	if _, err := store.GetRunMailboxScope(ctx, "tenant-ipc-b", run.ID); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("resolving a run from another tenant returned %v", err)
	}
}
