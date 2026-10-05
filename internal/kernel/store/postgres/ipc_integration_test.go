//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

const (
	ipcTenantA = "tenant-ipc-a"
	ipcTenantB = "tenant-ipc-b"
	ipcRunA    = "3f5c0b7e-6d31-4a19-9c50-3f0c6b1d9a44"
	ipcRunB    = "0b0c8f1a-9d2e-4f3b-8a11-7c6e5d4b3a21"
)

func ipcMailbox(agentVersionRef, runID string) kernelstore.AgentAddress {
	return kernelstore.AgentAddress{AgentVersionRef: agentVersionRef, Instance: runID}
}

// ipcAppendInput builds a message with a deterministic identity so a test can
// replay it unchanged.
func ipcAppendInput(tenant string, id uuid.UUID, key string, to kernelstore.AgentAddress, sentAt time.Time) kernelstore.AppendIPCMessageInput {
	return kernelstore.AppendIPCMessageInput{
		TenantID:            tenant,
		ID:                  id,
		FromAgentVersionRef: "planner@1.2.0",
		To:                  to,
		Kind:                "handoff",
		Payload:             json.RawMessage(`{"goal":"summarize","b":{"x":1},"a":2}`),
		IdempotencyKey:      key,
		TraceID:             "trace-" + key,
		CorrelationID:       "corr-" + key,
		SentAt:              sentAt,
	}
}

func TestIPCMailboxStoresAndDrainsMessages(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	consumer := kernelstore.IPCMailboxConsumerName(ipcRunA)
	sent := []time.Time{
		time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 18, 12, 0, 1, 0, time.UTC),
		time.Date(2026, 9, 18, 12, 0, 2, 0, time.UTC),
	}
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for index, id := range ids {
		result, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, id, "send-"+id.String(), mailbox, sent[index]))
		if err != nil {
			t.Fatalf("append message %d: %v", index, err)
		}
		if result.Replayed {
			t.Fatalf("append message %d reported a replay of a first attempt", index)
		}
		if result.Message.ID != id {
			t.Fatalf("append message %d stored %s, want %s", index, result.Message.ID, id)
		}
	}

	drained, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantA, To: mailbox, Limit: 100, ConsumerName: consumer,
	})
	if err != nil {
		t.Fatalf("drain mailbox: %v", err)
	}
	if len(drained) != len(ids) {
		t.Fatalf("drained %d messages, want %d", len(drained), len(ids))
	}
	for index, message := range drained {
		if message.ID != ids[index] {
			t.Fatalf("drained message %d is %s, want %s (mailbox order is not insertion order)", index, message.ID, ids[index])
		}
		if index > 0 && message.Sequence <= drained[index-1].Sequence {
			t.Fatalf("mailbox sequence did not increase: %d after %d", message.Sequence, drained[index-1].Sequence)
		}
		if message.TenantID != ipcTenantA {
			t.Fatalf("drained message %d carries tenant %q", index, message.TenantID)
		}
		if message.To != mailbox {
			t.Fatalf("drained message %d is addressed to %+v, want %+v", index, message.To, mailbox)
		}
		if message.FromAgentVersionRef != "planner@1.2.0" || message.Kind != "handoff" {
			t.Fatalf("drained message %d lost its sender or kind: %+v", index, message)
		}
		if !message.SentAt.Equal(sent[index]) {
			t.Fatalf("drained message %d has sent at %s, want %s", index, message.SentAt, sent[index])
		}
		if !message.Deadline.IsZero() {
			t.Fatalf("drained message %d reports a deadline it never carried: %s", index, message.Deadline)
		}
		if message.ReplyToMessageID != uuid.Nil {
			t.Fatalf("drained message %d reports a reply target it never carried: %s", index, message.ReplyToMessageID)
		}
		if message.TraceID != "trace-send-"+ids[index].String() || message.CorrelationID != "corr-send-"+ids[index].String() {
			t.Fatalf("drained message %d lost its correlation: %+v", index, message)
		}
		// payload and attachments are jsonb: PostgreSQL returns them in its own
		// normalized form, so they are compared as documents, not as bytes.
		assertSameJSON(t, "payload", message.Payload, json.RawMessage(`{"goal":"summarize","b":{"x":1},"a":2}`))
		assertSameJSON(t, "attachments", message.Attachments, json.RawMessage(`[]`))
	}

	// The cursor is exclusive, so resuming after the second message returns the
	// third and nothing else.
	resumed, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantA, To: mailbox, AfterSequence: drained[1].Sequence, Limit: 100, ConsumerName: consumer,
	})
	if err != nil {
		t.Fatalf("drain mailbox after a cursor: %v", err)
	}
	if len(resumed) != 1 || resumed[0].ID != ids[2] {
		t.Fatalf("cursor drain returned %d messages, want exactly the third", len(resumed))
	}

	// A different instance of the same agent version has its own mailbox.
	other, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantA, To: ipcMailbox("worker@1.0.0", ipcRunB), Limit: 100,
		ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunB),
	})
	if err != nil {
		t.Fatalf("drain a sibling mailbox: %v", err)
	}
	if len(other) != 0 {
		t.Fatalf("a sibling instance drained %d messages addressed to another run", len(other))
	}

	fetched, err := store.GetIPCMessage(ctx, ipcTenantA, ids[1])
	if err != nil {
		t.Fatalf("get message: %v", err)
	}
	if fetched.Sequence != drained[1].Sequence || fetched.ID != ids[1] {
		t.Fatalf("get returned %+v, want the drained message %s", fetched, ids[1])
	}
}

func TestIPCMessageDeadlineRoundTrips(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	id := uuid.New()
	input := ipcAppendInput(ipcTenantA, id, "send-deadline", mailbox, time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC))
	input.Deadline = input.SentAt.Add(5 * time.Minute)
	input.ReplyToMessageID = uuid.New()
	if _, err := store.AppendIPCMessage(ctx, input); err != nil {
		t.Fatalf("append message with a deadline: %v", err)
	}

	drained, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantA, To: mailbox, Limit: 10, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
	})
	if err != nil {
		t.Fatalf("drain mailbox: %v", err)
	}
	if len(drained) != 1 {
		t.Fatalf("drained %d messages, want 1", len(drained))
	}
	if !drained[0].Deadline.Equal(input.Deadline) {
		t.Fatalf("deadline round tripped as %s, want %s", drained[0].Deadline, input.Deadline)
	}
	if drained[0].ReplyToMessageID != input.ReplyToMessageID {
		t.Fatalf("reply target round tripped as %s, want %s", drained[0].ReplyToMessageID, input.ReplyToMessageID)
	}
}

// TestIPCAppendReplayIsIdempotent covers the retry a caller performs when the
// first send may have committed without the caller learning the outcome.
func TestIPCAppendReplayIsIdempotent(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	input := ipcAppendInput(ipcTenantA, uuid.New(), "send-retry", mailbox, time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC))

	first, err := store.AppendIPCMessage(ctx, input)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	second, err := store.AppendIPCMessage(ctx, input)
	if err != nil {
		t.Fatalf("replayed append: %v", err)
	}
	if !second.Replayed {
		t.Fatal("the replay was not reported as a replay")
	}
	if second.Message.Sequence != first.Message.Sequence || second.Message.ID != first.Message.ID {
		t.Fatalf("the replay produced %+v, want the stored message %+v", second.Message, first.Message)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ipc_messages WHERE tenant_id = $1`, ipcTenantA).Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if count != 1 {
		t.Fatalf("the table holds %d messages after a replayed send, want 1", count)
	}
}

func TestIPCAppendRejectsKeyReuseWithADifferentMessage(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	sentAt := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, uuid.New(), "send-shared", mailbox, sentAt)); err != nil {
		t.Fatalf("first append: %v", err)
	}
	_, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, uuid.New(), "send-shared", mailbox, sentAt))
	if !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("reusing an idempotency key for a different message reported %v, want %v", err, kernelstore.ErrIdempotencyConflict)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ipc_messages WHERE tenant_id = $1`, ipcTenantA).Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if count != 1 {
		t.Fatalf("the table holds %d messages after a rejected send, want 1", count)
	}
}

func TestIPCAppendRejectsIdentityReuseWithADifferentKey(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	sentAt := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	id := uuid.New()
	if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, id, "send-first", mailbox, sentAt)); err != nil {
		t.Fatalf("first append: %v", err)
	}
	_, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, id, "send-second", mailbox, sentAt))
	if !errors.Is(err, kernelstore.ErrIdempotencyConflict) {
		t.Fatalf("reusing a message id with a different key reported %v, want %v", err, kernelstore.ErrIdempotencyConflict)
	}
}

// TestIPCAcknowledgeSuppressesRedelivery is the receipt half of the delivery
// contract: a message a consumer has receipted is not handed out again, while a
// different consumer of the same mailbox still sees it.
func TestIPCAcknowledgeSuppressesRedelivery(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	consumer := kernelstore.IPCMailboxConsumerName(ipcRunA)
	sentAt := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	for index, id := range ids {
		if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, id, "send-"+id.String(), mailbox, sentAt.Add(time.Duration(index)*time.Second))); err != nil {
			t.Fatalf("append message %d: %v", index, err)
		}
	}

	drain := func(consumerName string) []kernelstore.IPCMessage {
		t.Helper()
		messages, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
			TenantID: ipcTenantA, To: mailbox, Limit: 100, ConsumerName: consumerName,
		})
		if err != nil {
			t.Fatalf("drain mailbox as %s: %v", consumerName, err)
		}
		return messages
	}

	if got := drain(consumer); len(got) != 2 {
		t.Fatalf("first drain returned %d messages, want 2", len(got))
	}

	acknowledgedAt := sentAt.Add(time.Minute)
	ack, err := store.AcknowledgeIPCMessages(ctx, kernelstore.AcknowledgeIPCMessagesInput{
		TenantID: ipcTenantA, ConsumerName: consumer, MessageIDs: ids, AcknowledgedAt: acknowledgedAt,
	})
	if err != nil {
		t.Fatalf("acknowledge messages: %v", err)
	}
	if len(ack.Acknowledged) != 2 || len(ack.AlreadyAcknowledged) != 0 {
		t.Fatalf("first acknowledgement reported %+v, want both messages newly acknowledged", ack)
	}

	if got := drain(consumer); len(got) != 0 {
		t.Fatalf("a drain after acknowledgement returned %d messages, want 0", len(got))
	}

	// Redelivery of the same batch is a no-op, and the retry is visible as such.
	ack, err = store.AcknowledgeIPCMessages(ctx, kernelstore.AcknowledgeIPCMessagesInput{
		TenantID: ipcTenantA, ConsumerName: consumer, MessageIDs: ids, AcknowledgedAt: acknowledgedAt,
	})
	if err != nil {
		t.Fatalf("replayed acknowledgement: %v", err)
	}
	if len(ack.Acknowledged) != 0 || len(ack.AlreadyAcknowledged) != 2 {
		t.Fatalf("replayed acknowledgement reported %+v, want both already acknowledged", ack)
	}
	if !reflect.DeepEqual(ack.AlreadyAcknowledged, ids) {
		t.Fatalf("replayed acknowledgement lost the caller's order: %+v", ack.AlreadyAcknowledged)
	}

	// A receipt is scoped to one consumer, so it does not hide the message from
	// another consumer of the same mailbox.
	if got := drain("observer"); len(got) != 2 {
		t.Fatalf("another consumer saw %d messages, want 2", len(got))
	}
}

// TestIPCMailboxSurvivesTheDrainingAttempt documents the property the address
// form exists for: a message is addressed to the run, so the next attempt of
// that run drains the same mailbox and a message whose receipt never landed is
// still there.
func TestIPCMailboxSurvivesTheDrainingAttempt(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	sentAt := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	first, second := uuid.New(), uuid.New()
	for index, id := range []uuid.UUID{first, second} {
		if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, id, "send-"+id.String(), mailbox, sentAt.Add(time.Duration(index)*time.Second))); err != nil {
			t.Fatalf("append message %d: %v", index, err)
		}
	}

	// Attempt 1 drains both and receipts only the first, then dies.
	drained, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantA, To: mailbox, Limit: 100, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
	})
	if err != nil {
		t.Fatalf("drain as the first attempt: %v", err)
	}
	if len(drained) != 2 {
		t.Fatalf("the first attempt drained %d messages, want 2", len(drained))
	}
	if _, err := store.AcknowledgeIPCMessages(ctx, kernelstore.AcknowledgeIPCMessagesInput{
		TenantID: ipcTenantA, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
		MessageIDs: []uuid.UUID{first}, AcknowledgedAt: sentAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("acknowledge the first message: %v", err)
	}

	// Attempt 2 of the same run drains the same mailbox and sees the message
	// that was never acknowledged, and not the one that was.
	redelivered, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantA, To: mailbox, Limit: 100, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
	})
	if err != nil {
		t.Fatalf("drain as the second attempt: %v", err)
	}
	if len(redelivered) != 1 || redelivered[0].ID != second {
		t.Fatalf("the second attempt drained %+v, want exactly the unacknowledged message %s", redelivered, second)
	}
}

// TestIPCAcknowledgeRejectsAnUnknownMessage pins the fail-closed rule: a receipt
// for a message that does not exist would hide the real message from this
// consumer if it were stored later, so the whole batch is refused.
func TestIPCAcknowledgeRejectsAnUnknownMessage(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	sentAt := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	known := uuid.New()
	if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, known, "send-known", mailbox, sentAt)); err != nil {
		t.Fatalf("append message: %v", err)
	}

	_, err := store.AcknowledgeIPCMessages(ctx, kernelstore.AcknowledgeIPCMessagesInput{
		TenantID: ipcTenantA, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
		MessageIDs: []uuid.UUID{known, uuid.New()}, AcknowledgedAt: sentAt.Add(time.Minute),
	})
	if !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("acknowledging an unknown message reported %v, want %v", err, kernelstore.ErrNotFound)
	}

	// The rejected batch wrote nothing: the known message is still drainable.
	drained, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantA, To: mailbox, Limit: 100, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
	})
	if err != nil {
		t.Fatalf("drain mailbox: %v", err)
	}
	if len(drained) != 1 || drained[0].ID != known {
		t.Fatalf("a rejected acknowledgement left a partial receipt: drained %+v", drained)
	}
}

func TestIPCCrossTenantIsolation(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	sentAt := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	id := uuid.New()
	if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantA, id, "send-a", mailbox, sentAt)); err != nil {
		t.Fatalf("append message: %v", err)
	}

	if _, err := store.GetIPCMessage(ctx, ipcTenantB, id); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("another tenant read the message: %v", err)
	}
	drained, err := store.ListMailbox(ctx, kernelstore.ListMailboxInput{
		TenantID: ipcTenantB, To: mailbox, Limit: 100, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
	})
	if err != nil {
		t.Fatalf("drain another tenant's view of the mailbox: %v", err)
	}
	if len(drained) != 0 {
		t.Fatalf("another tenant drained %d messages from this mailbox", len(drained))
	}
	if _, err := store.AcknowledgeIPCMessages(ctx, kernelstore.AcknowledgeIPCMessagesInput{
		TenantID: ipcTenantB, ConsumerName: kernelstore.IPCMailboxConsumerName(ipcRunA),
		MessageIDs: []uuid.UUID{id}, AcknowledgedAt: sentAt.Add(time.Minute),
	}); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("another tenant acknowledged the message: %v", err)
	}

	// Both tenants may use the same message id and the same idempotency key;
	// neither index is global.
	if _, err := store.AppendIPCMessage(ctx, ipcAppendInput(ipcTenantB, id, "send-a", mailbox, sentAt)); err != nil {
		t.Fatalf("a second tenant could not reuse the identity: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ipc_messages WHERE id = $1`, id.String()).Scan(&count); err != nil {
		t.Fatalf("count messages by id: %v", err)
	}
	if count != 2 {
		t.Fatalf("two tenants holding one message id produced %d rows, want 2", count)
	}
}

// TestIPCPayloadLimitIsEnforcedBeforeTheDatabase keeps the rejection at the
// contract boundary: an oversized payload must never reach a column, because
// the delivery path has to publish the message through a 1 MiB NATS event.
func TestIPCPayloadLimitIsEnforcedBeforeTheDatabase(t *testing.T) {
	clock := newFakeClock()
	pool, store := prepare(t, clock.Now)
	ctx := context.Background()

	mailbox := ipcMailbox("worker@1.0.0", ipcRunA)
	input := ipcAppendInput(ipcTenantA, uuid.New(), "send-huge", mailbox, time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC))
	input.Payload = json.RawMessage(`{"blob":"` + strings.Repeat("x", kernelstore.IPCPayloadLimit) + `"}`)
	if _, err := store.AppendIPCMessage(ctx, input); !errors.Is(err, kernelstore.ErrIPCPayloadTooLarge) {
		t.Fatalf("an oversized payload reported %v, want %v", err, kernelstore.ErrIPCPayloadTooLarge)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ipc_messages`).Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if count != 0 {
		t.Fatalf("a rejected payload wrote %d rows", count)
	}
}

// TestIPCAppendRejectsAnUnscopedMailbox keeps the address rule at the store
// boundary rather than leaving it to callers.
func TestIPCAppendRejectsAnUnscopedMailbox(t *testing.T) {
	clock := newFakeClock()
	_, store := prepare(t, clock.Now)
	ctx := context.Background()

	input := ipcAppendInput(ipcTenantA, uuid.New(), "send-unscoped",
		kernelstore.AgentAddress{AgentVersionRef: "worker@1.0.0"},
		time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC))
	if _, err := store.AppendIPCMessage(ctx, input); !errors.Is(err, kernelstore.ErrIPCAddressUnscoped) {
		t.Fatalf("a version-only address reported %v, want %v", err, kernelstore.ErrIPCAddressUnscoped)
	}
}

func assertSameJSON(t *testing.T, label string, got, want json.RawMessage) {
	t.Helper()
	if !json.Valid(got) {
		t.Fatalf("%s is not valid JSON: %q", label, got)
	}
	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("decode %s: %v", label, err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("decode expected %s: %v", label, err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("%s is %s, want %s", label, got, want)
	}
}
