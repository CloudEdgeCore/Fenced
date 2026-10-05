package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	_ kernelstore.IPCStore          = (*Store)(nil)
	_ kernelstore.MailboxScopeStore = (*Store)(nil)
)

// ipcMessageColumns is the projection shared by every read of a stored message.
// idempotency_key is included even though the read model does not expose it:
// AppendIPCMessage has to tell a replay of the caller's own send apart from a
// collision with another message, and it does that on the row it reads back
// after the insert was skipped.
//
// payload and attachments are jsonb, so PostgreSQL returns them in its own
// normalized form (object keys reordered, whitespace removed). A reader must
// compare them as JSON documents, never as bytes.
const ipcMessageColumns = `sequence, id::text, tenant_id, from_agent_version_ref, to_address,
	kind, payload, attachments, correlation_id, reply_to_message_id::text, trace_id,
	sent_at, deadline, idempotency_key`

// AppendIPCMessage stores one message in the recipient's mailbox.
//
// A message stored for the first time also enqueues the IPCMessageStoredEventType
// dispatch event into outbox_events, in the same transaction as the row it
// announces. The two cannot diverge: a rolled-back send leaves no announcement,
// and a committed send cannot be missing one. An idempotent replay enqueues
// nothing, because the message it resolves to was announced by the attempt that
// created it.
//
// The event payload is a notification, not the message. The body stays in the
// mailbox: a payload at the IPCPayloadLimit would sit close to the 1 MiB
// JetStream MaxMsgSize once embedded in the event envelope, and the event stream
// is not the place for message contents.
//
// No audit row is written: audit attribution for IPC (which principal sent what
// to whom) is a later concern, so until then a stored message is not represented
// in the audit chain. That is a stated gap, not an oversight.
//
// There is no retry loop around the transaction. The insert takes no row lock
// and leans on a unique constraint with ON CONFLICT DO NOTHING, so contention
// resolves as "no row returned" rather than as a lost update; a retryable
// conflict can only arrive as a genuine deadlock, and a deadlock surfaces to
// the caller as ErrRetryableTransaction, which a send may safely retry because
// its idempotency key makes the retry idempotent.
func (s *Store) AppendIPCMessage(ctx context.Context, in kernelstore.AppendIPCMessageInput) (kernelstore.AppendIPCMessageResult, error) {
	var result kernelstore.AppendIPCMessageResult
	if err := in.Validate(); err != nil {
		return result, err
	}
	attachments := in.Attachments
	if len(bytes.TrimSpace(attachments)) == 0 {
		attachments = json.RawMessage(`[]`)
	}
	var deadline *time.Time
	if !in.Deadline.IsZero() {
		value := in.Deadline.UTC()
		deadline = &value
	}
	var replyTo *string
	if in.ReplyToMessageID != uuid.Nil {
		value := in.ReplyToMessageID.String()
		replyTo = &value
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	// ON CONFLICT DO NOTHING carries no conflict target on purpose: a message
	// collides either on its own identity (tenant_id, id) or on the sender's
	// idempotency key, and both mean this insert must not create a row. The
	// re-read below decides which of the two happened.
	stored, _, scanErr := scanIPCAppendRow(tx.QueryRow(ctx, `INSERT INTO ipc_messages (
		id, tenant_id, from_agent_version_ref, to_address, kind, payload, attachments,
		correlation_id, reply_to_message_id, idempotency_key, trace_id, sent_at, deadline
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	ON CONFLICT DO NOTHING
	RETURNING `+ipcMessageColumns,
		in.ID.String(), in.TenantID, in.FromAgentVersionRef, in.To.Canonical(), in.Kind,
		[]byte(in.Payload), []byte(attachments), in.CorrelationID, replyTo,
		in.IdempotencyKey, in.TraceID, in.SentAt.UTC(), deadline))
	if scanErr == nil {
		// occurred_at is the caller-supplied send time rather than s.now(): the
		// announcement describes the send, and deriving the timestamp from the
		// input keeps the event a function of the message it announces.
		if err := insertEvent(ctx, tx, in.TenantID, kernelstore.IPCMessageEventAggregateType,
			stored.ID, stored.Sequence, kernelstore.IPCMessageStoredEventType,
			ipcMessageStoredPayload(stored), in.SentAt.UTC(), s.newID()); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return kernelstore.AppendIPCMessageResult{Message: stored}, nil
	}
	if !errors.Is(scanErr, pgx.ErrNoRows) {
		return result, classify(scanErr)
	}

	existing, idempotencyKey, err := readIPCAppendConflict(ctx, tx, in)
	if err != nil {
		return result, err
	}
	if existing.ID != in.ID || existing.FromAgentVersionRef != in.FromAgentVersionRef || idempotencyKey != in.IdempotencyKey {
		return result, fmt.Errorf("%w: message id=%s key=%s sender=%s collides with message id=%s key=%s sender=%s",
			kernelstore.ErrIdempotencyConflict, in.ID, in.IdempotencyKey, in.FromAgentVersionRef,
			existing.ID, idempotencyKey, existing.FromAgentVersionRef)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return kernelstore.AppendIPCMessageResult{Message: existing, Replayed: true}, nil
}

// ipcMessageStoredPayload renders the announcement of a stored message. It
// carries the addressing and correlation facts a consumer needs to decide
// whether the message concerns it, and never the body: a reader that wants the
// body reads the mailbox. Optional fields are omitted rather than written as
// empty strings, so a consumer can tell "absent" from "present but empty".
func ipcMessageStoredPayload(message kernelstore.IPCMessage) map[string]any {
	payload := map[string]any{
		"tenantId":            message.TenantID,
		"messageId":           message.ID.String(),
		"sequence":            message.Sequence,
		"fromAgentVersionRef": message.FromAgentVersionRef,
		"toAddress":           message.To.Canonical(),
		"kind":                message.Kind,
		"sentAt":              message.SentAt.UTC().Format(time.RFC3339Nano),
	}
	if message.CorrelationID != "" {
		payload["correlationId"] = message.CorrelationID
	}
	if message.ReplyToMessageID != uuid.Nil {
		payload["replyToMessageId"] = message.ReplyToMessageID.String()
	}
	if message.TraceID != "" {
		payload["traceId"] = message.TraceID
	}
	if !message.Deadline.IsZero() {
		payload["deadline"] = message.Deadline.UTC().Format(time.RFC3339Nano)
	}
	return payload
}

// readIPCAppendConflict reads back the row an insert could have collided with.
// At most two rows can match: one by message identity and one by the sender's
// idempotency key. Both are locked, because a concurrent send carrying the same
// key is exactly what is being resolved.
func readIPCAppendConflict(ctx context.Context, tx pgx.Tx, in kernelstore.AppendIPCMessageInput) (kernelstore.IPCMessage, string, error) {
	rows, err := tx.Query(ctx, `SELECT `+ipcMessageColumns+`
		FROM ipc_messages
		WHERE tenant_id = $1
		  AND (id = $2 OR (from_agent_version_ref = $3 AND idempotency_key = $4))
		ORDER BY sequence
		FOR UPDATE`, in.TenantID, in.ID.String(), in.FromAgentVersionRef, in.IdempotencyKey)
	if err != nil {
		return kernelstore.IPCMessage{}, "", classify(err)
	}
	defer rows.Close()
	var found []kernelstore.IPCMessage
	var keys []string
	for rows.Next() {
		message, key, scanErr := scanIPCAppendRow(rows)
		if scanErr != nil {
			return kernelstore.IPCMessage{}, "", classify(scanErr)
		}
		found = append(found, message)
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return kernelstore.IPCMessage{}, "", classify(err)
	}
	if len(found) == 0 {
		// The insert was skipped but nothing matches the identity or the key.
		// The caller cannot be told the send succeeded, and it cannot be told
		// what it collided with either.
		return kernelstore.IPCMessage{}, "", fmt.Errorf("%w: ipc message %s collided with a row that is no longer readable", kernelstore.ErrNotFound, in.ID)
	}
	return found[0], keys[0], nil
}

// GetIPCMessage returns one message. A message stored under another tenant is
// reported as ErrNotFound rather than as a permission failure, because tenant
// scoping is not a filter the caller may observe.
func (s *Store) GetIPCMessage(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.IPCMessage, error) {
	if strings.TrimSpace(tenantID) == "" || id == uuid.Nil {
		return kernelstore.IPCMessage{}, fmt.Errorf("tenant and message id are required")
	}
	message, _, err := scanIPCAppendRow(s.pool.QueryRow(ctx, `SELECT `+ipcMessageColumns+`
		FROM ipc_messages WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
	return message, classify(err)
}

// GetRunMailboxScope resolves the mailbox scope a run id names.
//
// The agent version reference is read from the run's task rather than from the
// run: a run carries no version of its own, and the task held the immutable
// publication resolved during admission. A run id that exists in another tenant
// is reported as ErrNotFound, exactly like one that does not exist, because
// tenant scoping is not a filter the caller may observe.
func (s *Store) GetRunMailboxScope(ctx context.Context, tenantID string, runID uuid.UUID) (kernelstore.RunMailboxScope, error) {
	if strings.TrimSpace(tenantID) == "" || runID == uuid.Nil {
		return kernelstore.RunMailboxScope{}, fmt.Errorf("tenant and run id are required")
	}
	var scope kernelstore.RunMailboxScope
	err := s.pool.QueryRow(ctx, `SELECT r.id::text, r.task_id::text, t.agent_version_ref
		FROM runs r
		JOIN tasks t ON t.tenant_id = r.tenant_id AND t.id = r.task_id
		WHERE r.tenant_id = $1 AND r.id = $2`, tenantID, runID.String()).
		Scan(&scope.RunID, &scope.TaskID, &scope.AgentVersionRef)
	if err != nil {
		return kernelstore.RunMailboxScope{}, classify(err)
	}
	scope.TenantID = tenantID
	return scope, nil
}

// ListMailbox drains one mailbox forward by sequence. Messages the consumer has
// already receipted are filtered out in the query rather than after it, so a
// drained batch is never shorter than the limit for a reason the caller cannot
// see.
func (s *Store) ListMailbox(ctx context.Context, in kernelstore.ListMailboxInput) ([]kernelstore.IPCMessage, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+ipcMessageColumns+`
		FROM ipc_messages
		WHERE tenant_id = $1
		  AND to_address = $2
		  AND sequence > $3
		  AND NOT EXISTS (
		      SELECT 1 FROM inbox_receipts r
		      WHERE r.tenant_id = ipc_messages.tenant_id
		        AND r.consumer_name = $4
		        AND r.event_id = ipc_messages.id
		  )
		ORDER BY sequence
		LIMIT $5`, in.TenantID, in.To.Canonical(), in.AfterSequence, in.ConsumerName, in.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var messages []kernelstore.IPCMessage
	for rows.Next() {
		message, _, scanErr := scanIPCAppendRow(rows)
		if scanErr != nil {
			return nil, classify(scanErr)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	return messages, nil
}

// AcknowledgeIPCMessages records a consumer's receipts for a batch of messages.
// The whole batch is validated before any receipt is written, and a message that
// does not exist in the tenant rejects the batch: a receipt is keyed by message
// id, so acknowledging an id that does not exist yet would permanently hide the
// real message from this consumer if it were stored later.
//
// A message may be acknowledged by any consumer of the tenant; the receipt is
// scoped by consumer name, so acknowledging a message the caller never read
// affects only that caller's own drain. Which consumer may receipt which mailbox
// is an authorization concern this layer does not own.
func (s *Store) AcknowledgeIPCMessages(ctx context.Context, in kernelstore.AcknowledgeIPCMessagesInput) (kernelstore.AcknowledgeIPCMessagesResult, error) {
	var result kernelstore.AcknowledgeIPCMessagesResult
	if err := in.Validate(); err != nil {
		return result, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	known, err := knownIPCMessageIDs(ctx, tx, in.TenantID, in.MessageIDs)
	if err != nil {
		return result, err
	}
	for _, id := range in.MessageIDs {
		if _, ok := known[id]; !ok {
			return result, fmt.Errorf("%w: ipc message %s does not exist in this tenant", kernelstore.ErrNotFound, id)
		}
	}

	// RETURNING yields exactly the ids this call inserted, so the two buckets
	// do not have to be inferred from an affected-row total.
	rows, err := tx.Query(ctx, `WITH receipted AS (SELECT unnest($3::uuid[]) AS id)
		INSERT INTO inbox_receipts (tenant_id, consumer_name, event_id, processed_at)
		SELECT $1, $2, receipted.id, $4 FROM receipted
		ON CONFLICT (tenant_id, consumer_name, event_id) DO NOTHING
		RETURNING event_id::text`, in.TenantID, in.ConsumerName, in.MessageIDs, in.AcknowledgedAt.UTC())
	if err != nil {
		return result, classify(err)
	}
	defer rows.Close()
	inserted := make(map[uuid.UUID]struct{}, len(in.MessageIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return result, classify(err)
		}
		parsed, parseErr := uuid.Parse(id)
		if parseErr != nil {
			return result, fmt.Errorf("parse acknowledged ipc message id: %w", parseErr)
		}
		inserted[parsed] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return result, classify(err)
	}
	// Both buckets are reported in the caller's order, so a retry is
	// comparable field by field against the first attempt.
	for _, id := range in.MessageIDs {
		if _, ok := inserted[id]; ok {
			result.Acknowledged = append(result.Acknowledged, id)
			continue
		}
		result.AlreadyAcknowledged = append(result.AlreadyAcknowledged, id)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return result, nil
}

// knownIPCMessageIDs returns the subset of ids that exist in the tenant.
func knownIPCMessageIDs(ctx context.Context, tx pgx.Tx, tenantID string, ids []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	rows, err := tx.Query(ctx, `SELECT id::text FROM ipc_messages
		WHERE tenant_id = $1 AND id = ANY($2)`, tenantID, ids)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	known := make(map[uuid.UUID]struct{}, len(ids))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, classify(err)
		}
		parsed, parseErr := uuid.Parse(id)
		if parseErr != nil {
			return nil, fmt.Errorf("parse ipc message id: %w", parseErr)
		}
		known[parsed] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	return known, nil
}

// ipcMessageRow holds the scan destinations of ipcMessageColumns, so the read
// path and the append conflict probe share one projection and one decoder.
type ipcMessageRow struct {
	message          kernelstore.IPCMessage
	id               string
	toAddress        string
	payload          []byte
	attachments      []byte
	replyToMessageID *string
	deadline         *time.Time
	idempotencyKey   string
}

func (r *ipcMessageRow) targets() []any {
	return []any{
		&r.message.Sequence, &r.id, &r.message.TenantID, &r.message.FromAgentVersionRef,
		&r.toAddress, &r.message.Kind, &r.payload, &r.attachments,
		&r.message.CorrelationID, &r.replyToMessageID, &r.message.TraceID,
		&r.message.SentAt, &r.deadline, &r.idempotencyKey,
	}
}

func (r *ipcMessageRow) decode() (kernelstore.IPCMessage, string, error) {
	message := r.message
	id, err := uuid.Parse(r.id)
	if err != nil {
		return kernelstore.IPCMessage{}, "", fmt.Errorf("parse ipc message id: %w", err)
	}
	message.ID = id
	address, err := kernelstore.ParseAgentAddress(r.toAddress)
	if err != nil {
		return kernelstore.IPCMessage{}, "", fmt.Errorf("parse ipc message mailbox: %w", err)
	}
	message.To = address
	if r.replyToMessageID != nil {
		replyTo, parseErr := uuid.Parse(*r.replyToMessageID)
		if parseErr != nil {
			return kernelstore.IPCMessage{}, "", fmt.Errorf("parse ipc reply target: %w", parseErr)
		}
		message.ReplyToMessageID = replyTo
	}
	if r.deadline != nil {
		message.Deadline = r.deadline.UTC()
	}
	message.SentAt = message.SentAt.UTC()
	// Both columns are NOT NULL, so a copy is always present; the defaults only
	// guard against a driver returning an empty slice for a JSON null.
	message.Payload = json.RawMessage(`{}`)
	if len(r.payload) > 0 {
		message.Payload = append(json.RawMessage(nil), r.payload...)
	}
	message.Attachments = json.RawMessage(`[]`)
	if len(r.attachments) > 0 {
		message.Attachments = append(json.RawMessage(nil), r.attachments...)
	}
	return message, r.idempotencyKey, nil
}

// scanIPCAppendRow decodes one row of ipcMessageColumns and returns the row's
// stored idempotency key alongside the read model. It does not classify the
// scan error, because its callers distinguish pgx.ErrNoRows from a real
// failure themselves.
func scanIPCAppendRow(row scanner) (kernelstore.IPCMessage, string, error) {
	target := &ipcMessageRow{}
	if err := row.Scan(target.targets()...); err != nil {
		return kernelstore.IPCMessage{}, "", err
	}
	return target.decode()
}
