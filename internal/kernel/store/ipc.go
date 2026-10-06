package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrIPCAddressUnscoped reports an address that names only an agent
	// version. Mailboxes are instance-scoped: a version-only address would be
	// drained by whichever instance polls first, which is a competing-consumer
	// semantic the v1 contract does not define.
	ErrIPCAddressUnscoped = errors.New("ipc address must name an instance")
	// ErrIPCPayloadTooLarge reports a payload above IPCPayloadLimit.
	ErrIPCPayloadTooLarge = errors.New("ipc payload exceeds the mailbox limit")
	// ErrIPCDeadlineElapsed reports a deadline that does not lie after SentAt:
	// such a message would be born already expired, which is a caller bug
	// rather than a message to store.
	ErrIPCDeadlineElapsed = errors.New("ipc message deadline has already elapsed")
)

const (
	// IPCPayloadLimit bounds one message's JSON payload. A message event
	// travels through outbox_events to NATS, whose stream caps one message at
	// 1 MiB (internal/platform/outbox/jetstream.go), so the payload stays well
	// below that and larger content must travel as an attachment reference.
	IPCPayloadLimit = 256 << 10

	// ipcMaxTextFieldLength bounds the free-text fields of a message so one
	// sender cannot make a row unbounded. It matches the bound the memory
	// store applies to its own text fields.
	ipcMaxTextFieldLength = 255

	// ipcMaxBatchSize bounds one mailbox drain and one acknowledgement batch.
	ipcMaxBatchSize = 1000

	// IPCMessageEventAggregateType is the outbox aggregate type of the event
	// that announces a stored message. The outbox renders subjects as
	// "fenced.events.<aggregate_type lowercased>.<event_type lowercased>" and
	// requires both tokens to match ^[A-Za-z][A-Za-z0-9_]{0,127}$, so the value
	// is pinned here before any delivery code exists: it may not contain '.' or
	// '-'.
	IPCMessageEventAggregateType = "ipc"

	// IPCMessageStoredEventType is the outbox event type of the notification
	// that announces a stored message. Like the aggregate type it is pinned
	// before the delivery code exists, because the outbox renders both into one
	// NATS subject ("fenced.events.ipc.messagestored") and validates each token
	// against ^[A-Za-z][A-Za-z0-9_]{0,127}$.
	//
	// The event announces that a message reached a mailbox. It does not mean the
	// message was delivered to a receiver: the runtime protocol is pull-only, so
	// a receiver observes its mailbox when it chooses to drain it and the
	// platform has no way to notify it.
	IPCMessageStoredEventType = "MessageStored"

	// ipcAddressSeparator joins the two address segments. Neither segment may
	// contain it: the AgentVersion reference grammar ("[namespace/]name@version")
	// admits only [A-Za-z0-9._-] tokens, and the instance is validated to the
	// same restriction, so splitting the canonical form is unambiguous.
	ipcAddressSeparator = "#"
)

// AgentAddress names one mailbox. AgentVersionRef is an immutable reference in
// agentversion.FormatRef form ("[namespace/]name@version"), not a mutable agent
// name. Instance is the receiving run id, never an attempt id: an attempt is a
// replaceable execution attempt of a run, so addressing the attempt would
// orphan every message the moment that attempt failed.
type AgentAddress struct {
	AgentVersionRef string
	Instance        string
}

// Canonical renders the stored form of the address. The round trip through
// ParseAgentAddress is total: Validate rejects any segment containing the
// separator, so the split cannot be ambiguous.
func (a AgentAddress) Canonical() string {
	return a.AgentVersionRef + ipcAddressSeparator + a.Instance
}

// Validate reports whether the address names a single mailbox. A version-only
// address is rejected with ErrIPCAddressUnscoped.
func (a AgentAddress) Validate() error {
	if err := validateIPCSegment("agent version reference", a.AgentVersionRef); err != nil {
		return err
	}
	if strings.TrimSpace(a.Instance) == "" {
		return ErrIPCAddressUnscoped
	}
	return validateIPCSegment("instance", a.Instance)
}

// ParseAgentAddress splits the canonical form written by Canonical.
func ParseAgentAddress(canonical string) (AgentAddress, error) {
	ref, instance, found := strings.Cut(canonical, ipcAddressSeparator)
	if !found {
		return AgentAddress{}, fmt.Errorf("%w: %q carries no %q separator", ErrIPCAddressUnscoped, canonical, ipcAddressSeparator)
	}
	address := AgentAddress{AgentVersionRef: ref, Instance: instance}
	if err := address.Validate(); err != nil {
		return AgentAddress{}, err
	}
	return address, nil
}

// IPCMailboxConsumerName renders the inbox_receipts consumer name a drain of
// the mailbox for runID must use. Receipts are keyed by (tenant, consumer
// name, message id), so scoping the name to the receiving run is what makes a
// redelivery idempotent without letting one run's receipt hide a message from
// another run's drain.
func IPCMailboxConsumerName(runID string) string {
	return "run:" + runID
}

// IPCMessage is one durable message in a mailbox. Sequence is the mailbox
// cursor: it increases inside one mailbox but not by one, because the sequence
// is global to the table.
type IPCMessage struct {
	Sequence            int64
	ID                  uuid.UUID
	TenantID            string
	FromAgentVersionRef string
	To                  AgentAddress
	Kind                string
	Payload             json.RawMessage
	Attachments         json.RawMessage
	CorrelationID       string
	// ReplyToMessageID is uuid.Nil when the message is not a reply.
	ReplyToMessageID uuid.UUID
	TraceID          string
	SentAt           time.Time
	// Deadline is the zero time when the message carries no deadline.
	Deadline time.Time
}

// AppendIPCMessageInput stores one message in the recipient's mailbox.
//
// ID and SentAt are caller-supplied, not generated here: a send retried after
// a connection failure must resolve to the message the first attempt created
// and must report the original send time, so both fields have to survive the
// retry unchanged.
type AppendIPCMessageInput struct {
	TenantID            string
	ID                  uuid.UUID
	FromAgentVersionRef string
	To                  AgentAddress
	Kind                string
	// Payload is the message body as a JSON document. An empty body is
	// written as {}.
	Payload json.RawMessage
	// Attachments is a JSON array of reference objects. It is stored without
	// being interpreted; a payload above IPCPayloadLimit must travel this way.
	Attachments   json.RawMessage
	CorrelationID string
	// ReplyToMessageID is uuid.Nil when the message is not a reply.
	ReplyToMessageID uuid.UUID
	IdempotencyKey   string
	TraceID          string
	SentAt           time.Time
	// Deadline is optional; the zero time stores no deadline.
	Deadline time.Time
}

// Validate rejects a message that cannot be stored as asked. Every check is a
// function of the input alone, never of the wall clock, so replaying the same
// logical send reaches the same verdict however long the retry took.
func (in AppendIPCMessageInput) Validate() error {
	if strings.TrimSpace(in.TenantID) == "" {
		return fmt.Errorf("tenant is required")
	}
	if in.ID == uuid.Nil {
		return fmt.Errorf("message id is required")
	}
	if strings.TrimSpace(in.FromAgentVersionRef) == "" {
		return fmt.Errorf("sender agent version reference is required")
	}
	if err := validateIPCSegment("sender agent version reference", in.FromAgentVersionRef); err != nil {
		return err
	}
	if err := in.To.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(in.Kind) == "" || len(in.Kind) > ipcMaxTextFieldLength {
		return fmt.Errorf("message kind is required and must not exceed %d bytes", ipcMaxTextFieldLength)
	}
	if len(in.Payload) > IPCPayloadLimit {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrIPCPayloadTooLarge, len(in.Payload), IPCPayloadLimit)
	}
	if !json.Valid(in.Payload) {
		return fmt.Errorf("payload must be a JSON document: send {} for a message that carries no data")
	}
	if err := validateIPCAttachments(in.Attachments); err != nil {
		return err
	}
	if strings.TrimSpace(in.IdempotencyKey) == "" || len(in.IdempotencyKey) > ipcMaxTextFieldLength {
		return fmt.Errorf("idempotency key is required and must not exceed %d bytes", ipcMaxTextFieldLength)
	}
	if len(in.CorrelationID) > ipcMaxTextFieldLength || len(in.TraceID) > ipcMaxTextFieldLength {
		return fmt.Errorf("correlation id and trace id must not exceed %d bytes", ipcMaxTextFieldLength)
	}
	if in.SentAt.IsZero() {
		return fmt.Errorf("sent at is required")
	}
	if !in.Deadline.IsZero() && !in.Deadline.After(in.SentAt) {
		return fmt.Errorf("%w: deadline %s is not after sent at %s", ErrIPCDeadlineElapsed, in.Deadline.UTC().Format(time.RFC3339Nano), in.SentAt.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// AppendIPCMessageResult reports the stored message. Replayed is true when the
// idempotency key resolved to a message an earlier attempt had already stored,
// in which case Message is that original message rather than a new one.
type AppendIPCMessageResult struct {
	Message  IPCMessage
	Replayed bool
}

// ListMailboxInput drains one mailbox forward by sequence.
type ListMailboxInput struct {
	TenantID string
	To       AgentAddress
	// AfterSequence is exclusive cursor semantics, matching
	// ListAuditInput.AfterSeq.
	AfterSequence int64
	Limit         int
	// ConsumerName scopes the receipt. Messages already receipted for this
	// consumer are not returned. Use IPCMailboxConsumerName to derive it from
	// the receiving run id.
	ConsumerName string
}

// Validate rejects a drain that could read another mailbox or an unbounded
// page.
func (in ListMailboxInput) Validate() error {
	if strings.TrimSpace(in.TenantID) == "" {
		return fmt.Errorf("tenant is required")
	}
	if err := in.To.Validate(); err != nil {
		return err
	}
	if in.AfterSequence < 0 {
		return fmt.Errorf("after sequence must not be negative")
	}
	if in.Limit <= 0 || in.Limit > ipcMaxBatchSize {
		return fmt.Errorf("limit must be between 1 and %d", ipcMaxBatchSize)
	}
	if strings.TrimSpace(in.ConsumerName) == "" || len(in.ConsumerName) > ipcMaxTextFieldLength {
		return fmt.Errorf("consumer name is required and must not exceed %d bytes", ipcMaxTextFieldLength)
	}
	return nil
}

// AcknowledgeIPCMessagesInput records that a consumer has observed a batch of
// messages, so a later drain does not return them again.
type AcknowledgeIPCMessagesInput struct {
	TenantID       string
	ConsumerName   string
	MessageIDs     []uuid.UUID
	AcknowledgedAt time.Time
}

// Validate rejects an acknowledgement that cannot be applied as asked.
func (in AcknowledgeIPCMessagesInput) Validate() error {
	if strings.TrimSpace(in.TenantID) == "" {
		return fmt.Errorf("tenant is required")
	}
	if strings.TrimSpace(in.ConsumerName) == "" || len(in.ConsumerName) > ipcMaxTextFieldLength {
		return fmt.Errorf("consumer name is required and must not exceed %d bytes", ipcMaxTextFieldLength)
	}
	if len(in.MessageIDs) == 0 || len(in.MessageIDs) > ipcMaxBatchSize {
		return fmt.Errorf("between 1 and %d message ids are required", ipcMaxBatchSize)
	}
	seen := make(map[uuid.UUID]struct{}, len(in.MessageIDs))
	for _, id := range in.MessageIDs {
		if id == uuid.Nil {
			return fmt.Errorf("message ids must not be empty")
		}
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("message id %s appears more than once", id)
		}
		seen[id] = struct{}{}
	}
	if in.AcknowledgedAt.IsZero() {
		return fmt.Errorf("acknowledged at is required")
	}
	return nil
}

// AcknowledgeIPCMessagesResult partitions the batch into receipts this call
// created and receipts an earlier call had already created. Both are applied;
// the split exists so a retry can be told apart from a first attempt.
type AcknowledgeIPCMessagesResult struct {
	Acknowledged        []uuid.UUID
	AlreadyAcknowledged []uuid.UUID
}

// IPCStore is the durable mailbox surface. Delivery is expressed as a side
// effect of the durable write rather than as a method of its own:
// AppendIPCMessage enqueues the dispatch event in the same transaction, so a
// stored message and its announcement cannot diverge. Publishing that event to
// NATS is the dispatcher's job (internal/platform/outbox), and notifying a
// receiver is deliberately absent: fenced.runtime.v1 is pull-only, so a
// receiver drains its mailbox on its own schedule and no method here pretends
// otherwise.
type IPCStore interface {
	// AppendIPCMessage stores one message in the recipient's mailbox. A
	// replay of the same idempotency key returns the message the first attempt
	// stored, with Replayed=true; the same key carrying a different message is
	// ErrIdempotencyConflict.
	//
	// A message that is stored for the first time also enqueues one
	// IPCMessageStoredEventType event into outbox_events in the same
	// transaction. A replay enqueues nothing: the message it resolves to was
	// already announced, and announcing it again would turn one send into two
	// events.
	AppendIPCMessage(context.Context, AppendIPCMessageInput) (AppendIPCMessageResult, error)
	// GetIPCMessage returns one message or ErrNotFound. A message belonging to
	// another tenant is not found.
	GetIPCMessage(context.Context, string, uuid.UUID) (IPCMessage, error)
	// ListMailbox returns the messages addressed to To with a sequence above
	// the cursor that the consumer has not receipted, in mailbox order.
	ListMailbox(context.Context, ListMailboxInput) ([]IPCMessage, error)
	// AcknowledgeIPCMessages records the consumer's receipts for a batch. Only
	// messages that exist in the tenant may be acknowledged: a receipt for an
	// unknown id would hide the real message if it arrived later.
	AcknowledgeIPCMessages(context.Context, AcknowledgeIPCMessagesInput) (AcknowledgeIPCMessagesResult, error)
}

// RunMailboxScope is the mailbox identity a run id resolves to. A mailbox is
// addressed by (agent version reference, run id), and the version reference is
// not a property of the run itself: it comes from the run's task, which is the
// immutable publication resolved during admission.
type RunMailboxScope struct {
	TenantID string
	RunID    string
	TaskID   string
	// AgentVersionRef is the version reference of the run's task. A send that
	// pairs a run with any other version reference names a mailbox that no
	// attempt will ever drain.
	AgentVersionRef string
}

// MailboxScopeStore resolves the run an address names. It is separate from
// IPCStore because it reads the run/task tables rather than the mailbox, and
// because a send needs it before it has anything to store: without the check, a
// sender that mistypes a run id stores a message into a mailbox that is never
// drained, and nothing reports it.
type MailboxScopeStore interface {
	// GetRunMailboxScope returns the scope of runID within the tenant, or
	// ErrNotFound when no such run exists in that tenant.
	GetRunMailboxScope(context.Context, string, uuid.UUID) (RunMailboxScope, error)
}

// validateIPCSegment rejects a value that cannot be one half of a canonical
// mailbox address.
func validateIPCSegment(label, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("ipc %s is required", label)
	}
	if len(value) > ipcMaxTextFieldLength {
		return fmt.Errorf("ipc %s must not exceed %d bytes", label, ipcMaxTextFieldLength)
	}
	if strings.Contains(value, ipcAddressSeparator) {
		return fmt.Errorf("ipc %s must not contain %q", label, ipcAddressSeparator)
	}
	return nil
}

// validateIPCAttachments rejects a stored attachment list that is not a JSON
// array, so a malformed reference list cannot reach the column as an object.
func validateIPCAttachments(attachments json.RawMessage) error {
	if len(attachments) == 0 {
		return nil
	}
	trimmed := bytes.TrimSpace(attachments)
	if len(trimmed) == 0 || trimmed[0] != '[' || !json.Valid(trimmed) {
		return fmt.Errorf("attachments must be a JSON array of references")
	}
	return nil
}
