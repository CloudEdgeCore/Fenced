package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	ipcv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/ipc/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Mailbox drain bounds. A drain returns as soon as it has anything to return, so
// these bound the empty case only.
const (
	ipcDefaultMaxMessages = 20
	ipcMaxMessages        = 1000

	// ipcMaxWaitMillis caps one drain's server-side wait. It is not the only
	// cap: a wait also may not outlive half the caller's remaining lease (see
	// ipcEffectiveWait), and the lease is what actually decides how long a
	// bounded poll may hold on.
	ipcMaxWaitMillis int32 = 10_000

	// ipcPollInterval is how often a waiting drain re-reads the mailbox. There
	// is no notification path to wait on: fenced.runtime.v1 is pull-only and
	// the platform cannot tell a receiver that mail has arrived, so a bounded
	// poll is the only mechanism available. A drain that waits the full
	// ipcMaxWaitMillis issues at most ~40 of these reads, which is the price of
	// pulling without a push channel.
	ipcPollInterval = 250 * time.Millisecond
)

// ipcMessageIDNamespace is the fixed UUIDv5 namespace message ids are derived
// in. Deriving it from the OID namespace keeps the constant a function of a name
// instead of an unexplained literal.
var ipcMessageIDNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("fenced.ipc.v1/message"))

// MailboxScope resolves the mailbox a run id names.
type MailboxScope interface {
	GetRunMailboxScope(context.Context, string, uuid.UUID) (store.RunMailboxScope, error)
}

// PeerAuthorizer decides whether the sending AgentVersion holds an immutable
// grant for the receiving one. *capability.Authorizer satisfies it.
type PeerAuthorizer interface {
	Authorize(context.Context, string, string, capability.Kind, ...string) error
}

// PeerPolicy is the tenant-wide half of the same decision: whether this tenant
// may message that AgentVersion at all. *policy.Engine satisfies it.
//
// It is a narrow consumer-side interface for the same reason tool and model
// gateways declare their own: the service depends on one method, not on the
// policy engine's shape.
type PeerPolicy interface {
	EvaluatePeer(context.Context, string, policy.PeerContext) policy.Decision
}

// IPCService is the fenced agent-to-agent mailbox boundary. Agents reach their
// mailbox through this service rather than through the Control API, so they
// cannot choose a tenant, and can never address a mailbox outside their own run.
//
// Authorization is two layers, checked in the order the tool and model path
// uses (service-layer capability first, then Rego):
//
//   - capability: the sending AgentVersion must grant the receiving one in
//     capabilities.peers. Grants are per immutable version, so republishing a
//     version is what changes them.
//   - tenant policy: the tenant must list the receiving AgentVersion in
//     allowed_peers. This is per tenant and denies by default, so a tenant that
//     has not opted in cannot message anyone regardless of its versions' grants.
//
// Two things it deliberately still does not do:
//
//   - It does not notify a receiver. The platform cannot push into a running
//     agent: fenced.runtime.v1 exposes seven pull RPCs, its HeartbeatResponse
//     carries no payload, and nothing in the repository resembles a wake path.
//     A receiver drains its mailbox on its own schedule.
//   - It does not check the *receiver's* willingness. The grant answers "may I
//     write to you", never "may you write to me", so a sender that knows a run
//     id can reach it. A receiving-side allowlist is a separate design.
type IPCService struct {
	ipcv1.UnimplementedIPCGatewayServiceServer
	mailbox       store.IPCStore
	scopes        MailboxScope
	fences        RuntimeFence
	capabilities  PeerAuthorizer
	peerPolicy    PeerPolicy
	allowedTenant string
}

func NewIPCService(
	mailbox store.IPCStore,
	scopes MailboxScope,
	fences RuntimeFence,
	allowedTenant string,
	capabilities PeerAuthorizer,
	peerPolicy PeerPolicy,
) *IPCService {
	return &IPCService{
		mailbox: mailbox, scopes: scopes, fences: fences,
		capabilities: capabilities, peerPolicy: peerPolicy, allowedTenant: allowedTenant,
	}
}

// SendMessage stores one message in the mailbox its address names.
func (s *IPCService) SendMessage(ctx context.Context, request *ipcv1.SendMessageRequest) (*ipcv1.SendMessageResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	assignment, err := s.fence(ctx, request.GetIdentity(), request.GetAgentVersionRef())
	if err != nil {
		return nil, err
	}
	to := store.AgentAddress{
		AgentVersionRef: request.GetTo().GetAgentVersionRef(),
		Instance:        request.GetTo().GetInstance(),
	}
	if err := to.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Instance is the receiving run id, so it has to parse as one. Validation
	// above only bounds the address grammar; without this check a well-formed
	// but non-UUID instance would reach the mailbox as an address nothing
	// resolves.
	toRunID, parseErr := uuid.Parse(to.Instance)
	if parseErr != nil {
		return nil, status.Error(codes.InvalidArgument, "mailbox instance must be the receiving run id")
	}
	// Authorization runs before the mailbox is resolved, and the order is
	// deliberate. Resolving the run distinguishes "no such run" from "that run
	// belongs to a different version", which would let an unauthorized sender
	// probe which run ids exist by reading the error code. Denying first makes
	// every unauthorized address look the same.
	if err := s.authorizePeer(ctx, assignment, to.AgentVersionRef); err != nil {
		return nil, err
	}
	// A mailbox is addressed by (version reference, run id). Resolving the run
	// is what turns a mistyped instance, or a run paired with a version
	// reference it does not belong to, into an error instead of a message that
	// quietly lands in a mailbox nobody drains.
	scope, scopeErr := s.scopes.GetRunMailboxScope(ctx, assignment.Task.TenantID, toRunID)
	if scopeErr != nil {
		return nil, ipcRPCError(scopeErr)
	}
	if scope.AgentVersionRef != to.AgentVersionRef {
		return nil, status.Error(codes.InvalidArgument, "mailbox names a run that does not belong to that agent version reference")
	}

	attachments, attachErr := ipcEncodeAttachments(request.GetAttachments())
	if attachErr != nil {
		return nil, attachErr
	}
	payload := request.GetPayloadJson()
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	// The message id is not on the wire, so it is derived from the send's
	// identity instead. (tenant, sender, idempotency key) is exactly what the
	// store's idempotency index is keyed by, so a retried send derives the same
	// id, collides with the row the first attempt stored, and replays it.
	messageID := ipcMessageID(assignment.Task.TenantID, assignment.Task.AgentVersionRef, request.GetIdempotencyKey())
	sentAt := time.Now().UTC()
	var deadline time.Time
	if request.GetDeadline() != nil {
		deadline = request.GetDeadline().AsTime().UTC()
	}
	var replyTo uuid.UUID
	if request.GetReplyToMessageId() != "" {
		replyTo, parseErr = uuid.Parse(request.GetReplyToMessageId())
		if parseErr != nil {
			return nil, status.Error(codes.InvalidArgument, "reply target must be a message id")
		}
	}
	input := store.AppendIPCMessageInput{
		TenantID:            assignment.Task.TenantID,
		ID:                  messageID,
		FromAgentVersionRef: assignment.Task.AgentVersionRef,
		To:                  to,
		Kind:                request.GetKind(),
		Payload:             payload,
		Attachments:         attachments,
		CorrelationID:       request.GetCorrelationId(),
		ReplyToMessageID:    replyTo,
		IdempotencyKey:      request.GetIdempotencyKey(),
		// TraceID is left empty: SendMessageRequest carries no trace field, and
		// stitching the OpenTelemetry context into stored messages is the trace
		// stage's concern. IpcMessage.trace_id therefore reads back empty until
		// then, rather than echoing a value no one supplied.
		TraceID:  "",
		SentAt:   sentAt,
		Deadline: deadline,
	}
	if err := input.Validate(); err != nil {
		return nil, ipcInputError(err)
	}
	result, err := s.mailbox.AppendIPCMessage(ctx, input)
	if err != nil {
		return nil, ipcRPCError(err)
	}
	return &ipcv1.SendMessageResponse{
		MessageId: result.Message.ID.String(),
		Replayed:  result.Replayed,
		// A replay reports the send time of the message that was stored, not the
		// time of the retry, so the response is identical whichever attempt the
		// caller's retry reached.
		AcceptedAt: timestamppb.New(result.Message.SentAt),
	}, nil
}

// ReceiveMessages drains the caller's own mailbox. The request carries no
// address on purpose: the only mailbox a caller may drain is the one owned by
// its fenced run, so the address is derived from the fenced assignment rather
// than from anything the caller sent.
func (s *IPCService) ReceiveMessages(ctx context.Context, request *ipcv1.ReceiveMessagesRequest) (*ipcv1.ReceiveMessagesResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	assignment, err := s.fence(ctx, request.GetIdentity(), request.GetAgentVersionRef())
	if err != nil {
		return nil, err
	}
	limit := int(request.GetMaxMessages())
	if limit == 0 {
		limit = ipcDefaultMaxMessages
	}
	if request.GetWaitMillis() < 0 {
		return nil, status.Error(codes.InvalidArgument, "wait millis must not be negative")
	}
	consumer := store.IPCMailboxConsumerName(assignment.Run.ID.String())
	input := store.ListMailboxInput{
		TenantID: assignment.Task.TenantID,
		To: store.AgentAddress{
			AgentVersionRef: assignment.Task.AgentVersionRef,
			Instance:        assignment.Run.ID.String(),
		},
		// The cursor stays at zero: the receipt the drain writes on
		// acknowledgement is what advances the mailbox, so the caller does not
		// have to remember a position across calls and cannot lose one.
		AfterSequence: 0,
		Limit:         limit,
		ConsumerName:  consumer,
	}
	if err := input.Validate(); err != nil {
		return nil, ipcInputError(err)
	}
	expiry := time.Now().Add(ipcEffectiveWait(request.GetWaitMillis(), assignment.Lease.ExpiresAt))
	for {
		messages, err := s.mailbox.ListMailbox(ctx, input)
		if err != nil {
			return nil, ipcRPCError(err)
		}
		if len(messages) > 0 || !time.Now().Before(expiry) {
			converted, convertErr := ipcMessages(messages)
			if convertErr != nil {
				return nil, convertErr
			}
			return &ipcv1.ReceiveMessagesResponse{Messages: converted}, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.Error(codes.Canceled, "mailbox drain cancelled")
		case <-time.After(ipcPollInterval):
		}
	}
}

// AcknowledgeMessage records the caller's receipts for a batch it has drained.
func (s *IPCService) AcknowledgeMessage(ctx context.Context, request *ipcv1.AcknowledgeMessageRequest) (*ipcv1.AcknowledgeMessageResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	assignment, err := s.fence(ctx, request.GetIdentity(), request.GetAgentVersionRef())
	if err != nil {
		return nil, err
	}
	// The receipt key is (tenant, consumer name, message id), so a caller that
	// could choose its consumer name could write a receipt that hides a
	// message from another run's drain. The name is therefore derived from the
	// fenced run; an explicit value is accepted only when it already agrees.
	consumer := store.IPCMailboxConsumerName(assignment.Run.ID.String())
	if supplied := request.GetConsumerName(); supplied != "" && supplied != consumer {
		return nil, status.Error(codes.InvalidArgument, "consumer name is derived from the fenced run and must not be chosen by the caller")
	}
	ids := make([]uuid.UUID, 0, len(request.GetMessageIds()))
	for _, raw := range request.GetMessageIds() {
		id, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			return nil, status.Error(codes.InvalidArgument, "message ids must be message id values")
		}
		ids = append(ids, id)
	}
	input := store.AcknowledgeIPCMessagesInput{
		TenantID:       assignment.Task.TenantID,
		ConsumerName:   consumer,
		MessageIDs:     ids,
		AcknowledgedAt: time.Now().UTC(),
	}
	if err := input.Validate(); err != nil {
		return nil, ipcInputError(err)
	}
	result, err := s.mailbox.AcknowledgeIPCMessages(ctx, input)
	if err != nil {
		return nil, ipcRPCError(err)
	}
	response := &ipcv1.AcknowledgeMessageResponse{}
	for _, id := range result.Acknowledged {
		response.AcknowledgedMessageIds = append(response.AcknowledgedMessageIds, id.String())
	}
	for _, id := range result.AlreadyAcknowledged {
		response.AlreadyAcknowledgedMessageIds = append(response.AlreadyAcknowledgedMessageIds, id.String())
	}
	return response, nil
}

// fence resolves the calling attempt and returns the assignment behind it. Every
// RPC starts here, so no handler can act on an unverified identity.
func (s *IPCService) fence(ctx context.Context, identity *ipcv1.AttemptIdentity, versionRef string) (store.RuntimeAssignment, error) {
	var zero store.RuntimeAssignment
	if identity == nil || identity.GetFencingToken() <= 0 {
		return zero, status.Error(codes.InvalidArgument, "attempt identity and positive fencing token are required")
	}
	if err := authorizeTenant(ctx, s.allowedTenant, identity.GetTenantId()); err != nil {
		return zero, err
	}
	if strings.TrimSpace(versionRef) == "" {
		return zero, status.Error(codes.InvalidArgument, "agent version reference is required")
	}
	if s.fences == nil || s.mailbox == nil || s.scopes == nil {
		return zero, status.Error(codes.PermissionDenied, "ipc gateway enforcement is not configured")
	}
	attemptID, err := uuid.Parse(identity.GetAttemptId())
	if err != nil {
		return zero, status.Error(codes.InvalidArgument, "attempt ID must be a UUID")
	}
	assignment, err := s.fences.GetRuntimeAssignment(ctx, identity.GetTenantId(), attemptID, identity.GetFencingToken())
	if err != nil {
		// A stale token and an attempt that does not exist are reported the same
		// way: the difference is only observable to someone probing for other
		// attempts.
		if errors.Is(err, store.ErrFenced) || errors.Is(err, store.ErrNotFound) {
			return zero, status.Error(codes.PermissionDenied, "attempt identity is stale")
		}
		return zero, ipcRPCError(err)
	}
	if assignment.Task.AgentVersionRef != versionRef {
		return zero, status.Error(codes.PermissionDenied, "agent version does not match the fenced Attempt")
	}
	return assignment, nil
}

// authorizePeer enforces both halves of "may this send happen": the sending
// version's immutable grant for that peer, then the tenant-wide allowlist.
//
// Both are required, and they fail differently on purpose. A missing grant is a
// property of the published AgentVersion, so the fix is to republish it; a
// missing tenant entry is an operator decision, so the fix is the tenant policy
// file. Reporting them under one code would hide which of the two needs to
// change.
//
// Unconfigured enforcement denies rather than passes, mirroring the memory
// boundary: a gateway started without an authorizer must not become an open
// relay.
func (s *IPCService) authorizePeer(ctx context.Context, assignment store.RuntimeAssignment, peer string) error {
	if s.capabilities == nil || s.peerPolicy == nil {
		return status.Error(codes.PermissionDenied, "ipc authorization is not configured")
	}
	tenantID := assignment.Task.TenantID
	if err := s.capabilities.Authorize(ctx, tenantID, assignment.Task.AgentVersionRef, capability.IPC, peer); err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	decision := s.peerPolicy.EvaluatePeer(ctx, tenantID, policy.PeerContext{To: peer})
	if !decision.Allow {
		return status.Error(codes.PermissionDenied,
			"receiving agent is not allowed by tenant policy: "+strings.Join(decision.DenyReasons, ", "))
	}
	return nil
}

// ipcEffectiveWait bounds one drain's server-side wait.
//
// The requested wait is capped twice. A fixed ceiling bounds how long one call
// may occupy a handler, and half the caller's remaining lease bounds how long it
// may outlive the identity it was made under: a drain that ran past its lease
// would hand messages to an attempt that the platform has already superseded.
// The drain never sleeps before its first read, so a short cap costs nothing
// when the mailbox is not empty.
func ipcEffectiveWait(requestedMillis int32, leaseExpiresAt time.Time) time.Duration {
	wait := time.Duration(requestedMillis) * time.Millisecond
	if ceiling := time.Duration(ipcMaxWaitMillis) * time.Millisecond; wait > ceiling {
		wait = ceiling
	}
	if remaining := time.Until(leaseExpiresAt) / 2; remaining < wait {
		wait = remaining
	}
	if wait < 0 {
		return 0
	}
	return wait
}

// ipcMessageID derives the id of a send from the send's identity. The wire
// contract carries no message id, and the store requires the caller to supply
// one so that a retried send resolves to the message the first attempt created
// rather than to a second message. Deriving it from (tenant, sender,
// idempotency key) satisfies both: the same logical send always reaches the same
// id, and the derivation uses exactly the tuple the store's idempotency index is
// keyed by. NUL separators keep the three fields from running together.
func ipcMessageID(tenantID, fromAgentVersionRef, idempotencyKey string) uuid.UUID {
	return uuid.NewSHA1(ipcMessageIDNamespace, []byte(tenantID+"\x00"+fromAgentVersionRef+"\x00"+idempotencyKey))
}

// ipcAttachmentReference is the stored form of one attachment. The body of an
// attachment never travels through a mailbox: a payload above
// store.IPCPayloadLimit has to be referenced this way, which is why the limit
// exists.
type ipcAttachmentReference struct {
	URI       string `json:"uri,omitempty"`
	SHA256    string `json:"sha256,omitempty"`
	SizeBytes int64  `json:"sizeBytes,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
}

func ipcEncodeAttachments(references []*ipcv1.MessageAttachment) (json.RawMessage, error) {
	list := make([]ipcAttachmentReference, 0, len(references))
	for _, reference := range references {
		list = append(list, ipcAttachmentReference{
			URI:       reference.GetUri(),
			SHA256:    reference.GetSha256(),
			SizeBytes: reference.GetSizeBytes(),
			MediaType: reference.GetMediaType(),
		})
	}
	encoded, err := json.Marshal(list)
	if err != nil {
		return nil, status.Error(codes.Internal, "encode message attachments")
	}
	return encoded, nil
}

func ipcMessages(messages []store.IPCMessage) ([]*ipcv1.IpcMessage, error) {
	converted := make([]*ipcv1.IpcMessage, 0, len(messages))
	for _, message := range messages {
		attachments, err := ipcDecodeAttachments(message.Attachments)
		if err != nil {
			return nil, err
		}
		converted = append(converted, &ipcv1.IpcMessage{
			MessageId:           message.ID.String(),
			FromAgentVersionRef: message.FromAgentVersionRef,
			To: &ipcv1.AgentAddress{
				AgentVersionRef: message.To.AgentVersionRef,
				Instance:        message.To.Instance,
			},
			Kind: message.Kind,
			// The column is jsonb, so PostgreSQL returns its own normalized form
			// (object keys reordered, whitespace removed). A reader must compare
			// the payload as a JSON document, never as bytes.
			PayloadJson:   append([]byte(nil), message.Payload...),
			CorrelationId: message.CorrelationID,
			SentAt:        timestamppb.New(message.SentAt),
			TraceId:       message.TraceID,
			Attachments:   attachments,
			// DeliveryAttempt stays at its zero value. ipc_messages has no such
			// column and a pulled message has no server-side delivery count, so
			// the field is reserved until a delivery model defines it.
		})
		if message.ReplyToMessageID != uuid.Nil {
			converted[len(converted)-1].ReplyToMessageId = message.ReplyToMessageID.String()
		}
		if !message.Deadline.IsZero() {
			converted[len(converted)-1].Deadline = timestamppb.New(message.Deadline)
		}
	}
	return converted, nil
}

func ipcDecodeAttachments(encoded json.RawMessage) ([]*ipcv1.MessageAttachment, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var list []ipcAttachmentReference
	if err := json.Unmarshal(encoded, &list); err != nil {
		// The store validates the column as a JSON array of references on write,
		// so a row that cannot be decoded was written by something that bypassed
		// the store. Reporting nothing would hide a real message body, so the
		// call fails instead.
		return nil, status.Error(codes.Internal, "stored message attachments are not readable")
	}
	references := make([]*ipcv1.MessageAttachment, 0, len(list))
	for _, reference := range list {
		references = append(references, &ipcv1.MessageAttachment{
			Uri:       reference.URI,
			Sha256:    reference.SHA256,
			SizeBytes: reference.SizeBytes,
			MediaType: reference.MediaType,
		})
	}
	return references, nil
}

// ipcInputError maps a rejected store input to its status. Every error the store
// input validators return is a function of the request alone, so none of them is
// an internal failure.
func ipcInputError(err error) error {
	if errors.Is(err, store.ErrIPCPayloadTooLarge) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return status.Error(codes.InvalidArgument, err.Error())
}

// ipcRPCError maps a store failure to a status. Internal error text is never
// echoed, so a storage detail cannot reach an agent.
func ipcRPCError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrFenced):
		return status.Error(codes.PermissionDenied, "attempt identity is stale")
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "ipc mailbox target or message is not found")
	case errors.Is(err, store.ErrIPCAddressUnscoped):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrIPCPayloadTooLarge):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, store.ErrIPCDeadlineElapsed):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "idempotency key is already bound to a different message")
	case store.IsRetryableTransaction(err):
		return status.Error(codes.Unavailable, "transient transaction conflict; retry")
	default:
		return status.Error(codes.Internal, "ipc gateway operation failed")
	}
}
