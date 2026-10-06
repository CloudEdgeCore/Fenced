package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
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

// fakeMailbox records what the service asked the store to do and replies with
// scripted results, so the service layer can be tested without a database.
type fakeMailbox struct {
	appended    []store.AppendIPCMessageInput
	appendErr   error
	appendReply *store.AppendIPCMessageResult

	drained      []store.ListMailboxInput
	drainErr     error
	drainReplies [][]store.IPCMessage

	acknowledged []store.AcknowledgeIPCMessagesInput
	ackErr       error
	ackReply     store.AcknowledgeIPCMessagesResult
}

func (f *fakeMailbox) AppendIPCMessage(_ context.Context, in store.AppendIPCMessageInput) (store.AppendIPCMessageResult, error) {
	f.appended = append(f.appended, in)
	if f.appendErr != nil {
		return store.AppendIPCMessageResult{}, f.appendErr
	}
	if f.appendReply != nil {
		return *f.appendReply, nil
	}
	return store.AppendIPCMessageResult{Message: store.IPCMessage{ID: in.ID, TenantID: in.TenantID, Sequence: int64(len(f.appended)), SentAt: in.SentAt}}, nil
}

func (f *fakeMailbox) GetIPCMessage(context.Context, string, uuid.UUID) (store.IPCMessage, error) {
	return store.IPCMessage{}, store.ErrNotFound
}

func (f *fakeMailbox) ListMailbox(_ context.Context, in store.ListMailboxInput) ([]store.IPCMessage, error) {
	f.drained = append(f.drained, in)
	if f.drainErr != nil {
		return nil, f.drainErr
	}
	if len(f.drainReplies) == 0 {
		return nil, nil
	}
	reply := f.drainReplies[0]
	if len(f.drainReplies) > 1 {
		f.drainReplies = f.drainReplies[1:]
	}
	return reply, nil
}

func (f *fakeMailbox) AcknowledgeIPCMessages(_ context.Context, in store.AcknowledgeIPCMessagesInput) (store.AcknowledgeIPCMessagesResult, error) {
	f.acknowledged = append(f.acknowledged, in)
	if f.ackErr != nil {
		return store.AcknowledgeIPCMessagesResult{}, f.ackErr
	}
	return f.ackReply, nil
}

type fakeMailboxScope struct {
	scope       store.RunMailboxScope
	err         error
	requestedAt []uuid.UUID
}

func (f *fakeMailboxScope) GetRunMailboxScope(_ context.Context, _ string, runID uuid.UUID) (store.RunMailboxScope, error) {
	f.requestedAt = append(f.requestedAt, runID)
	if f.err != nil {
		return store.RunMailboxScope{}, f.err
	}
	scope := f.scope
	scope.RunID = runID.String()
	return scope, nil
}

type fakeFence struct {
	assignment store.RuntimeAssignment
	err        error
	calls      int
}

func (f *fakeFence) GetRuntimeAssignment(context.Context, string, uuid.UUID, int64) (store.RuntimeAssignment, error) {
	f.calls++
	if f.err != nil {
		return store.RuntimeAssignment{}, f.err
	}
	return f.assignment, nil
}

const ipcTestTenant = "tenant-ipc"

// fakePeerAuthorizer stands in for the AgentVersion capability authorizer. Its
// zero value allows everything, so tests about the fence and the store do not
// have to restate the grant they are not exercising.
type fakePeerAuthorizer struct {
	err     error
	calls   int
	tenants []string
	peers   []string
}

func (f *fakePeerAuthorizer) Authorize(_ context.Context, tenantID, _ string, _ capability.Kind, candidates ...string) error {
	f.calls++
	f.tenants = append(f.tenants, tenantID)
	f.peers = append(f.peers, candidates...)
	return f.err
}

// fakePeerPolicy stands in for the tenant policy engine. Unlike the authorizer
// its zero value denies, matching the engine it replaces: allow is explicit.
type fakePeerPolicy struct {
	allow  bool
	reason string
	calls  int
	peers  []string
}

func (f *fakePeerPolicy) EvaluatePeer(_ context.Context, _ string, peer policy.PeerContext) policy.Decision {
	f.calls++
	f.peers = append(f.peers, peer.To)
	if f.allow {
		return policy.Decision{Allow: true}
	}
	return policy.Decision{DenyReasons: []string{f.reason}}
}

// ipcTestService wires the service to fakes whose fenced assignment is a live
// attempt, which is the only state any RPC accepts. Authorization is permitted
// by default; use ipcTestServiceWith to exercise a denial.
func ipcTestService(t *testing.T, mailbox *fakeMailbox, scope *fakeMailboxScope, fence *fakeFence) (*IPCService, store.RuntimeAssignment) {
	t.Helper()
	return ipcTestServiceWith(t, mailbox, scope, fence, &fakePeerAuthorizer{}, &fakePeerPolicy{allow: true})
}

func ipcTestServiceWith(t *testing.T, mailbox *fakeMailbox, scope *fakeMailboxScope, fence *fakeFence, authorizer PeerAuthorizer, peerPolicy PeerPolicy) (*IPCService, store.RuntimeAssignment) {
	t.Helper()
	taskID, runID, attemptID := uuid.New(), uuid.New(), uuid.New()
	assignment := store.RuntimeAssignment{
		Task:    store.Task{ID: taskID, TenantID: ipcTestTenant, AgentVersionRef: "planner@1.0.0"},
		Run:     store.Run{ID: runID, TenantID: ipcTestTenant, TaskID: taskID},
		Attempt: store.Attempt{ID: attemptID, TenantID: ipcTestTenant, RunID: runID, FencingToken: 7},
		Lease:   store.Lease{TenantID: ipcTestTenant, RunID: runID, AttemptID: attemptID, FencingToken: 7, ExpiresAt: time.Now().Add(2 * time.Minute)},
	}
	if fence.assignment.Task.ID == uuid.Nil {
		fence.assignment = assignment
	}
	if scope.scope.AgentVersionRef == "" {
		scope.scope.AgentVersionRef = "worker@1.0.0"
	}
	// A fixed tenant is the isolated-development wiring; the peer-bound "*" form
	// needs a verified SPIFFE peer in the context, which a unit test has not got.
	return NewIPCService(mailbox, scope, fence, ipcTestTenant, authorizer, peerPolicy), fence.assignment
}

func ipcTestIdentity(assignment store.RuntimeAssignment) *ipcv1.AttemptIdentity {
	return &ipcv1.AttemptIdentity{
		TenantId:     assignment.Task.TenantID,
		AttemptId:    assignment.Attempt.ID.String(),
		FencingToken: assignment.Attempt.FencingToken,
	}
}

func ipcTestStatus(t *testing.T, err error) codes.Code {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	return status.Code(err)
}

func TestIPCSendMessageEnforcesTheFence(t *testing.T) {
	assignment := store.RuntimeAssignment{
		Task:    store.Task{TenantID: ipcTestTenant, AgentVersionRef: "planner@1.0.0"},
		Attempt: store.Attempt{FencingToken: 7},
	}
	service, _ := ipcTestService(t, &fakeMailbox{}, &fakeMailboxScope{}, &fakeFence{assignment: assignment})

	cases := []struct {
		name     string
		identity *ipcv1.AttemptIdentity
		version  string
		code     codes.Code
	}{
		{"no identity", nil, "planner@1.0.0", codes.InvalidArgument},
		{"no fencing token", &ipcv1.AttemptIdentity{TenantId: ipcTestTenant, AttemptId: assignment.Attempt.ID.String()}, "planner@1.0.0", codes.InvalidArgument},
		{"no version reference", ipcTestIdentity(assignment), "", codes.InvalidArgument},
		{"attempt id is not a uuid", &ipcv1.AttemptIdentity{TenantId: ipcTestTenant, AttemptId: "not-a-uuid", FencingToken: 7}, "planner@1.0.0", codes.InvalidArgument},
		{"another tenant", &ipcv1.AttemptIdentity{TenantId: "tenant-other", AttemptId: assignment.Attempt.ID.String(), FencingToken: 7}, "planner@1.0.0", codes.PermissionDenied},
		{"version does not match the attempt", ipcTestIdentity(assignment), "other@1.0.0", codes.PermissionDenied},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := service.SendMessage(context.Background(), &ipcv1.SendMessageRequest{
				Identity: testCase.identity, AgentVersionRef: testCase.version,
				To:   &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0", Instance: uuid.New().String()},
				Kind: "handoff", IdempotencyKey: "key-1",
			})
			if got := ipcTestStatus(t, err); got != testCase.code {
				t.Fatalf("status = %s, want %s (%v)", got, testCase.code, err)
			}
		})
	}
}

func TestIPCSendMessageResolvesTheRecipientMailbox(t *testing.T) {
	t.Run("a stale fence is a permission failure, not a missing database row", func(t *testing.T) {
		service, assignment := ipcTestService(t, &fakeMailbox{}, &fakeMailboxScope{}, &fakeFence{err: store.ErrFenced})
		_, err := service.SendMessage(context.Background(), &ipcv1.SendMessageRequest{
			Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
			To:   &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0", Instance: uuid.New().String()},
			Kind: "handoff", IdempotencyKey: "key-1",
		})
		if got := ipcTestStatus(t, err); got != codes.PermissionDenied {
			t.Fatalf("status = %s, want PermissionDenied", got)
		}
	})

	t.Run("an unscoped mailbox has no instance", func(t *testing.T) {
		mailbox := &fakeMailbox{}
		service, assignment := ipcTestService(t, mailbox, &fakeMailboxScope{}, &fakeFence{})
		_, err := service.SendMessage(context.Background(), &ipcv1.SendMessageRequest{
			Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
			To:   &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0"},
			Kind: "handoff", IdempotencyKey: "key-1",
		})
		if got := ipcTestStatus(t, err); got != codes.InvalidArgument {
			t.Fatalf("status = %s, want InvalidArgument", got)
		}
		if len(mailbox.appended) != 0 {
			t.Fatal("an address with no instance reached the store")
		}
	})

	t.Run("an instance that is not a run id is refused", func(t *testing.T) {
		mailbox := &fakeMailbox{}
		service, assignment := ipcTestService(t, mailbox, &fakeMailboxScope{}, &fakeFence{})
		_, err := service.SendMessage(context.Background(), &ipcv1.SendMessageRequest{
			Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
			To:   &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0", Instance: "run-1"},
			Kind: "handoff", IdempotencyKey: "key-1",
		})
		if got := ipcTestStatus(t, err); got != codes.InvalidArgument {
			t.Fatalf("status = %s, want InvalidArgument", got)
		}
		if len(mailbox.appended) != 0 {
			t.Fatal("a non-uuid instance reached the store")
		}
	})

	t.Run("an unknown recipient run is not found", func(t *testing.T) {
		service, assignment := ipcTestService(t, &fakeMailbox{}, &fakeMailboxScope{err: store.ErrNotFound}, &fakeFence{})
		_, err := service.SendMessage(context.Background(), &ipcv1.SendMessageRequest{
			Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
			To:   &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0", Instance: uuid.New().String()},
			Kind: "handoff", IdempotencyKey: "key-1",
		})
		if got := ipcTestStatus(t, err); got != codes.NotFound {
			t.Fatalf("status = %s, want NotFound", got)
		}
	})

	t.Run("a run paired with the wrong version reference is refused", func(t *testing.T) {
		mailbox := &fakeMailbox{}
		scope := &fakeMailboxScope{scope: store.RunMailboxScope{AgentVersionRef: "worker@2.0.0"}}
		service, assignment := ipcTestService(t, mailbox, scope, &fakeFence{})
		_, err := service.SendMessage(context.Background(), &ipcv1.SendMessageRequest{
			Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
			To:   &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0", Instance: uuid.New().String()},
			Kind: "handoff", IdempotencyKey: "key-1",
		})
		if got := ipcTestStatus(t, err); got != codes.InvalidArgument {
			t.Fatalf("status = %s, want InvalidArgument", got)
		}
		if len(mailbox.appended) != 0 {
			t.Fatal("a message addressed to a mailbox nobody drains reached the store")
		}
	})
}

func TestIPCSendMessageDerivesTheSameIDForARetry(t *testing.T) {
	mailbox := &fakeMailbox{}
	service, assignment := ipcTestService(t, mailbox, &fakeMailboxScope{}, &fakeFence{})
	runID := uuid.New().String()
	request := &ipcv1.SendMessageRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
		To:   &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0", Instance: runID},
		Kind: "handoff", IdempotencyKey: "retry-me",
		PayloadJson: []byte(`{"goal":"summarize"}`),
	}
	first, err := service.SendMessage(context.Background(), request)
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	// A replay from the store reports the message an earlier attempt stored.
	mailbox.appendReply = &store.AppendIPCMessageResult{
		Message:  store.IPCMessage{ID: uuid.MustParse(first.GetMessageId()), TenantID: ipcTestTenant, SentAt: time.Now().UTC()},
		Replayed: true,
	}
	second, err := service.SendMessage(context.Background(), request)
	if err != nil {
		t.Fatalf("retried send: %v", err)
	}
	if first.GetMessageId() != second.GetMessageId() {
		t.Fatalf("a retry derived a different message id: %s then %s", first.GetMessageId(), second.GetMessageId())
	}
	if !second.GetReplayed() {
		t.Fatal("the retry did not report the store's replay")
	}
	if len(mailbox.appended) != 2 {
		t.Fatalf("store saw %d appends, want 2", len(mailbox.appended))
	}
	if mailbox.appended[0].ID != mailbox.appended[1].ID {
		t.Fatal("the two attempts stored different message ids")
	}
	if mailbox.appended[0].FromAgentVersionRef != "planner@1.0.0" {
		t.Fatalf("sender is %q, want the fenced version reference", mailbox.appended[0].FromAgentVersionRef)
	}
	if mailbox.appended[0].Payload == nil || !json.Valid(mailbox.appended[0].Payload) {
		t.Fatalf("payload reached the store as %q", mailbox.appended[0].Payload)
	}
}

func TestIPCSendMessageRejectsAMessageItCannotStore(t *testing.T) {
	mailbox := &fakeMailbox{}
	service, assignment := ipcTestService(t, mailbox, &fakeMailboxScope{}, &fakeFence{})
	target := &ipcv1.AgentAddress{AgentVersionRef: "worker@1.0.0", Instance: uuid.New().String()}

	oversized := make([]byte, store.IPCPayloadLimit+1)
	for index := range oversized {
		oversized[index] = 'a'
	}
	cases := []struct {
		name    string
		request *ipcv1.SendMessageRequest
		code    codes.Code
	}{
		{
			name: "payload above the mailbox limit",
			request: &ipcv1.SendMessageRequest{
				Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", To: target,
				Kind: "handoff", IdempotencyKey: "key-1", PayloadJson: oversized,
			},
			code: codes.ResourceExhausted,
		},
		{
			name: "deadline that does not lie after the send",
			request: &ipcv1.SendMessageRequest{
				Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", To: target,
				Kind: "handoff", IdempotencyKey: "key-1",
				Deadline: timestamppb.New(time.Now().Add(-time.Minute)),
			},
			code: codes.InvalidArgument,
		},
		{
			name: "kind is required",
			request: &ipcv1.SendMessageRequest{
				Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", To: target,
				IdempotencyKey: "key-1",
			},
			code: codes.InvalidArgument,
		},
		{
			name: "idempotency key is required",
			request: &ipcv1.SendMessageRequest{
				Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", To: target, Kind: "handoff",
			},
			code: codes.InvalidArgument,
		},
		{
			name: "reply target is not a message id",
			request: &ipcv1.SendMessageRequest{
				Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", To: target,
				Kind: "handoff", IdempotencyKey: "key-1", ReplyToMessageId: "not-a-uuid",
			},
			code: codes.InvalidArgument,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := service.SendMessage(context.Background(), testCase.request); ipcTestStatus(t, err) != testCase.code {
				t.Fatalf("status = %s, want %s (%v)", status.Code(err), testCase.code, err)
			}
		})
	}
	if len(mailbox.appended) != 0 {
		t.Fatalf("%d rejected sends reached the store", len(mailbox.appended))
	}
}

func TestIPCReceiveMessagesDrainsTheFencedRunsMailbox(t *testing.T) {
	mailbox := &fakeMailbox{drainReplies: [][]store.IPCMessage{{{
		ID: uuid.New(), TenantID: ipcTestTenant, FromAgentVersionRef: "manager@1.0.0", Kind: "handoff",
		To:      store.AgentAddress{AgentVersionRef: "planner@1.0.0", Instance: "ignored"},
		Payload: json.RawMessage(`{"goal":"summarize"}`), SentAt: time.Now().UTC(),
	}}}}
	service, assignment := ipcTestService(t, mailbox, &fakeMailboxScope{}, &fakeFence{})

	response, err := service.ReceiveMessages(context.Background(), &ipcv1.ReceiveMessagesRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
	})
	if err != nil {
		t.Fatalf("receive messages: %v", err)
	}
	if len(mailbox.drained) != 1 {
		t.Fatalf("a non-empty mailbox took %d reads, want 1", len(mailbox.drained))
	}
	drain := mailbox.drained[0]
	wantAddress := store.AgentAddress{AgentVersionRef: "planner@1.0.0", Instance: assignment.Run.ID.String()}
	if drain.To != wantAddress {
		t.Fatalf("drained mailbox %+v, want the fenced run's %+v", drain.To, wantAddress)
	}
	if drain.ConsumerName != store.IPCMailboxConsumerName(assignment.Run.ID.String()) {
		t.Fatalf("consumer is %q, want the fenced run's", drain.ConsumerName)
	}
	if drain.TenantID != ipcTestTenant {
		t.Fatalf("drained tenant %q", drain.TenantID)
	}
	if drain.Limit != ipcDefaultMaxMessages {
		t.Fatalf("default limit is %d, want %d", drain.Limit, ipcDefaultMaxMessages)
	}
	if drain.AfterSequence != 0 {
		t.Fatalf("drain started at cursor %d, want 0", drain.AfterSequence)
	}
	if len(response.GetMessages()) != 1 || response.GetMessages()[0].GetKind() != "handoff" {
		t.Fatalf("drain returned %+v", response.GetMessages())
	}
	// A pulled message has no server-side delivery count, so the reserved field
	// stays at its zero value rather than reporting a number nobody computed.
	if response.GetMessages()[0].GetDeliveryAttempt() != 0 {
		t.Fatalf("delivery attempt is %d, want 0", response.GetMessages()[0].GetDeliveryAttempt())
	}
}

func TestIPCReceiveMessagesCapsTheWaitAtHalfTheRemainingLease(t *testing.T) {
	mailbox := &fakeMailbox{}
	fence := &fakeFence{}
	service, _ := ipcTestService(t, mailbox, &fakeMailboxScope{}, fence)
	// The lease expires almost immediately, so the poll may not run for the
	// second the caller asked for: a drain that outlives its lease would hand
	// messages to an attempt the platform has already superseded.
	fence.assignment.Lease.ExpiresAt = time.Now().Add(400 * time.Millisecond)
	assignment := fence.assignment

	started := time.Now()
	if _, err := service.ReceiveMessages(context.Background(), &ipcv1.ReceiveMessagesRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", WaitMillis: 10_000,
	}); err != nil {
		t.Fatalf("receive messages: %v", err)
	}
	elapsed := time.Since(started)
	if elapsed > 2*time.Second {
		t.Fatalf("the drain ran for %s, so the lease cap was not applied", elapsed)
	}
	if len(mailbox.drained) < 2 {
		t.Fatalf("the drain read the mailbox %d times, want at least 2 polls before giving up", len(mailbox.drained))
	}
}

func TestIPCReceiveMessagesRejectsANegativeWait(t *testing.T) {
	service, assignment := ipcTestService(t, &fakeMailbox{}, &fakeMailboxScope{}, &fakeFence{})
	_, err := service.ReceiveMessages(context.Background(), &ipcv1.ReceiveMessagesRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", WaitMillis: -1,
	})
	if got := ipcTestStatus(t, err); got != codes.InvalidArgument {
		t.Fatalf("status = %s, want InvalidArgument", got)
	}
}

func TestIPCAcknowledgeDerivesTheConsumerName(t *testing.T) {
	mailbox := &fakeMailbox{ackReply: store.AcknowledgeIPCMessagesResult{Acknowledged: []uuid.UUID{uuid.New()}}}
	service, assignment := ipcTestService(t, mailbox, &fakeMailboxScope{}, &fakeFence{})
	messageID := mailbox.ackReply.Acknowledged[0].String()

	// A caller that names its own consumer could write a receipt that hides a
	// message from another run's drain, so an explicit name is refused unless it
	// already agrees with the fenced run's.
	_, err := service.AcknowledgeMessage(context.Background(), &ipcv1.AcknowledgeMessageRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
		ConsumerName: "run:" + uuid.New().String(), MessageIds: []string{messageID},
	})
	if got := ipcTestStatus(t, err); got != codes.InvalidArgument {
		t.Fatalf("status = %s, want InvalidArgument", got)
	}
	if len(mailbox.acknowledged) != 0 {
		t.Fatal("a caller-chosen consumer name reached the store")
	}

	response, err := service.AcknowledgeMessage(context.Background(), &ipcv1.AcknowledgeMessageRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", MessageIds: []string{messageID},
	})
	if err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	if len(mailbox.acknowledged) != 1 {
		t.Fatalf("store saw %d acknowledgements, want 1", len(mailbox.acknowledged))
	}
	if mailbox.acknowledged[0].ConsumerName != store.IPCMailboxConsumerName(assignment.Run.ID.String()) {
		t.Fatalf("receipt consumer is %q, want the fenced run's", mailbox.acknowledged[0].ConsumerName)
	}
	if len(response.GetAcknowledgedMessageIds()) != 1 {
		t.Fatalf("acknowledged %v, want the one message", response.GetAcknowledgedMessageIds())
	}
}

func TestIPCAcknowledgeRejectsABatchItCannotApply(t *testing.T) {
	mailbox := &fakeMailbox{}
	service, assignment := ipcTestService(t, mailbox, &fakeMailboxScope{}, &fakeFence{})
	cases := []struct {
		name       string
		messageIDs []string
	}{
		{"empty batch", nil},
		{"id is not a message id", []string{"not-a-uuid"}},
		{"an empty id in the batch", []string{uuid.New().String(), ""}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := service.AcknowledgeMessage(context.Background(), &ipcv1.AcknowledgeMessageRequest{
				Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0", MessageIds: testCase.messageIDs,
			})
			if got := ipcTestStatus(t, err); got != codes.InvalidArgument {
				t.Fatalf("status = %s, want InvalidArgument", got)
			}
		})
	}
	duplicate := uuid.New().String()
	if _, err := service.AcknowledgeMessage(context.Background(), &ipcv1.AcknowledgeMessageRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: "planner@1.0.0",
		MessageIds: []string{duplicate, duplicate},
	}); ipcTestStatus(t, err) != codes.InvalidArgument {
		t.Fatalf("a duplicated message id was accepted: %v", err)
	}
	if len(mailbox.acknowledged) != 0 {
		t.Fatalf("%d rejected batches reached the store", len(mailbox.acknowledged))
	}
}

func TestIPCGatewayIsNotConfiguredWithoutItsDependencies(t *testing.T) {
	service := NewIPCService(nil, nil, nil, ipcTestTenant, nil, nil)
	_, err := service.ReceiveMessages(context.Background(), &ipcv1.ReceiveMessagesRequest{
		Identity:        &ipcv1.AttemptIdentity{TenantId: ipcTestTenant, AttemptId: uuid.New().String(), FencingToken: 1},
		AgentVersionRef: "planner@1.0.0",
	})
	if got := ipcTestStatus(t, err); got != codes.PermissionDenied {
		t.Fatalf("status = %s, want PermissionDenied", got)
	}
}

// ipcSendRequest is a well-formed send, so a test that wants a denial cannot
// be passing because of malformed input.
func ipcSendRequest(assignment store.RuntimeAssignment, to string) *ipcv1.SendMessageRequest {
	return &ipcv1.SendMessageRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: assignment.Task.AgentVersionRef,
		To:   &ipcv1.AgentAddress{AgentVersionRef: to, Instance: uuid.New().String()},
		Kind: "handoff", IdempotencyKey: "key-1",
	}
}

func TestIPCSendMessageRequiresThePeerGrant(t *testing.T) {
	denied := &fakePeerAuthorizer{err: capability.ErrDenied}
	policyFake := &fakePeerPolicy{allow: true}
	mailbox := &fakeMailbox{}
	service, assignment := ipcTestServiceWith(t, mailbox, &fakeMailboxScope{}, &fakeFence{}, denied, policyFake)

	_, err := service.SendMessage(context.Background(), ipcSendRequest(assignment, "worker@1.0.0"))
	if got := ipcTestStatus(t, err); got != codes.PermissionDenied {
		t.Fatalf("status = %s, want PermissionDenied", got)
	}
	if len(mailbox.appended) != 0 {
		t.Fatalf("%d denied sends reached the store", len(mailbox.appended))
	}
	if policyFake.calls != 0 {
		t.Fatal("tenant policy ran even though the capability denied")
	}
	if len(denied.tenants) != 1 || len(denied.peers) != 1 {
		t.Fatalf("authorizer calls = %d tenants / %d peers, want 1 each", len(denied.tenants), len(denied.peers))
	}
	if denied.tenants[0] != ipcTestTenant || denied.peers[0] != "worker@1.0.0" {
		t.Fatalf("authorizer asked about %v in %v", denied.peers, denied.tenants)
	}
}

func TestIPCSendMessageRequiresTheTenantToAllowThePeer(t *testing.T) {
	policyFake := &fakePeerPolicy{reason: "PEER_NOT_ALLOWED"}
	mailbox := &fakeMailbox{}
	service, assignment := ipcTestServiceWith(t, mailbox, &fakeMailboxScope{}, &fakeFence{},
		&fakePeerAuthorizer{}, policyFake)

	_, err := service.SendMessage(context.Background(), ipcSendRequest(assignment, "worker@1.0.0"))
	if got := ipcTestStatus(t, err); got != codes.PermissionDenied {
		t.Fatalf("status = %s, want PermissionDenied", got)
	}
	if !strings.Contains(err.Error(), "PEER_NOT_ALLOWED") {
		t.Fatalf("denial does not name the policy reason: %v", err)
	}
	if len(mailbox.appended) != 0 {
		t.Fatalf("%d denied sends reached the store", len(mailbox.appended))
	}
}

func TestIPCSendMessageDeniesWhenAuthorizationIsUnconfigured(t *testing.T) {
	// A gateway started without an authorizer must not become an open relay,
	// even though the fence alone would accept the call.
	mailbox := &fakeMailbox{}
	scope := &fakeMailboxScope{}
	service, assignment := ipcTestServiceWith(t, mailbox, scope, &fakeFence{}, nil, nil)

	_, err := service.SendMessage(context.Background(), ipcSendRequest(assignment, "worker@1.0.0"))
	if got := ipcTestStatus(t, err); got != codes.PermissionDenied {
		t.Fatalf("status = %s, want PermissionDenied", got)
	}
	// The denial must come from the missing authorizer, not from an earlier
	// check that happens to return the same code.
	if !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("denial did not come from the unconfigured authorizer: %v", err)
	}
	if len(mailbox.appended) != 0 {
		t.Fatalf("%d unconfigured sends reached the store", len(mailbox.appended))
	}
	if len(scope.requestedAt) != 0 {
		t.Fatal("the mailbox was resolved before authorization decided")
	}
}

func TestIPCSendMessageAuthorizesBeforeResolvingTheMailbox(t *testing.T) {
	// The order is part of the contract, not an implementation detail: probing
	// which run ids exist is only possible if the mailbox is resolved first.
	scope := &fakeMailboxScope{err: store.ErrNotFound}
	denied := &fakePeerAuthorizer{err: capability.ErrDenied}
	service, assignment := ipcTestServiceWith(t, &fakeMailbox{}, scope, &fakeFence{},
		denied, &fakePeerPolicy{allow: true})

	_, err := service.SendMessage(context.Background(), ipcSendRequest(assignment, "worker@1.0.0"))
	if got := ipcTestStatus(t, err); got != codes.PermissionDenied {
		t.Fatalf("status = %s, want PermissionDenied for an unauthorized sender", got)
	}
	if denied.calls != 1 {
		t.Fatalf("authorizer calls = %d, want 1: the denial came from somewhere else", denied.calls)
	}
	if len(scope.requestedAt) != 0 {
		t.Fatal("an unauthorized sender learned whether the run exists")
	}
}

func TestIPCReceiveAndAcknowledgeNeedNoPeerGrant(t *testing.T) {
	// Both RPCs can only ever reach the caller's own fenced run, so a grant
	// would be an empty action. They must keep working with no authorizer.
	mailbox := &fakeMailbox{drainReplies: [][]store.IPCMessage{{{
		ID: uuid.New(), TenantID: ipcTestTenant, FromAgentVersionRef: "worker@1.0.0",
		To: store.AgentAddress{AgentVersionRef: "planner@1.0.0"}, Kind: "handoff",
		Payload: json.RawMessage(`{}`), SentAt: time.Now().UTC(),
	}}}}
	service, assignment := ipcTestServiceWith(t, mailbox, &fakeMailboxScope{}, &fakeFence{}, nil, nil)

	if _, err := service.ReceiveMessages(context.Background(), &ipcv1.ReceiveMessagesRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: assignment.Task.AgentVersionRef,
	}); err != nil {
		t.Fatalf("receive needed a peer grant: %v", err)
	}
	if _, err := service.AcknowledgeMessage(context.Background(), &ipcv1.AcknowledgeMessageRequest{
		Identity: ipcTestIdentity(assignment), AgentVersionRef: assignment.Task.AgentVersionRef,
		MessageIds: []string{uuid.New().String()},
	}); err != nil {
		t.Fatalf("acknowledge needed a peer grant: %v", err)
	}
}

func TestIPCMessageIDIsAFunctionOfTheSendsIdentity(t *testing.T) {
	base := ipcMessageID(ipcTestTenant, "planner@1.0.0", "key-1")
	if base != ipcMessageID(ipcTestTenant, "planner@1.0.0", "key-1") {
		t.Fatal("the same send derived two different message ids")
	}
	distinct := map[uuid.UUID]string{}
	for name, id := range map[string]uuid.UUID{
		"key":    ipcMessageID(ipcTestTenant, "planner@1.0.0", "key-2"),
		"sender": ipcMessageID(ipcTestTenant, "planner@1.1.0", "key-1"),
		"tenant": ipcMessageID("tenant-other", "planner@1.0.0", "key-1"),
	} {
		if id == base {
			t.Fatalf("%s did not separate the derivation", name)
		}
		if _, clash := distinct[id]; clash {
			t.Fatalf("%s collided with another derivation", name)
		}
		distinct[id] = name
	}
	// The derivation must not let two field values run together: a NUL separator
	// keeps ("ab", "c") from reading as ("a", "bc").
	if ipcMessageID(ipcTestTenant, "ab", "c") == ipcMessageID(ipcTestTenant, "a", "bc") {
		t.Fatal("field values ran together in the derivation")
	}
}

func TestIPCRPCMappingKeepsStorageDetailInside(t *testing.T) {
	cases := []struct {
		err  error
		code codes.Code
	}{
		{store.ErrFenced, codes.PermissionDenied},
		{store.ErrNotFound, codes.NotFound},
		{store.ErrIPCAddressUnscoped, codes.InvalidArgument},
		{store.ErrIPCPayloadTooLarge, codes.ResourceExhausted},
		{store.ErrIPCDeadlineElapsed, codes.InvalidArgument},
		{store.ErrIdempotencyConflict, codes.AlreadyExists},
		{store.ErrRetryableTransaction, codes.Unavailable},
		{errors.New("connection refused to 10.0.0.5:5432"), codes.Internal},
	}
	for _, testCase := range cases {
		mapped := ipcRPCError(testCase.err)
		if status.Code(mapped) != testCase.code {
			t.Fatalf("%v mapped to %s, want %s", testCase.err, status.Code(mapped), testCase.code)
		}
		if testCase.code == codes.Internal && strings.Contains(status.Convert(mapped).Message(), "10.0.0.5") {
			t.Fatalf("internal status echoed the storage detail: %v", mapped)
		}
	}
}
