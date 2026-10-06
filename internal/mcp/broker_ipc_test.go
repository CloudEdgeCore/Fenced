package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// fakeMailboxBroker records the calls the broker makes and replies with scripted
// outcomes, so the system tools can be tested without a gateway.
type fakeMailboxBroker struct {
	identities []AttemptContext
	sends      []MailboxSendInput
	sendErr    error
	sendReply  MailboxSendOutcome

	receives   []MailboxReceiveInput
	receiveErr error
	receiveOut []MailboxMessage

	acks     []MailboxAckInput
	ackErr   error
	ackReply MailboxAckOutcome
}

func (f *fakeMailboxBroker) SendMessage(_ context.Context, identity AttemptContext, in MailboxSendInput) (MailboxSendOutcome, error) {
	f.identities = append(f.identities, identity)
	f.sends = append(f.sends, in)
	if f.sendErr != nil {
		return MailboxSendOutcome{}, f.sendErr
	}
	reply := f.sendReply
	if reply.MessageID == "" {
		reply.MessageID = uuid.New().String()
	}
	if reply.AcceptedAt.IsZero() {
		reply.AcceptedAt = time.Now().UTC()
	}
	return reply, nil
}

func (f *fakeMailboxBroker) ReceiveMessages(_ context.Context, identity AttemptContext, in MailboxReceiveInput) ([]MailboxMessage, error) {
	f.identities = append(f.identities, identity)
	f.receives = append(f.receives, in)
	if f.receiveErr != nil {
		return nil, f.receiveErr
	}
	return f.receiveOut, nil
}

func (f *fakeMailboxBroker) AcknowledgeMessages(_ context.Context, identity AttemptContext, in MailboxAckInput) (MailboxAckOutcome, error) {
	f.identities = append(f.identities, identity)
	f.acks = append(f.acks, in)
	if f.ackErr != nil {
		return MailboxAckOutcome{}, f.ackErr
	}
	return f.ackReply, nil
}

func newIPCBrokerForTest(t *testing.T, identity AttemptContext, mailbox MailboxBroker) *Broker {
	t.Helper()
	slot := &StaticIdentity{Context: identity}
	return NewBroker(NewToolAdapter(&fakeToolInvokerForBroker{}, slot), nil, nil, nil, mailbox, slot)
}

func listedToolNames(t *testing.T, broker *Broker) map[string]map[string]any {
	t.Helper()
	listed, rpcErr := broker.ListTools(context.Background(), json.RawMessage(`{}`))
	if rpcErr != nil {
		t.Fatalf("list tools: %v", rpcErr)
	}
	names := map[string]map[string]any{}
	for _, tool := range listed.(map[string]any)["tools"].([]map[string]any) {
		names[tool["name"].(string)] = tool
	}
	return names
}

func TestBrokerListsIPCToolsOnlyWhenAMailboxIsConfigured(t *testing.T) {
	names := listedToolNames(t, newIPCBrokerForTest(t, brokerTestContext(), &fakeMailboxBroker{}))
	for _, expected := range []string{SystemIPCSend, SystemIPCReceive, SystemIPCAck} {
		declaration, ok := names[expected]
		if !ok {
			t.Fatalf("tool %q is not listed alongside %v", expected, names)
		}
		// The revision is how an agent that caches tools/list learns the surface
		// changed, so it has to be advertised on every declaration.
		if !strings.Contains(declaration["description"].(string), systemToolsRevision) {
			t.Fatalf("tool %q does not carry the revision %q: %v", expected, systemToolsRevision, declaration["description"])
		}
		if declaration["inputSchema"] == nil {
			t.Fatalf("tool %q has no schema", expected)
		}
	}

	unconfigured := listedToolNames(t, newIPCBrokerForTest(t, brokerTestContext(), nil))
	for _, absent := range []string{SystemIPCSend, SystemIPCReceive, SystemIPCAck} {
		if _, ok := unconfigured[absent]; ok {
			t.Fatalf("tool %q is listed although no mailbox is configured", absent)
		}
	}
}

func TestBrokerIPCReceiveToolWaitsForABoundedTime(t *testing.T) {
	// The receive schema carries the bounded wait, because there is no push
	// channel: an agent that does not wait has to poll on its own schedule.
	schema := string(schemaFor[ipcReceiveToolInput]())
	for _, expected := range []string{"maxMessages", "waitMillis"} {
		if !strings.Contains(schema, expected) {
			t.Fatalf("receive schema does not offer %q: %s", expected, schema)
		}
	}
	if strings.Contains(schema, "toRunId") || strings.Contains(schema, "payloadJson") {
		t.Fatalf("receive schema exposes a send field: %s", schema)
	}
}

func TestBrokerIPCSendDerivesEverythingButTheRecipient(t *testing.T) {
	mailbox := &fakeMailboxBroker{sendReply: MailboxSendOutcome{
		MessageID: uuid.MustParse("00000000-0000-0000-0000-0000000000ff").String(),
		Replayed:  true,
	}}
	broker := newIPCBrokerForTest(t, brokerTestContext(), mailbox)
	runID := uuid.New().String()

	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemIPCSend,
		"arguments": map[string]any{
			"toAgentVersionRef": "worker@1.0.0", "toRunId": runID, "kind": "handoff",
			"payloadJson": map[string]any{"goal": "summarize"}, "idempotencyKey": "key-1",
			"correlationId": "corr-1", "deadlineSeconds": 60,
		},
	}))
	if rpcErr != nil {
		t.Fatalf("send: %v", rpcErr)
	}
	if len(mailbox.sends) != 1 {
		t.Fatalf("broker saw %d sends, want 1", len(mailbox.sends))
	}
	sent := mailbox.sends[0]
	if sent.ToAgentVersionRef != "worker@1.0.0" || sent.ToRunID != runID {
		t.Fatalf("send target is %+v", sent)
	}
	if sent.Kind != "handoff" || sent.IdempotencyKey != "key-1" || sent.CorrelationID != "corr-1" {
		t.Fatalf("send lost its addressing: %+v", sent)
	}
	if !json.Valid(sent.PayloadJSON) || !strings.Contains(string(sent.PayloadJSON), "summarize") {
		t.Fatalf("send payload is %q", sent.PayloadJSON)
	}
	if sent.Deadline.IsZero() || !sent.Deadline.After(time.Now()) {
		t.Fatalf("deadline is %s, want a future instant", sent.Deadline)
	}
	// The tenant and the sender come from the worker-injected identity, never
	// from the agent's arguments: those fields do not exist in the schema.
	identity := mailbox.identities[0]
	if identity.TenantID != "tenant-1" || identity.AgentVersionRef != "agent@1" {
		t.Fatalf("send ran under identity %+v", identity)
	}
	document := decodeToolJSON(t, result)
	if document["messageId"] != mailbox.sendReply.MessageID {
		t.Fatalf("send reported message %v, want %s", document["messageId"], mailbox.sendReply.MessageID)
	}
	if document["replayed"] != true {
		t.Fatalf("send reported replayed=%v, want the broker's replay", document["replayed"])
	}
}

func TestBrokerIPCSendRejectsArgumentsItCannotUse(t *testing.T) {
	broker := newIPCBrokerForTest(t, brokerTestContext(), &fakeMailboxBroker{})
	target := map[string]any{"toAgentVersionRef": "worker@1.0.0", "toRunId": uuid.New().String()}

	cases := []struct {
		name      string
		arguments map[string]any
	}{
		{"no recipient run", map[string]any{"toAgentVersionRef": "worker@1.0.0", "kind": "handoff", "idempotencyKey": "k"}},
		{"no kind", map[string]any{"toAgentVersionRef": target["toAgentVersionRef"], "toRunId": target["toRunId"], "idempotencyKey": "k"}},
		{"no idempotency key", map[string]any{"toAgentVersionRef": target["toAgentVersionRef"], "toRunId": target["toRunId"], "kind": "handoff"}},
		{"a negative deadline", map[string]any{"toAgentVersionRef": target["toAgentVersionRef"], "toRunId": target["toRunId"], "kind": "handoff", "idempotencyKey": "k", "deadlineSeconds": -1}},
		{"a payload above the mailbox limit", map[string]any{
			"toAgentVersionRef": target["toAgentVersionRef"], "toRunId": target["toRunId"], "kind": "handoff", "idempotencyKey": "k",
			"payloadJson": map[string]any{"blob": strings.Repeat("a", store.IPCPayloadLimit+1)},
		}},
		{"an argument the tool does not define", map[string]any{
			"toAgentVersionRef": target["toAgentVersionRef"], "toRunId": target["toRunId"], "kind": "handoff", "idempotencyKey": "k",
			"tenantId": "tenant-other",
		}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
				"name": SystemIPCSend, "arguments": testCase.arguments,
			}))
			if rpcErr == nil {
				t.Fatal("a send that cannot be stored was accepted")
			}
		})
	}
}

func TestBrokerIPCReceiveDrainsWithoutAnAddress(t *testing.T) {
	mailbox := &fakeMailboxBroker{receiveOut: []MailboxMessage{{
		MessageID:           uuid.New().String(),
		FromAgentVersionRef: "manager@1.0.0",
		ToAgentVersionRef:   "agent@1",
		ToRunID:             uuid.New().String(),
		Kind:                "handoff",
		PayloadJSON:         json.RawMessage(`{"goal":"summarize"}`),
		CorrelationID:       "corr-1",
		SentAt:              time.Now().UTC(),
	}}}
	broker := newIPCBrokerForTest(t, brokerTestContext(), mailbox)

	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemIPCReceive, "arguments": map[string]any{"maxMessages": 5, "waitMillis": 1500},
	}))
	if rpcErr != nil {
		t.Fatalf("receive: %v", rpcErr)
	}
	if len(mailbox.receives) != 1 {
		t.Fatalf("broker saw %d drains, want 1", len(mailbox.receives))
	}
	if mailbox.receives[0].MaxMessages != 5 || mailbox.receives[0].WaitMillis != 1500 {
		t.Fatalf("drain bounds are %+v", mailbox.receives[0])
	}
	document := decodeToolJSON(t, result)
	messages, ok := document["messages"].([]any)
	if !ok || len(messages) != 1 {
		t.Fatalf("drain returned %v", document["messages"])
	}
	entry := messages[0].(map[string]any)
	if entry["messageId"] != mailbox.receiveOut[0].MessageID || entry["kind"] != "handoff" {
		t.Fatalf("drained message is %v", entry)
	}
	// Reading does not receipt, so the result has to say how a batch is retired.
	if document["acknowledgeWith"] != SystemIPCAck {
		t.Fatalf("drain does not name the acknowledging tool: %v", document)
	}

	if _, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemIPCReceive, "arguments": map[string]any{"maxMessages": -1},
	})); rpcErr == nil {
		t.Fatal("a negative batch bound was accepted")
	}
}

func TestBrokerIPCAcknowledgesOnlyWithMessageIDs(t *testing.T) {
	messageID := uuid.New().String()
	mailbox := &fakeMailboxBroker{ackReply: MailboxAckOutcome{
		Acknowledged:        []string{messageID},
		AlreadyAcknowledged: []string{uuid.New().String()},
	}}
	broker := newIPCBrokerForTest(t, brokerTestContext(), mailbox)

	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemIPCAck, "arguments": map[string]any{"messageIds": []string{messageID}},
	}))
	if rpcErr != nil {
		t.Fatalf("acknowledge: %v", rpcErr)
	}
	if len(mailbox.acks) != 1 || len(mailbox.acks[0].MessageIDs) != 1 || mailbox.acks[0].MessageIDs[0] != messageID {
		t.Fatalf("broker saw acknowledgements %+v", mailbox.acks)
	}
	document := decodeToolJSON(t, result)
	if len(document["acknowledged"].([]any)) != 1 {
		t.Fatalf("acknowledged bucket is %v", document["acknowledged"])
	}
	if len(document["alreadyAcknowledged"].([]any)) != 1 {
		t.Fatalf("already-acknowledged bucket is %v", document["alreadyAcknowledged"])
	}

	if _, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemIPCAck, "arguments": map[string]any{},
	})); rpcErr == nil {
		t.Fatal("an acknowledgement with no message ids was accepted")
	}
}

func TestBrokerIPCToolsDenyClosedWithoutAMailbox(t *testing.T) {
	broker := newIPCBrokerForTest(t, brokerTestContext(), nil)
	for _, name := range []string{SystemIPCSend, SystemIPCReceive, SystemIPCAck} {
		if _, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
			"name": name, "arguments": map[string]any{},
		})); rpcErr == nil {
			t.Fatalf("tool %q answered although no mailbox is configured", name)
		}
	}
}

func TestBrokerIPCFailuresAreToolOutcomes(t *testing.T) {
	// A broker failure is reported to the agent as a failed tool result, not as
	// a protocol error: the agent should see why, and keep its session.
	mailbox := &fakeMailboxBroker{receiveErr: context.DeadlineExceeded}
	broker := newIPCBrokerForTest(t, brokerTestContext(), mailbox)
	result, rpcErr := broker.CallTool(context.Background(), mustJSON(t, map[string]any{
		"name": SystemIPCReceive, "arguments": map[string]any{},
	}))
	if rpcErr != nil {
		t.Fatalf("a broker failure surfaced as a protocol error: %v", rpcErr)
	}
	document, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result is %#v", result)
	}
	if document["isError"] != true {
		t.Fatalf("result is not flagged as an error: %v", document)
	}
}
