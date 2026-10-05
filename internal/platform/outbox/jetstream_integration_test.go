//go:build integration

package outbox

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamPublisherPersistsAndDeduplicatesEvent(t *testing.T) {
	url := os.Getenv("FENCED_TEST_NATS_URL")
	if url == "" {
		t.Skip("FENCED_TEST_NATS_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connection, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("create JetStream: %v", err)
	}
	if err := EnsureStream(ctx, js, 1); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	stream, err := js.Stream(ctx, StreamName)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	if err := stream.Purge(ctx); err != nil {
		t.Fatalf("purge test stream: %v", err)
	}
	event := store.OutboxEvent{
		ID: uuid.New(), TenantID: "tenant-a", AggregateType: "Task", AggregateID: uuid.New(),
		AggregateVersion: 1, EventType: "TaskQueued", Payload: json.RawMessage(`{"taskId":"test"}`),
		OccurredAt: time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC),
	}
	publisher := NewJetStreamPublisher(js)
	if err := publisher.Publish(ctx, event); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if err := publisher.Publish(ctx, event); err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream message count = %d, want 1", info.State.Msgs)
	}
}
