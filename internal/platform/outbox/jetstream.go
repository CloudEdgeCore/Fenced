package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamName    = "FENCED_EVENTS"
	SubjectPrefix = "fenced.events"
)

var subjectToken = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)

type JetStreamPublisher struct {
	jetStream jetstream.JetStream
}

func NewJetStreamPublisher(js jetstream.JetStream) *JetStreamPublisher {
	return &JetStreamPublisher{jetStream: js}
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, replicas int) error {
	if replicas < 1 || replicas > 5 {
		return fmt.Errorf("JetStream replicas must be between 1 and 5")
	}
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: StreamName, Description: "Agent OS durable domain event projection",
		Subjects: []string{SubjectPrefix + ".>"}, Retention: jetstream.LimitsPolicy,
		// The cap must stay modest: JetStream file stores refuse to create a
		// stream whose MaxBytes exceeds the currently available disk.
		MaxAge: 7 * 24 * time.Hour, MaxBytes: 2 << 30, MaxMsgSize: 1 << 20,
		Storage: jetstream.FileStorage, Replicas: replicas, Duplicates: 10 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("ensure Agent OS event stream: %w", err)
	}
	return nil
}

func (p *JetStreamPublisher) Publish(ctx context.Context, event store.OutboxEvent) error {
	subject, err := eventSubject(event.AggregateType, event.EventType)
	if err != nil {
		return err
	}
	envelope := struct {
		SchemaVersion    string          `json:"schemaVersion"`
		EventID          string          `json:"eventId"`
		TenantID         string          `json:"tenantId"`
		AggregateType    string          `json:"aggregateType"`
		AggregateID      string          `json:"aggregateId"`
		AggregateVersion int64           `json:"aggregateVersion"`
		EventType        string          `json:"eventType"`
		OccurredAt       time.Time       `json:"occurredAt"`
		Payload          json.RawMessage `json:"payload"`
	}{
		SchemaVersion: "fenced.events/v1alpha1", EventID: event.ID.String(), TenantID: event.TenantID,
		AggregateType: event.AggregateType, AggregateID: event.AggregateID.String(),
		AggregateVersion: event.AggregateVersion, EventType: event.EventType,
		OccurredAt: event.OccurredAt.UTC(), Payload: event.Payload,
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode event %s: %w", event.ID, err)
	}
	if len(encoded) > 1<<20 {
		return fmt.Errorf("event %s exceeds 1 MiB JetStream contract", event.ID)
	}
	if _, err := p.jetStream.Publish(ctx, subject, encoded, jetstream.WithMsgID(event.ID.String())); err != nil {
		return fmt.Errorf("publish event %s: %w", event.ID, err)
	}
	return nil
}

func eventSubject(aggregateType, eventType string) (string, error) {
	if !subjectToken.MatchString(aggregateType) || !subjectToken.MatchString(eventType) {
		return "", fmt.Errorf("event subject tokens are invalid")
	}
	return SubjectPrefix + "." + strings.ToLower(aggregateType) + "." + strings.ToLower(eventType), nil
}
