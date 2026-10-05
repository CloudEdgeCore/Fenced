// Command fenced-projector consumes canonical store events from JetStream
// and applies them to OpenSearch search projections. Memory upserts index
// documents and memory tombstones delete them, propagating deletions
// (ADR-013); audit records are indexed append-only with their hash chain
// (ADR-014). Application is idempotent by resource version / seq, so
// replays and redeliveries are safe.
//
// Usage:
//
//	fenced-projector -database-url ... -nats-url nats://127.0.0.1:54222 \
//	    -opensearch-addr http://127.0.0.1:39200
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/opensearch"
	"github.com/CloudEdgeCore/Fenced/internal/platform/otel"
	"github.com/CloudEdgeCore/Fenced/internal/platform/outbox"
	"github.com/CloudEdgeCore/Fenced/internal/platform/projector"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type projectionConfig struct {
	name       string
	filter     string
	index      string
	mapping    []byte
	consumerID string
	// apply builds the projector's apply function for the repository.
	apply func(*postgresstore.Store, *opensearch.Client) func(context.Context, store.OutboxEvent) error
}

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (or DATABASE_URL)")
	natsURL := flag.String("nats-url", nats.DefaultURL, "NATS server URL")
	opensearchAddr := flag.String("opensearch-addr", "http://127.0.0.1:39200", "OpenSearch cluster address")
	opensearchUser := flag.String("opensearch-user", "", "OpenSearch basic auth user (security plugin)")
	opensearchPassword := flag.String("opensearch-password", "", "OpenSearch basic auth password (security plugin)")
	opensearchIndex := flag.String("opensearch-index", "fenced-memory", "OpenSearch index for projected memory records")
	auditIndex := flag.String("audit-index", "fenced-audit", "OpenSearch index for projected audit records")
	projections := flag.String("projection", "memory,audit", "comma-separated projections to run: memory, audit")
	replicas := flag.Int("stream-replicas", 1, "JetStream replica count")
	flag.Parse()
	if *databaseURL == "" {
		slog.Error("database URL is required")
		os.Exit(2)
	}
	enabled := map[string]bool{}
	for _, name := range strings.Split(*projections, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		switch name {
		case "memory", "audit":
			enabled[name] = true
		default:
			slog.Error("unknown projection", "projection", name)
			os.Exit(2)
		}
	}
	if len(enabled) == 0 {
		slog.Error("at least one projection is required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTelemetry, err := otel.Init(ctx)
	if err != nil {
		slog.Error("initialize OpenTelemetry", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = shutdownTelemetry(shutdownCtx)
	}()
	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		slog.Error("create PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		slog.Error("connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	natsConnection, err := nats.Connect(*natsURL, nats.Name("fenced-projector"), nats.Timeout(5*time.Second))
	if err != nil {
		slog.Error("connect to NATS", "error", err)
		os.Exit(1)
	}
	defer natsConnection.Close()
	js, err := jetstream.New(natsConnection)
	if err != nil {
		slog.Error("create JetStream context", "error", err)
		os.Exit(1)
	}
	if err := outbox.EnsureStream(ctx, js, *replicas); err != nil {
		slog.Error("configure event stream", "error", err)
		os.Exit(1)
	}

	var searchOptions []opensearch.Option
	if *opensearchUser != "" {
		searchOptions = append(searchOptions, opensearch.WithCredentials(*opensearchUser, *opensearchPassword))
	}
	repository := postgresstore.New(pool)

	configs := []projectionConfig{}
	if enabled["memory"] {
		configs = append(configs, projectionConfig{
			name: "memory", filter: outbox.SubjectPrefix + ".memory.>",
			index: *opensearchIndex, mapping: projector.MemoryMapping, consumerID: "fenced-projector-memory",
			apply: func(repository *postgresstore.Store, search *opensearch.Client) func(context.Context, store.OutboxEvent) error {
				return projector.NewMemoryProjector(repository, search).Apply
			},
		})
	}
	if enabled["audit"] {
		configs = append(configs, projectionConfig{
			name: "audit", filter: outbox.SubjectPrefix + ".audit.>",
			index: *auditIndex, mapping: projector.AuditMapping, consumerID: "fenced-projector-audit",
			apply: func(repository *postgresstore.Store, search *opensearch.Client) func(context.Context, store.OutboxEvent) error {
				return projector.NewAuditProjector(repository, search).Apply
			},
		})
	}

	var consumers []jetstream.ConsumeContext
	defer func() {
		for _, consumer := range consumers {
			consumer.Stop()
		}
	}()
	for _, config := range configs {
		consumer, err := startProjection(ctx, js, repository, config, *opensearchAddr, searchOptions)
		if err != nil {
			slog.Error("start projection", "projection", config.name, "error", err)
			os.Exit(1)
		}
		consumers = append(consumers, consumer)
		slog.Info("Agent OS projection listening", "projection", config.name, "index", config.index, "filter", config.filter, "consumer", config.consumerID)
	}
	<-ctx.Done()
}

func startProjection(ctx context.Context, js jetstream.JetStream, repository *postgresstore.Store, config projectionConfig, opensearchAddr string, searchOptions []opensearch.Option) (jetstream.ConsumeContext, error) {
	searchClient, err := opensearch.New(opensearchAddr, config.index, searchOptions...)
	if err != nil {
		return nil, err
	}
	pingCtx, cancelPing := context.WithTimeout(ctx, 15*time.Second)
	if err := searchClient.Ping(pingCtx); err != nil {
		cancelPing()
		return nil, err
	}
	cancelPing()
	indexCtx, cancelIndex := context.WithTimeout(ctx, 15*time.Second)
	defer cancelIndex()
	if err := searchClient.EnsureIndex(indexCtx, config.mapping); err != nil {
		return nil, err
	}
	apply := config.apply(repository, searchClient)

	consumer, err := js.CreateOrUpdateConsumer(ctx, outbox.StreamName, jetstream.ConsumerConfig{
		Durable:       config.consumerID,
		Name:          config.consumerID,
		FilterSubject: config.filter,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 256,
	})
	if err != nil {
		return nil, err
	}
	consumeContext, err := consumer.Consume(func(message jetstream.Msg) {
		if err := applyMessage(ctx, apply, message); err != nil {
			slog.Error("projection apply failed; retrying", "projection", config.name, "error", err, "subject", message.Subject())
			_ = message.NakWithDelay(2 * time.Second)
			return
		}
		_ = message.Ack()
	})
	if err != nil {
		return nil, err
	}
	return consumeContext, nil
}

func applyMessage(ctx context.Context, apply func(context.Context, store.OutboxEvent) error, message jetstream.Msg) error {
	envelope, err := decodeEnvelope(message.Data())
	if err != nil {
		// A malformed envelope can never succeed; drop it instead of
		// redelivering forever.
		slog.Error("dropping malformed projection envelope", "error", err, "subject", message.Subject())
		_ = message.Ack()
		return nil
	}
	event := store.OutboxEvent{
		ID: envelope.EventID, TenantID: envelope.TenantID,
		AggregateType: envelope.AggregateType, AggregateID: envelope.AggregateID,
		AggregateVersion: envelope.AggregateVersion, EventType: envelope.EventType,
		Payload: envelope.Payload, OccurredAt: envelope.OccurredAt,
	}
	applyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return apply(applyCtx, event)
}

type eventEnvelope struct {
	EventID          uuid.UUID       `json:"eventId"`
	TenantID         string          `json:"tenantId"`
	AggregateType    string          `json:"aggregateType"`
	AggregateID      uuid.UUID       `json:"aggregateId"`
	AggregateVersion int64           `json:"aggregateVersion"`
	EventType        string          `json:"eventType"`
	OccurredAt       time.Time       `json:"occurredAt"`
	Payload          json.RawMessage `json:"payload"`
}

func decodeEnvelope(data []byte) (eventEnvelope, error) {
	var envelope eventEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return envelope, err
	}
	if envelope.EventID == uuid.Nil || envelope.TenantID == "" || envelope.AggregateID == uuid.Nil || len(envelope.Payload) == 0 {
		return envelope, errors.New("projection envelope is incomplete")
	}
	return envelope, nil
}
