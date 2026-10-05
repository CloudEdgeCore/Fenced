package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/otel"
	"github.com/CloudEdgeCore/Fenced/internal/platform/outbox"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (or DATABASE_URL)")
	natsURL := flag.String("nats-url", nats.DefaultURL, "NATS server URL")
	dispatcherID := flag.String("dispatcher-id", "", "stable unique dispatcher instance ID")
	batch := flag.Int("batch", 100, "maximum events claimed per dispatch")
	replicas := flag.Int("stream-replicas", 1, "JetStream replica count")
	flag.Parse()
	if *databaseURL == "" || *dispatcherID == "" {
		slog.Error("database URL and dispatcher ID are required")
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
	natsConnection, err := nats.Connect(*natsURL, nats.Name("fenced-outbox-"+*dispatcherID), nats.Timeout(5*time.Second))
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
	dispatcher := outbox.NewDispatcher(postgresstore.New(pool), outbox.NewJetStreamPublisher(js), *dispatcherID, *batch, 30*time.Second)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		published, err := dispatcher.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("dispatch outbox batch", "error", err)
		}
		if published > 0 {
			slog.Info("published outbox events", "count", published)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
