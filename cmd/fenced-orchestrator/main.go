// Command fenced-orchestrator runs the durable workflow orchestrator: it
// decides which workflow steps execute when and creates ordinary Tasks for
// them. Scheduling (where a Task runs) stays with the scheduler. The
// reconcile loop is stateless and durable — restarts and concurrent
// instances converge without double dispatch.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workflow"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/grpcx"
	"github.com/CloudEdgeCore/Fenced/internal/platform/otel"
	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (or DATABASE_URL)")
	orchestratorID := flag.String("orchestrator-id", "", "unique orchestrator instance identity")
	artifactRoot := flag.String("artifact-root", "", "content-addressed artifact directory (task result reading)")
	interval := flag.Duration("interval", 250*time.Millisecond, "reconcile interval")
	batch := flag.Int("batch", 100, "workflows per reconcile batch (1..100)")
	parallel := flag.Int("parallel", 4, "concurrent workflows per batch")
	claimLease := flag.Duration("claim-lease", 0, "distributed work-claim lease (0 = single-instance reconcile-everything mode)")
	spawnListen := flag.String("listen", "", "gRPC listen address of the WorkflowSpawnService (dynamic spawn; empty disables)")
	devMode := flag.Bool("dev-mode", false, "allow plaintext WorkflowSpawnService on a loopback listener")
	tlsCert := flag.String("tls-cert", "", "orchestrator X.509-SVID certificate (PEM)")
	tlsKey := flag.String("tls-key", "", "orchestrator X.509-SVID private key (PEM)")
	trustBundle := flag.String("trust-bundle", "", "SPIFFE trust bundle (PEM CA certificates)")
	spiffePattern := flag.String("spiffe-pattern", "spiffe://fenced.dev/ns/*/worker/*", "worker SPIFFE identities authorized to call the spawn service")
	flag.Parse()
	if strings.TrimSpace(*databaseURL) == "" || strings.TrimSpace(*orchestratorID) == "" ||
		strings.TrimSpace(*artifactRoot) == "" || *interval <= 0 || *batch <= 0 || *batch > 100 {
		slog.Error("database URL, orchestrator id, artifact root, and positive bounds are required (batch 1..100)")
		os.Exit(2)
	}
	tlsConfigured := *tlsCert != "" || *tlsKey != "" || *trustBundle != ""
	if tlsConfigured && (*tlsCert == "" || *tlsKey == "" || *trustBundle == "") {
		slog.Error("-tls-cert, -tls-key and -trust-bundle must be provided together")
		os.Exit(2)
	}
	if *spawnListen != "" {
		if *devMode {
			if !loopbackAddress(*spawnListen) {
				slog.Error("development spawn transport must listen on loopback")
				os.Exit(2)
			}
		} else if !tlsConfigured {
			slog.Error("production WorkflowSpawnService requires SPIFFE mTLS; use -dev-mode only for loopback development")
			os.Exit(2)
		}
	} else if tlsConfigured {
		slog.Error("spawn TLS flags require -listen")
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

	config, err := pgxpool.ParseConfig(*databaseURL)
	if err != nil {
		slog.Error("parse database URL", "error", err)
		os.Exit(2)
	}
	config.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		slog.Error("open PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		slog.Error("ping PostgreSQL", "error", err)
		os.Exit(1)
	}

	artifacts, err := artifact.NewFilesystem(*artifactRoot, 64<<20)
	if err != nil {
		slog.Error("create artifact store", "error", err)
		os.Exit(1)
	}
	repository := postgres.New(pool)
	controller := workflow.NewController(repository, repository, artifacts, *orchestratorID, *batch).
		WithParallelism(*parallel).
		WithClaiming(*claimLease, 0)

	if *spawnListen != "" {
		listener, err := net.Listen("tcp", *spawnListen)
		if err != nil {
			slog.Error("listen for spawn service", "address", *spawnListen, "error", err)
			os.Exit(1)
		}
		serverOptions := append([]grpc.ServerOption{}, grpcx.ServerOptions()...)
		serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20))
		spawnTrustDomain := ""
		if tlsConfigured {
			serverSVID, loadErr := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
			if loadErr != nil {
				slog.Error("load orchestrator SVID", "error", loadErr)
				os.Exit(1)
			}
			bundlePEM, readErr := os.ReadFile(*trustBundle)
			if readErr != nil {
				slog.Error("read trust bundle", "error", readErr)
				os.Exit(1)
			}
			trustPool, poolErr := spiffe.TrustBundlePool([][]byte{bundlePEM})
			if poolErr != nil {
				slog.Error("parse trust bundle", "error", poolErr)
				os.Exit(1)
			}
			pattern, patternErr := spiffe.ParsePattern(*spiffePattern)
			if patternErr != nil {
				slog.Error("parse SPIFFE pattern", "error", patternErr)
				os.Exit(1)
			}
			serverOptions = append(serverOptions, grpcx.ServerMTLSOptions(serverSVID, trustPool)...)
			serverOptions = append(serverOptions, grpcx.WithSpiffeIdentity(pattern))
			spawnTrustDomain = pattern.TrustDomain
			slog.Info("workflow spawn identity boundary active", "pattern", pattern.String())
		}
		server := grpc.NewServer(serverOptions...)
		runtimev1.RegisterWorkflowSpawnServiceServer(server, &spawnService{
			workflows: repository, runtime: repository, spiffeTrustDomain: spawnTrustDomain,
		})
		go func() {
			<-ctx.Done()
			server.Stop()
		}()
		go func() {
			if err := server.Serve(listener); err != nil {
				slog.Error("spawn service stopped", "error", err)
			}
		}()
		slog.Info("workflow spawn service listening", "address", *spawnListen)
	}

	slog.Info("workflow orchestrator running",
		"orchestratorId", *orchestratorID, "interval", interval.String(), "batch", *batch,
		"claimLease", claimLease.String())
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		processed, err := controller.Reconcile(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("workflow reconcile", "error", err)
		}
		if processed > 0 {
			slog.Debug("workflow reconcile advanced", "workflows", processed)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
