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
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/grpcx"
	"github.com/CloudEdgeCore/Fenced/internal/platform/otel"
	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	runtimecontrol "github.com/CloudEdgeCore/Fenced/internal/runtime/control"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (or DATABASE_URL)")
	listenAddress := flag.String("listen", "127.0.0.1:9090", "gRPC listen address")
	devTenant := flag.String("dev-tenant", "", "fixed development tenant")
	maxLeaseTTL := flag.Duration("max-lease-ttl", 2*time.Minute, "maximum runtime-requested lease TTL")
	devMode := flag.Bool("dev-mode", false, "acknowledge plaintext loopback-only development transport")
	tlsCert := flag.String("tls-cert", "", "control plane X.509-SVID certificate (PEM)")
	tlsKey := flag.String("tls-key", "", "control plane X.509-SVID private key (PEM)")
	trustBundle := flag.String("trust-bundle", "", "SPIFFE trust bundle (PEM CA certificates)")
	spiffePattern := flag.String("spiffe-pattern", "spiffe://fenced.dev/ns/*/worker/*", "SPIFFE ID pattern authorized to call the Runtime Protocol")
	flag.Parse()

	if *databaseURL == "" || *maxLeaseTTL <= 0 {
		slog.Error("database URL and positive max lease TTL are required")
		os.Exit(2)
	}
	tlsConfigured := *tlsCert != "" || *tlsKey != "" || *trustBundle != ""
	if tlsConfigured && (*tlsCert == "" || *tlsKey == "" || *trustBundle == "") {
		slog.Error("-tls-cert, -tls-key and -trust-bundle must be provided together for the mTLS boundary")
		os.Exit(2)
	}
	if *devMode {
		if strings.TrimSpace(*devTenant) == "" || !loopback(*listenAddress) {
			slog.Error("development mode requires a fixed tenant and loopback listener")
			os.Exit(2)
		}
	} else if strings.TrimSpace(*devTenant) != "" || !tlsConfigured {
		slog.Error("production mode requires SPIFFE mTLS and forbids a fixed development tenant")
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
	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		slog.Error("listen for Runtime Protocol", "error", err)
		os.Exit(1)
	}
	serverOptions := append([]grpc.ServerOption{}, grpcx.ServerOptions()...)
	var serviceOptions []runtimecontrol.Option
	if tlsConfigured {
		serverSVID, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			slog.Error("load control plane SVID", "error", err)
			os.Exit(1)
		}
		bundlePEM, err := os.ReadFile(*trustBundle)
		if err != nil {
			slog.Error("read trust bundle", "error", err)
			os.Exit(1)
		}
		pool, err := spiffe.TrustBundlePool([][]byte{bundlePEM})
		if err != nil {
			slog.Error("parse trust bundle", "error", err)
			os.Exit(1)
		}
		pattern, err := spiffe.ParsePattern(*spiffePattern)
		if err != nil {
			slog.Error("parse SPIFFE pattern", "error", err)
			os.Exit(1)
		}
		serverOptions = append(serverOptions, grpcx.ServerMTLSOptions(serverSVID, pool)...)
		serverOptions = append(serverOptions, grpcx.WithSpiffeIdentity(pattern))
		serviceOptions = append(serviceOptions, runtimecontrol.WithSpiffeClaimBinding(pattern.TrustDomain))
		slog.Info("Runtime Protocol identity boundary active (mTLS X.509-SVID)", "pattern", pattern.String())
	} else {
		slog.Warn("Runtime Protocol has NO transport identity boundary: workers connect in plaintext (dev mode only)")
	}
	server := grpc.NewServer(serverOptions...)
	allowedTenant := *devTenant
	if !*devMode {
		allowedTenant = "*"
	}
	runtimeService := runtimecontrol.NewService(postgresstore.New(pool), allowedTenant, *maxLeaseTTL, serviceOptions...)
	runtimev1.RegisterRuntimeControlServiceServer(server, runtimeService)
	if err := grpcx.RegisterLegacyServiceAlias(server, runtimev1.RuntimeControlService_ServiceDesc, runtimeService,
		"fenced.runtime.v1alpha1.RuntimeControlService"); err != nil {
		slog.Error("register legacy Runtime Protocol alias", "error", err)
		os.Exit(1)
	}
	healthServer := health.NewServer()
	healthServer.SetServingStatus("fenced.runtime.v1.RuntimeControlService", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("fenced.runtime.v1alpha1.RuntimeControlService", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	go func() {
		<-ctx.Done()
		healthServer.Shutdown()
		done := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			server.Stop()
		}
	}()
	slog.Info("Agent OS Runtime Protocol listening", "address", *listenAddress, "devMode", *devMode, "tenant", allowedTenant)
	if err := server.Serve(listener); err != nil && ctx.Err() == nil {
		slog.Error("serve Runtime Protocol", "error", err)
		os.Exit(1)
	}
}

func loopback(address string) bool {
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
