package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CloudEdgeCore/Fenced/cmd/mtlsutil"
	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	ipcv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/ipc/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/grpcx"
	"github.com/CloudEdgeCore/Fenced/internal/platform/otel"
	runtimeadapter "github.com/CloudEdgeCore/Fenced/internal/runtime/adapter"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/reference"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	controlAddress := flag.String("control-address", "127.0.0.1:9090", "Runtime Protocol control endpoint")
	endpoint := flag.String("adapter-endpoint", "", "explicit Agent Runtime Interface endpoint (overrides runtime bindings and manifest entrypoints; plaintext HTTP must be loopback, remote endpoints must be HTTPS)")
	runtimeBindings := flag.String("runtime-bindings", "", "JSON runtime binding file mapping agent version refs to Runtime Interface endpoints (empty disables)")
	dedicatedEndpoint := flag.Bool("dedicated-endpoint", false, "acknowledge that -adapter-endpoint intentionally overrides runtime bindings (single-agent-version workers only)")
	allowPlaintextRemote := flag.Bool("allow-plaintext-remote-runtime", false, "development escape hatch: permit plaintext HTTP Runtime Interface endpoints on non-loopback hosts (never enable in production)")
	tenantID := flag.String("tenant", "", "tenant assigned to this worker")
	runtimeInstanceID := flag.String("runtime-instance-id", "", "assigned runtime instance ID")
	artifactRoot := flag.String("artifact-root", "", "content-addressed artifact directory")
	mcpListen := flag.String("mcp-listen", "", "optional loopback address for the sandbox Agent MCP endpoint (e.g. 127.0.0.1:9093)")
	gatewayAddress := flag.String("gateway-address", "", "Gateway Protocol address for brokered model/memory/tools (required by -mcp-listen)")
	pollInterval := flag.Duration("poll-interval", 250*time.Millisecond, "assignment polling interval")
	heartbeatTTL := flag.Duration("heartbeat-ttl", 30*time.Second, "requested runtime lease TTL")
	tlsCert := flag.String("tls-cert", "", "worker X.509-SVID certificate")
	tlsKey := flag.String("tls-key", "", "worker X.509-SVID private key")
	trustBundle := flag.String("trust-bundle", "", "SPIFFE trust bundle")
	devMode := flag.Bool("dev-mode", false, "allow plaintext Runtime Protocol for local development")
	spawnAddress := flag.String("spawn-address", "", "orchestrator WorkflowSpawnService address (dynamic step spawn; empty disables)")
	flag.Parse()
	if strings.TrimSpace(*tenantID) == "" || strings.TrimSpace(*runtimeInstanceID) == "" ||
		strings.TrimSpace(*artifactRoot) == "" ||
		*pollInterval <= 0 || *heartbeatTTL <= 0 {
		slog.Error("adapter tenant, runtime instance, artifact root and positive intervals are required")
		os.Exit(2)
	}
	// The explicit endpoint is optional when runtime bindings resolve every
	// assignment: deployments that bind agent version refs to endpoints no
	// longer pin a default endpoint to this worker.
	if strings.TrimSpace(*endpoint) == "" && strings.TrimSpace(*runtimeBindings) == "" {
		slog.Error("either -adapter-endpoint or -runtime-bindings is required")
		os.Exit(2)
	}
	if strings.TrimSpace(*endpoint) != "" {
		parsed, err := url.Parse(*endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
			slog.Error("adapter endpoint must be an absolute HTTP(S) URL")
			os.Exit(2)
		}
		// Transport security boundary (P0-02): plaintext HTTP is tolerated
		// only for a co-located loopback runtime; a remote Runtime Interface
		// must be HTTPS with server verification (optionally pinned by the
		// binding's TLS material).
		if parsed.Scheme == "http" {
			ip := net.ParseIP(parsed.Hostname())
			if parsed.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
				slog.Error("plaintext adapter endpoint must be co-located on loopback; remote runtimes require HTTPS", "endpoint", *endpoint)
				os.Exit(2)
			}
		}
	}
	if err := validateSandboxMCPListen(*mcpListen); err != nil {
		slog.Error("invalid -mcp-listen", "error", err)
		os.Exit(2)
	}
	bindings, err := runtimeadapter.LoadRuntimeBindingsFor(*runtimeBindings, runtimeadapter.EndpointPolicy{
		AllowPlaintextRemote: *allowPlaintextRemote,
	})
	if err != nil {
		slog.Error("load runtime bindings", "error", err)
		os.Exit(2)
	}
	// A worker with runtime bindings serves multiple agent versions; an
	// explicit endpoint would silently route every assignment to one
	// endpoint. Only an acknowledged dedicated worker may combine them.
	if err := runtimeadapter.ValidateEndpointOverride(strings.TrimSpace(*endpoint), bindings, *dedicatedEndpoint); err != nil {
		slog.Error("invalid endpoint configuration", "error", err)
		os.Exit(2)
	}
	tlsConfigured := *tlsCert != "" || *tlsKey != "" || *trustBundle != ""
	credentials, err := mtlsutil.ClientCredentials(tlsConfigured, *tlsCert, *tlsKey, *trustBundle)
	if err != nil {
		slog.Error("configure Runtime Protocol identity", "error", err)
		os.Exit(1)
	}
	if credentials == nil {
		if !*devMode {
			slog.Error("mTLS identity is required unless -dev-mode is explicit")
			os.Exit(2)
		}
		for name, address := range map[string]string{"control": *controlAddress, "gateway": *gatewayAddress, "spawn": *spawnAddress} {
			if strings.TrimSpace(address) != "" && !loopbackRPCAddress(address) {
				slog.Error("plaintext development endpoint must be loopback-only", "endpoint", name, "address", address)
				os.Exit(2)
			}
		}
		credentials = insecure.NewCredentials()
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
	connection, err := grpc.NewClient(*controlAddress,
		append([]grpc.DialOption{grpc.WithTransportCredentials(credentials)}, grpcx.ClientOptions()...)...)
	if err != nil {
		slog.Error("create Runtime Protocol client", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	artifacts, err := artifact.NewFilesystem(*artifactRoot, 64<<20)
	if err != nil {
		slog.Error("create artifact store", "error", err)
		os.Exit(1)
	}
	worker, err := runtimeadapter.NewWorker(runtimev1.NewRuntimeControlServiceClient(connection), artifacts,
		*endpoint, *tenantID, *runtimeInstanceID, *heartbeatTTL, nil)
	if err != nil {
		slog.Error("create adapter worker", "error", err)
		os.Exit(1)
	}
	if bindings != nil {
		worker.WithRuntimeBindings(bindings)
		slog.Info("runtime bindings loaded", "file", *runtimeBindings)
	}
	if strings.TrimSpace(*mcpListen) != "" {
		if strings.TrimSpace(*gatewayAddress) == "" {
			slog.Error("-mcp-listen requires -gateway-address so the sandbox Agent reaches the gateway services")
			os.Exit(2)
		}
		gatewayConnection, dialErr := grpc.NewClient(*gatewayAddress,
			append([]grpc.DialOption{grpc.WithTransportCredentials(credentials)}, grpcx.ClientOptions()...)...)
		if dialErr != nil {
			slog.Error("connect to Gateway Protocol", "error", dialErr)
			os.Exit(1)
		}
		defer gatewayConnection.Close()
		slot := mcp.NewExecutionRegistry()
		tools := mcp.NewToolAdapter(reference.NewGrpcToolInvoker(gatewayv1.NewToolGatewayServiceClient(gatewayConnection)), slot)
		var spawner mcp.WorkflowSpawner
		if strings.TrimSpace(*spawnAddress) != "" {
			spawnConnection, dialErr := grpc.NewClient(*spawnAddress,
				append([]grpc.DialOption{grpc.WithTransportCredentials(credentials)}, grpcx.ClientOptions()...)...)
			if dialErr != nil {
				slog.Error("connect to orchestrator spawn service", "error", dialErr)
				os.Exit(1)
			}
			defer spawnConnection.Close()
			spawner = runtimeadapter.NewGrpcWorkflowSpawner(runtimev1.NewWorkflowSpawnServiceClient(spawnConnection))
		}
		broker := mcp.NewBroker(tools,
			runtimeadapter.NewGrpcModelBroker(modelv1.NewModelInvocationServiceClient(gatewayConnection)),
			runtimeadapter.NewGrpcMemoryBroker(gatewayv1.NewMemoryGatewayServiceClient(gatewayConnection)),
			spawner,
			runtimeadapter.NewGrpcMailboxBroker(ipcv1.NewIPCGatewayServiceClient(gatewayConnection)), slot)
		worker.WithExecutionWindow(slot)
		mcpServer := mcp.NewServer("fenced-adapter", "v1.1.0", broker)
		listener, listenErr := net.Listen("tcp", *mcpListen)
		if listenErr != nil {
			slog.Error("listen for sandbox Agent MCP endpoint", "error", listenErr)
			os.Exit(1)
		}
		httpServer := &http.Server{
			Handler: mcpServer,
			// ReadTimeout bounds slow body reads (Slowloris); the MCP SSE
			// long-poll writes are bounded by the handler concurrency cap.
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 10 * time.Minute,
			IdleTimeout:  60 * time.Second,
		}
		go func() {
			if serveErr := httpServer.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
				slog.Error("sandbox Agent MCP endpoint", "error", serveErr)
			}
		}()
		defer func() { _ = httpServer.Close() }()
		slog.Info("sandbox Agent MCP endpoint listening", "address", *mcpListen)
	}
	idleDelay := *pollInterval
	const maxIdlePoll = 5 * time.Second
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		processed, err := worker.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("adapter runtime execution", "error", err)
		}
		if processed {
			slog.Info("adapter runtime completed assignment", "runtimeInstanceId", *runtimeInstanceID)
			idleDelay = *pollInterval
			timer.Reset(0)
			continue
		}
		if err != nil {
			idleDelay = *pollInterval
		} else if idleDelay < maxIdlePoll {
			idleDelay *= 2
			if idleDelay > maxIdlePoll {
				idleDelay = maxIdlePoll
			}
		}
		timer.Reset(idleDelay)
	}
}

func loopbackRPCAddress(address string) bool {
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

// validateSandboxMCPListen enforces the loopback-only boundary for the
// sandbox Agent MCP listener. The endpoint brokers model, tool, memory and
// spawn access for sandboxed agents, so until MCP gains its own
// SPIFFE/mTLS identity it must never bind a non-loopback address —
// regardless of dev or production mode and regardless of the configured
// control-plane mTLS.
func validateSandboxMCPListen(listen string) error {
	if strings.TrimSpace(listen) == "" {
		return nil
	}
	if !loopbackRPCAddress(listen) {
		return fmt.Errorf("sandbox MCP listener must be loopback-only (127.0.0.1, ::1 or localhost), got %q", listen)
	}
	return nil
}
