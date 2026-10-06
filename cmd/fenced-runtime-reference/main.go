package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CloudEdgeCore/Fenced/cmd/mtlsutil"
	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/grpcx"
	"github.com/CloudEdgeCore/Fenced/internal/platform/otel"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/reference"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	controlAddress := flag.String("control-address", "127.0.0.1:9090", "Runtime Protocol control endpoint")
	gatewayAddress := flag.String("gateway-address", "", "optional Tool Gateway endpoint for workload-spec tool scripts")
	modelGatewayAddress := flag.String("model-gateway-address", "", "optional Model Gateway endpoint for workload-spec model scripts")
	mcpListen := flag.String("mcp-listen", "", "optional loopback address for the sandbox Agent MCP endpoint (e.g. 127.0.0.1:9092)")
	tenantID := flag.String("tenant", "", "development tenant")
	runtimeInstanceID := flag.String("runtime-instance-id", "", "assigned runtime instance ID")
	artifactRoot := flag.String("artifact-root", "", "development content-addressed artifact directory")
	artifactGC := flag.Duration("artifact-gc-retention", 0, "delete artifacts older than this retention at startup (0 = disabled; O7)")
	pollInterval := flag.Duration("poll-interval", 250*time.Millisecond, "assignment polling interval")
	heartbeatTTL := flag.Duration("heartbeat-ttl", 30*time.Second, "requested runtime lease TTL")
	devMode := flag.Bool("dev-mode", false, "acknowledge deterministic non-sandboxed development provider")
	tlsCert := flag.String("tls-cert", "", "worker X.509-SVID certificate (PEM)")
	tlsKey := flag.String("tls-key", "", "worker X.509-SVID private key (PEM)")
	trustBundle := flag.String("trust-bundle", "", "SPIFFE trust bundle (PEM CA certificates)")
	flag.Parse()
	if strings.TrimSpace(*tenantID) == "" || strings.TrimSpace(*runtimeInstanceID) == "" || strings.TrimSpace(*artifactRoot) == "" ||
		*pollInterval <= 0 || *heartbeatTTL <= 0 || !*devMode {
		slog.Error("tenant, runtime instance, artifact root, positive intervals, and explicit -dev-mode are required")
		os.Exit(2)
	}
	tlsConfigured := *tlsCert != "" || *tlsKey != "" || *trustBundle != ""
	if !tlsConfigured && (!loopbackEndpoint(*controlAddress) ||
		(strings.TrimSpace(*gatewayAddress) != "" && !loopbackEndpoint(*gatewayAddress)) ||
		(strings.TrimSpace(*modelGatewayAddress) != "" && !loopbackEndpoint(*modelGatewayAddress))) {
		slog.Error("plaintext development RPC endpoints must be loopback-only")
		os.Exit(2)
	}
	if strings.TrimSpace(*mcpListen) != "" && !loopbackEndpoint(*mcpListen) {
		slog.Error("sandbox MCP listener must be loopback-only", "listen", *mcpListen)
		os.Exit(2)
	}
	controlCredentials, err := mtlsutil.ClientCredentials(tlsConfigured, *tlsCert, *tlsKey, *trustBundle)
	if err != nil {
		slog.Error("configure worker mTLS credentials", "error", err)
		os.Exit(1)
	}
	if controlCredentials == nil {
		controlCredentials = insecure.NewCredentials()
		slog.Warn("Runtime Protocol client has NO transport identity (plaintext dev mode)")
	} else {
		slog.Info("Runtime Protocol client authenticating with X.509-SVID", "instance", *runtimeInstanceID)
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
		append([]grpc.DialOption{grpc.WithTransportCredentials(controlCredentials)}, grpcx.ClientOptions()...)...)
	if err != nil {
		slog.Error("create Runtime Protocol client", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	artifactStore, err := artifact.NewFilesystem(*artifactRoot, 64<<20)
	if err != nil {
		slog.Error("create development artifact store", "error", err)
		os.Exit(1)
	}
	if *artifactGC > 0 {
		deleted, gcErr := artifactStore.GarbageCollect(ctx, *artifactGC, time.Now())
		if gcErr != nil {
			slog.Error("artifact garbage collection", "error", gcErr)
			os.Exit(1)
		}
		slog.Info("artifact garbage collection", "deleted", deleted, "retention", *artifactGC)
	}
	worker := reference.NewWorker(runtimev1.NewRuntimeControlServiceClient(connection), artifactStore,
		*tenantID, *runtimeInstanceID, *heartbeatTTL)
	var gatewayConnection *grpc.ClientConn
	if strings.TrimSpace(*gatewayAddress) != "" {
		gatewayConnection, err = grpc.NewClient(*gatewayAddress,
			append([]grpc.DialOption{grpc.WithTransportCredentials(controlCredentials)}, grpcx.ClientOptions()...)...)
		if err != nil {
			slog.Error("create Tool Gateway client", "error", err)
			os.Exit(1)
		}
		defer gatewayConnection.Close()
		worker.WithToolGateway(gatewayv1.NewToolGatewayServiceClient(gatewayConnection))
		slog.Info("reference runtime wired to Tool Gateway", "address", *gatewayAddress)
	}
	if strings.TrimSpace(*modelGatewayAddress) != "" {
		modelConnection, err := grpc.NewClient(*modelGatewayAddress,
			append([]grpc.DialOption{grpc.WithTransportCredentials(controlCredentials)}, grpcx.ClientOptions()...)...)
		if err != nil {
			slog.Error("create Model Gateway client", "error", err)
			os.Exit(1)
		}
		defer modelConnection.Close()
		worker.WithModelGateway(modelv1.NewModelGatewayServiceClient(modelConnection))
		slog.Info("reference runtime wired to Model Gateway", "address", *modelGatewayAddress)
	}
	if strings.TrimSpace(*mcpListen) != "" {
		if gatewayConnection == nil {
			slog.Error("-mcp-listen requires -gateway-address so the sandbox Agent can reach the Tool Gateway")
			os.Exit(2)
		}
		identitySlot := reference.NewIdentitySlot()
		adapter := mcp.NewToolAdapter(reference.NewGrpcToolInvoker(gatewayv1.NewToolGatewayServiceClient(gatewayConnection)), identitySlot)
		mcpServer := mcp.NewServer("fenced-runtime", "v1.0.0", adapter)
		listener, err := net.Listen("tcp", *mcpListen)
		if err != nil {
			slog.Error("listen for sandbox Agent MCP endpoint", "error", err)
			os.Exit(1)
		}
		httpServer := &http.Server{
			Handler:           mcpServer,
			ReadHeaderTimeout: 5 * time.Second,
			// ReadTimeout bounds slow body reads (Slowloris); the MCP SSE
			// responses are single frames, so no write deadline is needed
			// and IdleTimeout governs idle keep-alive connections.
			ReadTimeout:    15 * time.Second,
			IdleTimeout:    60 * time.Second,
			MaxHeaderBytes: 32 << 10,
		}
		go func() {
			if err := httpServer.Serve(listener); err != nil && ctx.Err() == nil {
				slog.Error("sandbox Agent MCP endpoint", "error", err)
			}
		}()
		worker.WithIdentitySlot(identitySlot)
		slog.Info("sandbox Agent MCP endpoint listening", "address", *mcpListen)
	}
	ticker := time.NewTicker(*pollInterval)
	defer ticker.Stop()
	for {
		processed, err := worker.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("reference runtime execution", "error", err)
		}
		if processed {
			slog.Info("reference runtime completed one assignment", "runtimeInstanceId", *runtimeInstanceID)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func loopbackEndpoint(address string) bool {
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
