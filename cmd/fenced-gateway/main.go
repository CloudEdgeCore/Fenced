package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	ipcv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/ipc/v1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	"github.com/CloudEdgeCore/Fenced/internal/gateway"
	"github.com/CloudEdgeCore/Fenced/internal/gateway/bao"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/capability"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/memory"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
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
	listen := flag.String("listen", "127.0.0.1:9091", "loopback-only gRPC listen address")
	tenantPoliciesFile := flag.String("tenant-policies", "", "JSON tenant policy data (absent tenants are denied by default)")
	tenantID := flag.String("tenant", "", "fixed development tenant")
	seedDevTools := flag.Bool("seed-dev-tools", false, "idempotently register the development echo.dev tool")
	seedDevModel := flag.String("seed-dev-model", "", "idempotently register a development model descriptor as provider/model (requires -dev-mode; prices default to 0)")
	seedDevModelInputPrice := flag.Float64("seed-dev-model-input-price", 0, "input price per million tokens for -seed-dev-model")
	seedDevModelOutputPrice := flag.Float64("seed-dev-model-output-price", 0, "output price per million tokens for -seed-dev-model")
	devMode := flag.Bool("dev-mode", false, "acknowledge the development executor and fixed tenant")
	toolEndpointsFile := flag.String("tool-endpoints", "", "production JSON mapping of immutable tool versions to HTTPS endpoints")
	embeddingEndpoint := flag.String("embedding-endpoint", "", "production HTTPS embedding service")
	embeddingToken := flag.String("embedding-token", os.Getenv("FENCED_EMBEDDING_TOKEN"), "embedding service bearer token")
	tlsCert := flag.String("tls-cert", "", "gateway X.509-SVID certificate (PEM)")
	tlsKey := flag.String("tls-key", "", "gateway X.509-SVID private key (PEM)")
	trustBundle := flag.String("trust-bundle", "", "SPIFFE trust bundle (PEM CA certificates)")
	spiffePattern := flag.String("spiffe-pattern", "spiffe://fenced.dev/ns/*/worker/*", "worker SPIFFE identities authorized to call the gateway")
	baoAddr := flag.String("bao-addr", "", "OpenBao address for the Secret Broker (e.g. http://127.0.0.1:58200)")
	baoToken := flag.String("bao-token", os.Getenv("BAO_TOKEN"), "OpenBao token (or BAO_TOKEN) with read access to Fenced secrets")
	baoMount := flag.String("bao-mount", bao.DefaultMount, "OpenBao KV v2 mount (default secret)")
	baoNamespace := flag.String("bao-namespace", "", "optional OpenBao namespace header")
	baoCacheTTL := flag.Duration("bao-cache-ttl", 30*time.Second, "issued secret handle cache TTL")
	baoDynamicRole := flag.String("bao-dynamic-role", "", "OpenBao database role for dynamic credentials (database/creds/<role>); takes precedence over KV reads")
	modelProvidersFile := flag.String("model-providers", "", "JSON OpenAI-compatible provider endpoints ({\"providers\":[{\"name\",\"baseUrl\",\"apiKeyEnv\",...}]}); absent providers fail closed")
	flag.Parse()
	if *databaseURL == "" {
		slog.Error("database URL is required")
		os.Exit(2)
	}
	tlsConfigured := *tlsCert != "" || *tlsKey != "" || *trustBundle != ""
	if tlsConfigured && (*tlsCert == "" || *tlsKey == "" || *trustBundle == "") {
		slog.Error("-tls-cert, -tls-key and -trust-bundle must be provided together")
		os.Exit(2)
	}
	if *devMode {
		if strings.TrimSpace(*tenantID) == "" || !isLoopback(*listen) {
			slog.Error("development mode requires a fixed tenant and loopback listener")
			os.Exit(2)
		}
	} else if strings.TrimSpace(*tenantID) != "" || !tlsConfigured || strings.TrimSpace(*toolEndpointsFile) == "" ||
		strings.TrimSpace(*embeddingEndpoint) == "" || !isHTTPSURL(*baoAddr) || strings.TrimSpace(*baoToken) == "" {
		slog.Error("production mode requires SPIFFE mTLS, HTTPS tool endpoints, OpenBao, and no fixed tenant")
		os.Exit(2)
	}
	tenantPolicies := policy.TenantPolicies{}
	if *tenantPoliciesFile != "" {
		var err error
		tenantPolicies, err = loadTenantPolicies(*tenantPoliciesFile)
		if err != nil {
			slog.Error("load tenant policies", "error", err)
			os.Exit(2)
		}
	}
	policyEngine, err := policy.New(tenantPolicies)
	if err != nil {
		slog.Error("prepare policy engine", "error", err)
		os.Exit(1)
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
	repository := postgresstore.New(pool)

	if *seedDevTools && !*devMode {
		slog.Error("-seed-dev-tools is forbidden in production mode")
		os.Exit(2)
	}
	if *seedDevModel != "" {
		if !*devMode {
			slog.Error("-seed-dev-model is forbidden in production mode")
			os.Exit(2)
		}
		providerName, modelName, found := strings.Cut(*seedDevModel, "/")
		if !found || kernelstore.ValidateModelRef(*seedDevModel) != nil {
			slog.Error("-seed-dev-model must be provider/model", "ref", *seedDevModel)
			os.Exit(2)
		}
		inputPrice, inputPriceErr := money.FromUSD(*seedDevModelInputPrice)
		outputPrice, outputPriceErr := money.FromUSD(*seedDevModelOutputPrice)
		if inputPriceErr != nil || outputPriceErr != nil {
			slog.Error("development model prices must be non-negative six-decimal USD values")
			os.Exit(2)
		}
		seedModel, seedModelErr := repository.RegisterModelDescriptor(ctx, kernelstore.RegisterModelDescriptorInput{
			TenantID: *tenantID, Provider: providerName, ModelName: modelName, SupportsStreaming: true,
			InputPriceMicroUSDPerMillion: inputPrice, OutputPriceMicroUSDPerMillion: outputPrice,
			PriceRevision: "dev",
		})
		if seedModelErr != nil {
			slog.Error("seed development model descriptor", "error", seedModelErr)
			os.Exit(1)
		}
		slog.Info("development model registered", "model", seedModel.Ref(), "streaming", seedModel.SupportsStreaming)
	}
	if *seedDevTools {
		seed, seedErr := repository.RegisterToolDescriptor(ctx, kernelstore.RegisterToolDescriptorInput{
			TenantID: *tenantID, Name: "echo.dev", Version: "1.0.0", SideEffectRisk: kernelstore.ToolRiskNone,
			Actions: []string{"echo"}, ResourcePatterns: []string{"*"},
			ParamsSchema: []byte(`{"type":"object","additionalProperties":true}`),
		})
		if seedErr != nil {
			slog.Error("seed development tool", "error", seedErr)
			os.Exit(1)
		}
		slog.Info("development tool registered", "tool", seed.Name+"@"+seed.Version)
	}

	var executor tool.ToolExecutor
	if *devMode {
		executor = &gateway.DevExecutor{MaxOutputBytes: 1 << 20}
	} else {
		endpoints, loadErr := loadToolEndpoints(*toolEndpointsFile)
		if loadErr != nil {
			slog.Error("load production tool endpoints", "error", loadErr)
			os.Exit(2)
		}
		executor, err = gateway.NewWebhookExecutor(endpoints, nil)
		if err != nil {
			slog.Error("configure production tool executor", "error", err)
			os.Exit(2)
		}
	}
	var secrets tool.SecretBroker
	if strings.TrimSpace(*baoAddr) != "" {
		if strings.TrimSpace(*baoToken) == "" {
			slog.Error("-bao-addr requires -bao-token")
			os.Exit(2)
		}
		var options []bao.Option
		if *baoMount != bao.DefaultMount {
			options = append(options, bao.WithMount(*baoMount))
		}
		if strings.TrimSpace(*baoNamespace) != "" {
			options = append(options, bao.WithNamespace(*baoNamespace))
		}
		if *baoCacheTTL > 0 {
			options = append(options, bao.WithCacheTTL(*baoCacheTTL))
		}
		if strings.TrimSpace(*baoDynamicRole) != "" {
			broker, err := bao.NewDynamicBroker(*baoAddr, *baoToken, *baoDynamicRole, options...)
			if err != nil {
				slog.Error("configure OpenBao dynamic Secret Broker", "error", err)
				os.Exit(1)
			}
			secrets = broker
			// The janitor keeps leases alive while the gateway runs and
			// revokes them on shutdown; credentials never outlive the
			// gateway process.
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						revokeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
						defer cancel()
						if err := broker.Close(revokeCtx); err != nil {
							slog.Error("revoke OpenBao leases on shutdown", "error", err)
						}
						return
					case <-ticker.C:
						janitorCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
						if err := broker.Janitor(janitorCtx); err != nil {
							slog.Error("OpenBao lease janitor", "error", err)
						}
						cancel()
					}
				}
			}()
			slog.Info("OpenBao dynamic Secret Broker active", "address", *baoAddr, "role", *baoDynamicRole, "cacheTtl", *baoCacheTTL)
		} else {
			broker, err := bao.NewBroker(*baoAddr, *baoToken, options...)
			if err != nil {
				slog.Error("configure OpenBao Secret Broker", "error", err)
				os.Exit(1)
			}
			pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
			if err := broker.Ping(pingCtx); err != nil {
				cancelPing()
				slog.Error("OpenBao Secret Broker is not reachable", "error", err)
				os.Exit(1)
			}
			cancelPing()
			secrets = broker
			slog.Info("OpenBao Secret Broker active", "address", *baoAddr, "mount", *baoMount, "cacheTtl", *baoCacheTTL)
		}
	} else {
		if !*devMode {
			slog.Error("production mode cannot use the development Secret Broker")
			os.Exit(2)
		}
		secrets = &gateway.DevSecretBroker{}
		slog.Warn("Secret Broker is the development stub: no real credentials are issued")
	}
	decisionGateway := tool.NewGateway(policyEngine, repository, repository, repository, executor, secrets)
	modelGateway := model.NewGateway(policyEngine, repository, repository, repository)
	modelProviders, err := provider.LoadRegistryFile(*modelProvidersFile)
	if err != nil {
		slog.Error("load model providers", "error", err)
		os.Exit(2)
	}
	modelProviders.UseCircuitBreaker(repository)
	if names := modelProviders.Names(); len(names) > 0 {
		slog.Info("model provider execution layer active", "providers", names)
	} else {
		slog.Warn("no model providers configured: model invocations fail closed (governance only)")
	}
	modelInvoker := model.NewInvoker(modelGateway, modelProviders)
	capabilityAuthorizer, err := capability.NewAuthorizer(repository)
	if err != nil {
		slog.Error("configure AgentVersion capability authorizer", "error", err)
		os.Exit(1)
	}
	serverOptions := append([]grpc.ServerOption{}, grpcx.ServerOptions()...)
	allowedTenant := *tenantID
	if !*devMode {
		serverSVID, loadErr := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if loadErr != nil {
			slog.Error("load gateway SVID", "error", loadErr)
			os.Exit(1)
		}
		bundlePEM, readErr := os.ReadFile(*trustBundle)
		if readErr != nil {
			slog.Error("read SPIFFE trust bundle", "error", readErr)
			os.Exit(1)
		}
		trustPool, parseErr := spiffe.TrustBundlePool([][]byte{bundlePEM})
		if parseErr != nil {
			slog.Error("parse SPIFFE trust bundle", "error", parseErr)
			os.Exit(1)
		}
		pattern, parseErr := spiffe.ParsePattern(*spiffePattern)
		if parseErr != nil {
			slog.Error("parse SPIFFE worker pattern", "error", parseErr)
			os.Exit(1)
		}
		serverOptions = append(serverOptions, grpcx.ServerMTLSOptions(serverSVID, trustPool)...)
		serverOptions = append(serverOptions, grpcx.WithSpiffeIdentity(pattern))
		allowedTenant = "*"
	}
	var embedder memory.Embedder = memory.DevEmbedder{}
	if !*devMode {
		embedder, err = memory.NewHTTPEmbedder(*embeddingEndpoint, *embeddingToken, nil)
		if err != nil {
			slog.Error("configure production embedding service", "error", err)
			os.Exit(2)
		}
	}
	server := grpc.NewServer(serverOptions...)
	toolService := gateway.NewService(decisionGateway, allowedTenant, capabilityAuthorizer).WithRuntimeFence(repository)
	memoryService := gateway.NewMemoryService(
		memory.NewGateway(embedder, repository), repository, allowedTenant, capabilityAuthorizer,
	)
	modelService := gateway.NewModelService(modelGateway, allowedTenant, capabilityAuthorizer)
	modelInvocationService := gateway.NewModelInvocationService(modelInvoker, allowedTenant, capabilityAuthorizer)
	// SendMessage is gated twice: the sending AgentVersion's immutable peer
	// grant, then the tenant's allowed_peers. Receive and acknowledge are not
	// gated: they can only ever reach the caller's own fenced run's mailbox.
	ipcService := gateway.NewIPCService(
		repository, repository, repository, allowedTenant, capabilityAuthorizer, policyEngine,
	)
	gatewayv1.RegisterToolGatewayServiceServer(server, toolService)
	gatewayv1.RegisterMemoryGatewayServiceServer(server, memoryService)
	modelv1.RegisterModelGatewayServiceServer(server, modelService)
	modelv1.RegisterModelInvocationServiceServer(server, modelInvocationService)
	ipcv1.RegisterIPCGatewayServiceServer(server, ipcService)
	legacyAliases := []struct {
		descriptor grpc.ServiceDesc
		service    any
		name       string
	}{
		{gatewayv1.ToolGatewayService_ServiceDesc, toolService, "fenced.gateway.v1alpha1.ToolGatewayService"},
		{gatewayv1.MemoryGatewayService_ServiceDesc, memoryService, "fenced.gateway.v1alpha1.MemoryGatewayService"},
		{modelv1.ModelGatewayService_ServiceDesc, modelService, "fenced.model.v1alpha1.ModelGatewayService"},
	}
	for _, alias := range legacyAliases {
		if err := grpcx.RegisterLegacyServiceAlias(server, alias.descriptor, alias.service, alias.name); err != nil {
			slog.Error("register legacy Gateway Protocol alias", "service", alias.name, "error", err)
			os.Exit(1)
		}
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		slog.Error("listen for Tool Gateway", "error", err)
		os.Exit(1)
	}
	go func() {
		<-ctx.Done()
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
	slog.Info("Agent OS Tool Gateway listening", "address", *listen, "devMode", *devMode, "tenant", allowedTenant)
	if err := server.Serve(listener); err != nil {
		slog.Error("Tool Gateway server", "error", err)
		os.Exit(1)
	}
}

func isHTTPSURL(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil && parsed.Fragment == ""
}

func loadToolEndpoints(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config struct {
		Endpoints map[string]string `json:"endpoints"`
	}
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("tool endpoint file must contain exactly one JSON document")
	}
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("tool endpoint file requires a non-empty endpoints object")
	}
	return config.Endpoints, nil
}

func isLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func loadTenantPolicies(path string) (policy.TenantPolicies, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var policies policy.TenantPolicies
	if err := decoder.Decode(&policies); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("tenant policy file must contain exactly one JSON document")
	}
	return policies, nil
}
