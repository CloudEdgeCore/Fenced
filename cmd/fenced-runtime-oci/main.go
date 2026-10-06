package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CloudEdgeCore/Fenced/cmd/mtlsutil"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/platform/artifact"
	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/grpcx"
	"github.com/CloudEdgeCore/Fenced/internal/platform/otel"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/oci"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	controlAddress := flag.String("control-address", "127.0.0.1:9090", "Runtime Protocol control endpoint")
	tenantID := flag.String("tenant", "", "tenant encoded in the worker X.509-SVID")
	runtimeInstanceID := flag.String("runtime-instance-id", "", "assigned runtime instance ID")
	artifactRoot := flag.String("artifact-root", "", "content-addressed artifact directory")
	artifactGC := flag.Duration("artifact-gc-retention", 0, "delete artifacts older than this retention at startup (0 = disabled; O7)")
	imageRef := flag.String("image-ref", "", "digest-pinned OCI image that runs the published AgentVersion")
	namespace := flag.String("containerd-namespace", "fenced", "containerd namespace for sandboxed containers")
	runtimeName := flag.String("containerd-runtime", "io.containerd.runsc.v1", "containerd runtime (runsc by default)")
	runtimeConfigPath := flag.String("runtime-config-path", "/etc/containerd/runsc.toml", "gVisor shim configuration passed to ctr (empty disables)")
	// snapshotter selects the containerd snapshotter for sandbox rootfs
	// mounts (v0.7): "overlayfs" on production hosts, "native" in nested
	// environments (containerd inside a container cannot mount
	// overlay-on-overlay).
	snapshotter := flag.String("containerd-snapshotter", "overlayfs", "containerd snapshotter for sandbox rootfs (overlayfs or native)")
	skipImagePull := flag.Bool("skip-image-pull", false, "assume the image is already loaded into containerd")
	directMode := flag.Bool("runsc-direct", false, "run sandboxes directly via the runsc CLI instead of the containerd shim (for hosts where the shim is unavailable, e.g. WSL2)")
	runscPlatform := flag.String("runsc-platform", "kvm", "gVisor platform when running sandboxes directly (kvm, systrap or ptrace)")
	cpuQuotaMillis := flag.Int64("cpu-quota-millis", 0, "cgroup CPU quota in milliseconds (0 = Admission decides)")
	memoryLimitMiB := flag.Int64("memory-limit-mib", 0, "cgroup memory limit in MiB (0 = Admission decides)")
	workspaceBytes := flag.Int64("workspace-bytes", 0, "tmpfs workspace size in bytes (0 = Admission decides)")
	pollInterval := flag.Duration("poll-interval", 250*time.Millisecond, "assignment polling interval")
	heartbeatTTL := flag.Duration("heartbeat-ttl", 30*time.Second, "requested runtime lease TTL")
	devMode := flag.Bool("dev-mode", false, "allow plaintext Runtime Protocol for local development")
	tlsCert := flag.String("tls-cert", "", "worker X.509-SVID certificate (PEM)")
	tlsKey := flag.String("tls-key", "", "worker X.509-SVID private key (PEM)")
	trustBundle := flag.String("trust-bundle", "", "SPIFFE trust bundle (PEM CA certificates)")
	flag.Parse()
	tlsConfigured := *tlsCert != "" || *tlsKey != "" || *trustBundle != ""
	if strings.TrimSpace(*tenantID) == "" || strings.TrimSpace(*runtimeInstanceID) == "" || strings.TrimSpace(*artifactRoot) == "" ||
		strings.TrimSpace(*imageRef) == "" || *pollInterval <= 0 || *heartbeatTTL <= 0 || (!*devMode && !tlsConfigured) {
		slog.Error("tenant, runtime instance, artifact root, digest-pinned image, positive intervals, and mTLS (or explicit dev mode) are required")
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

	options := []oci.RunscOption{
		oci.WithNamespace(*namespace),
		oci.WithRuntime(*runtimeName),
		oci.WithRuntimeConfigPath(*runtimeConfigPath),
		oci.WithSnapshotter(*snapshotter),
	}
	if *skipImagePull {
		options = append(options, oci.WithSkipPull())
	}
	var executor oci.Executor
	if *directMode {
		executor, err = oci.NewRunscDirectExecutor(oci.WithRunscPlatform(*runscPlatform))
	} else {
		executor, err = oci.NewRunscExecutor(options...)
	}
	if err != nil {
		slog.Error("create OCI/gVisor executor", "error", err)
		os.Exit(1)
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
	connection, err := grpc.NewClient(*controlAddress,
		append([]grpc.DialOption{grpc.WithTransportCredentials(controlCredentials)}, grpcx.ClientOptions()...)...)
	if err != nil {
		slog.Error("create Runtime Protocol client", "error", err)
		os.Exit(1)
	}
	defer connection.Close()
	artifactStore, err := artifact.NewFilesystem(*artifactRoot, 64<<20)
	if err != nil {
		slog.Error("create artifact store", "error", err)
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
	worker := oci.NewWorker(runtimev1.NewRuntimeControlServiceClient(connection), artifactStore, executor,
		*tenantID, *runtimeInstanceID, *heartbeatTTL, *imageRef)
	worker.WithResourceLimits(*cpuQuotaMillis, *memoryLimitMiB, *workspaceBytes)
	ticker := time.NewTicker(*pollInterval)
	defer ticker.Stop()
	for {
		processed, err := worker.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("OCI/gVisor runtime execution", "error", err)
		}
		if processed {
			slog.Info("OCI/gVisor runtime completed one assignment", "runtimeInstanceId", *runtimeInstanceID)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
