// Package oci provides the OCI + gVisor (runsc) Runtime Provider worker. It
// implements the fenced Runtime Protocol loop from ADR-006 and delegates
// workload execution to a sandboxed Executor so the protocol logic stays
// testable without containerd or runsc installed.
package oci

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

const (
	// ProviderName identifies this provider in checkpoints and audit records.
	ProviderName = "oci-gvisor"
	// RuntimeABI is the provider-level Runtime ABI declared in checkpoint envelopes.
	RuntimeABI = "fenced.oci/v1"
	// CheckpointSchema is the logical checkpoint state schema version.
	CheckpointSchema = "fenced.oci-logical/v1"

	checkpointMediaType = "application/vnd.fenced.oci-checkpoint+json"
	resultMediaType     = "application/vnd.fenced.oci-result+json"
)

// ArtifactStore is the content-addressed store used for checkpoint state and
// results. The filesystem adapter is development-only; production must use an
// S3-compatible adapter with the same artifact:// identity.
type ArtifactStore interface {
	Put(context.Context, string, string, io.Reader) (store.ArtifactReference, error)
	Open(context.Context, string, store.ArtifactReference) (io.ReadCloser, error)
}

// ExecutionSpec is the provider-agnostic input to run one Attempt in a
// sandboxed container. All fields come from the fenced Assignment and from
// validated workload metadata; the Executor must never trust container-supplied
// values for its own resource or identity decisions.
type ExecutionSpec struct {
	TenantID         string
	AttemptID        string
	AgentVersionRef  string
	WorkloadSpecJSON []byte
	ImageRef         string
	// Command overrides the image Entrypoint/Cmd as an argv array. Empty uses
	// the immutable image configuration.
	Command           []string
	WorkspaceBytes    int64
	CPUQuotaMillis    int64
	MemoryLimitMiB    int64
	RuntimeClass      string
	RuntimePoolID     string
	RuntimeInstanceID string
	// OutputSpooler persists bounded workload stdout/stderr to the artifact
	// store; nil keeps the pre-hardening bounded-discard behavior.
	OutputSpooler OutputSpooler
}

// RunResult is what a provider reports after the workload terminates.
// Container stdout is spooled to the artifact store (bounded), never carried
// in kernel messages.
type RunResult struct {
	ExitCode    int
	UsageMillis int64
	// FailureCode is an optional machine-readable classification. When empty,
	// the worker derives one from ExitCode.
	FailureCode string
	// Stdout is the spooled stdout/stderr artifact, when the execution
	// declared an OutputSpooler and produced output.
	Stdout *store.ArtifactReference
	// StdoutTruncated reports that output exceeded the spool cap.
	StdoutTruncated bool
}

// OutputSpooler persists bounded workload output so container stdout/stderr
// never grow unbounded in kernel memory (hardening checklist §4.4). The
// worker's artifact store satisfies it.
type OutputSpooler interface {
	Spool(context.Context, string, string, string, io.Reader) (store.ArtifactReference, error)
}

// SpoolCap bounds spooled output per execution.
const SpoolCap = 4 << 20

// spoolOutput copies reader output into the spooler with a hard SpoolCap.
// Without a spooler the output is drained and discarded (bounded), which is
// the pre-hardening behavior. After spooling, one probe byte is read to
// detect overflow, so exactly-full output is not misreported as truncated.
func spoolOutput(ctx context.Context, spooler OutputSpooler, tenantID, attemptID, mediaType string, reader io.Reader) (*store.ArtifactReference, bool, error) {
	if reader == nil {
		return nil, false, nil
	}
	limited := &io.LimitedReader{R: reader, N: SpoolCap}
	if spooler == nil {
		_, _ = io.Copy(io.Discard, limited)
	} else {
		reference, err := spooler.Spool(ctx, tenantID, attemptID, mediaType, limited)
		if err != nil {
			return nil, false, err
		}
		var probe [1]byte
		extra, _ := reader.Read(probe[:])
		return &reference, extra > 0, nil
	}
	var probe [1]byte
	extra, _ := reader.Read(probe[:])
	return nil, extra > 0, nil
}

// sandboxIDPrefix names the containerd containers this provider owns.
const sandboxIDPrefix = "fenced-"

// reapTargets returns the listed container IDs this provider should clean up
// as orphans: our prefix, and not currently owned by a live execution.
func reapTargets(listed []string, active map[string]struct{}) []string {
	var targets []string
	for _, id := range listed {
		if !strings.HasPrefix(id, sandboxIDPrefix) {
			continue
		}
		if _, owned := active[id]; owned {
			continue
		}
		targets = append(targets, id)
	}
	return targets
}

// Execution is a prepared workload owned by the provider.
type Execution interface {
	// ID is the provider-side sandbox identifier (for example the containerd
	// container ID), used by Destroy.
	ID() string
	// Wait blocks until the workload terminates or ctx is cancelled. On
	// cancellation the provider must request termination of the workload.
	Wait(ctx context.Context) (RunResult, error)
}

// Executor prepares and destroys sandboxed executions. Implementations must be
// safe for concurrent use by multiple workers on the same RuntimeInstance.
type Executor interface {
	Prepare(ctx context.Context, spec ExecutionSpec) (Execution, error)
	Destroy(ctx context.Context, execution Execution) error
}

// RunscOption configures the containerd/runsc executor. It is declared here so
// the non-Linux stub can reference it; the concrete options and methods live in
// runsc_linux.go.
type RunscOption func(*ctrExecutor)

// DirectRunscOption configures the direct runsc executor. It is declared here
// so the non-Linux stub can reference it; the concrete constructor lives in
// direct_linux.go.
type DirectRunscOption func(*directExecutor)

// directExecutor runs workloads directly through the runsc CLI with an OCI
// bundle, bypassing the containerd shim. The struct is declared here
// (platform-neutral fields) so the non-Linux stub can name the type; its
// constructor and methods are Linux-only and live in direct_linux.go.
type directExecutor struct {
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	ctrPath string
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	namespace string
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	runscPath string
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	platform string
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	rootDir string
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	outputLimit int64
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	mu sync.Mutex
	//lint:ignore U1000 Used by the Linux implementation in direct_linux.go.
	active map[string]struct{}
}

// ctrExecutor is the containerd CLI + runsc executor. The struct is declared
// here (platform-neutral fields) so the non-Linux stub can name the type; its
// constructor and methods are Linux-only and live in runsc_linux.go.
type ctrExecutor struct {
	//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
	ctrPath           string
	namespace         string
	runtime           string
	runtimeConfigPath string
	snapshotter       string
	skipPull          bool
	//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
	pullTimeout time.Duration
	//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
	outputLimit int64
	// active tracks sandbox container IDs owned by live executions so the
	// preflight reaper never deletes a running workload.
	//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
	mu sync.Mutex
	//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
	active map[string]struct{}
}

// WithNamespace sets the containerd namespace (default "fenced").
func WithNamespace(namespace string) RunscOption {
	return func(e *ctrExecutor) { e.namespace = namespace }
}

// WithRuntime sets the containerd runtime name (default "io.containerd.runsc.v1").
func WithRuntime(runtime string) RunscOption {
	return func(e *ctrExecutor) { e.runtime = runtime }
}

// WithRuntimeConfigPath sets the gVisor shim configuration passed to ctr.
// The runsc shim does not read containerd's CRI runtime configuration when
// invoked directly by ctr, so every sandbox run must carry this path.
func WithRuntimeConfigPath(path string) RunscOption {
	return func(e *ctrExecutor) { e.runtimeConfigPath = path }
}

// WithSnapshotter sets the containerd snapshotter for sandbox rootfs mounts
// (default "overlayfs"). Nested environments (containerd inside a container,
// e.g. Docker Desktop/WSL2) cannot mount overlay-on-overlay and must use
// "native"; the choice is passed to ctr explicitly because ctr hardcodes
// overlayfs as its own default.
func WithSnapshotter(snapshotter string) RunscOption {
	return func(e *ctrExecutor) { e.snapshotter = snapshotter }
}

// WithSkipPull skips the image pull step (development images pre-loaded into
// containerd only).
func WithSkipPull() RunscOption {
	return func(e *ctrExecutor) { e.skipPull = true }
}

// WithRunscBinary sets the runsc binary path (default "runsc" resolved on PATH).
func WithRunscBinary(path string) DirectRunscOption {
	return func(e *directExecutor) { e.runscPath = path }
}

// WithRunscPlatform sets the runsc platform (default "kvm").
func WithRunscPlatform(platform string) DirectRunscOption {
	return func(e *directExecutor) { e.platform = platform }
}

// WithRunscRoot sets the runsc state root directory (default
// "/run/containerd/runsc/fenced").
func WithRunscRoot(root string) DirectRunscOption {
	return func(e *directExecutor) { e.rootDir = root }
}

// ExecutionSpecLimits returns the resource limits encoded in an ExecutionSpec
// as (cpuMillis, memoryMiB, workspaceBytes); zero values mean "no explicit
// limit", which Admission must not allow for untrusted workloads.
func (s ExecutionSpec) Limits() (cpuMillis, memoryMiB, workspaceBytes int64) {
	return s.CPUQuotaMillis, s.MemoryLimitMiB, s.WorkspaceBytes
}

// register marks a sandbox container ID as owned by a live execution so the
// orphan reaper never deletes it.
//
//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
func (e *ctrExecutor) register(containerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active == nil {
		e.active = map[string]struct{}{}
	}
	e.active[containerID] = struct{}{}
}

// unregister releases ownership after the execution is destroyed.
//
//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
func (e *ctrExecutor) unregister(containerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.active, containerID)
}

// activeSnapshot copies the live-execution container IDs.
//
//lint:ignore U1000 Used by the Linux implementation in runsc_linux.go.
func (e *ctrExecutor) activeSnapshot() map[string]struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	snapshot := make(map[string]struct{}, len(e.active))
	for id := range e.active {
		snapshot[id] = struct{}{}
	}
	return snapshot
}
