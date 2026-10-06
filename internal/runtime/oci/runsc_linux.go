//go:build linux

package oci

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

var defaultCapabilitiesToDrop = []string{
	"CAP_AUDIT_WRITE",
	"CAP_CHOWN",
	"CAP_DAC_OVERRIDE",
	"CAP_FOWNER",
	"CAP_FSETID",
	"CAP_KILL",
	"CAP_MKNOD",
	"CAP_NET_BIND_SERVICE",
	"CAP_NET_RAW",
	"CAP_SETFCAP",
	"CAP_SETGID",
	"CAP_SETPCAP",
	"CAP_SETUID",
	"CAP_SYS_CHROOT",
}

// ctrExecutor drives the pinned containerd CLI with the gVisor runtime. The
// command surface is deliberately fail-closed: a read-only root filesystem,
// no Linux capabilities, the containerd default seccomp profile, explicit
// cgroup limits, and the pinned runsc shim configuration are applied to every
// untrusted workload.

// NewRunscExecutor constructs the containerd/runsc executor after verifying
// that the ctr CLI is available. It must run on Linux with containerd and
// runsc installed; the worker never falls back to an unsandboxed runtime.
func NewRunscExecutor(options ...RunscOption) (Executor, error) {
	path, err := exec.LookPath("ctr")
	if err != nil {
		return nil, fmt.Errorf("containerd CLI (ctr) is required by the OCI/gVisor provider: %w", err)
	}
	executor := &ctrExecutor{
		ctrPath: path, namespace: "fenced", runtime: "io.containerd.runsc.v1",
		runtimeConfigPath: "/etc/containerd/runsc.toml", snapshotter: "overlayfs",
		pullTimeout: 10 * time.Minute, outputLimit: 1 << 20,
	}
	for _, option := range options {
		option(executor)
	}
	return executor, nil
}

func (e *ctrExecutor) Prepare(ctx context.Context, spec ExecutionSpec) (Execution, error) {
	if strings.TrimSpace(spec.ImageRef) == "" {
		return nil, fmt.Errorf("OCI image reference is required")
	}
	if strings.TrimSpace(spec.AttemptID) == "" {
		return nil, fmt.Errorf("attempt ID is required")
	}
	if strings.ContainsAny(spec.AttemptID, " \t\n\"'") {
		return nil, fmt.Errorf("attempt ID is not a safe container identifier")
	}
	containerID := "fenced-" + spec.AttemptID
	// Claim ownership before reaping so concurrent Prepares never delete each
	// other's containers, then clean up containers orphaned by crashed
	// workers (hardening checklist §4.3).
	e.register(containerID)
	defer func() {
		if ctx.Err() != nil {
			e.unregister(containerID)
		}
	}()
	if err := e.reapOrphans(ctx); err != nil && ctx.Err() == nil {
		e.unregister(containerID)
		return nil, fmt.Errorf("reap orphaned sandbox containers: %w", err)
	}
	if !e.skipPull {
		pullCtx, cancel := context.WithTimeout(ctx, e.pullTimeout)
		defer cancel()
		if err := e.run(pullCtx, "images", "pull", "--snapshotter", e.snapshotter, spec.ImageRef); err != nil {
			e.unregister(containerID)
			return nil, fmt.Errorf("pull workload image %s: %w", spec.ImageRef, err)
		}
	}

	inputDir, err := os.MkdirTemp("", "fenced-input-*")
	if err != nil {
		e.unregister(containerID)
		return nil, fmt.Errorf("create workload input directory: %w", err)
	}
	inputPath := filepath.Join(inputDir, "workload.json")
	if err := os.WriteFile(inputPath, spec.WorkloadSpecJSON, 0o400); err != nil {
		_ = os.RemoveAll(inputDir)
		e.unregister(containerID)
		return nil, fmt.Errorf("persist workload spec: %w", err)
	}

	args := []string{"run", "--runtime", e.runtime}
	if strings.TrimSpace(e.runtimeConfigPath) != "" {
		args = append(args, "--runtime-config-path", e.runtimeConfigPath)
	}
	args = append(args,
		"--snapshotter", e.snapshotter,
		"--read-only",
		"--seccomp",
	)
	for _, capability := range defaultCapabilitiesToDrop {
		args = append(args, "--cap-drop", capability)
	}
	for _, mount := range e.mounts(inputPath, spec.WorkspaceBytes) {
		args = append(args, "--mount", mount)
	}
	for _, env := range e.environment(spec, "/fenced/input/workload.json") {
		args = append(args, "--env", env)
	}
	// Sandbox limits (hardening checklist §4.1): the ctr flags encode the OCI
	// Linux resources runsc applies (cpu-quota in microseconds, memory in
	// bytes). Admission guarantees non-zero values for container classes; the
	// exact ctr flag surface must be re-validated against the pinned
	// containerd version on the Linux integration CI.
	if spec.CPUQuotaMillis > 0 {
		args = append(args, "--cpu-quota", fmt.Sprintf("%d", spec.CPUQuotaMillis*1000))
	}
	if spec.MemoryLimitMiB > 0 {
		args = append(args, "--memory-limit", fmt.Sprintf("%d", spec.MemoryLimitMiB<<20))
	}
	args = append(args, spec.ImageRef, containerID)
	args = append(args, spec.Command...)

	// CommandContext ties the ctr process to the execution context: when the
	// lease keeper cancels the execution (cancellation or fence break), the
	// process is killed, Run returns, the spawn goroutine completes and the
	// staging directory is removed — no orphaned process or goroutine leak.
	command := exec.CommandContext(ctx, e.ctrPath, append([]string{"-n", e.namespace}, args...)...)
	// Bounded output spooling (hardening checklist §4.4): stdout/stderr go to
	// the artifact store through the spooler with a hard cap; without a
	// spooler the bounded discard buffer applies.
	spoolPipe, spoolWriter := io.Pipe()
	stdoutRef, stdoutTruncated, spoolErr := make(chan *store.ArtifactReference, 1), make(chan bool, 1), make(chan error, 1)
	go func() {
		ref, truncated, err := spoolOutput(ctx, spec.OutputSpooler, spec.TenantID, spec.AttemptID, "application/vnd.fenced.stdout+octet-stream", spoolPipe)
		spoolPipe.CloseWithError(err)
		stdoutRef <- ref
		stdoutTruncated <- truncated
		spoolErr <- err
	}()
	var discard limitedBuffer
	discard.max = e.outputLimit
	command.Stdout = io.MultiWriter(spoolWriter, &discard)
	command.Stderr = spoolWriter
	done := make(chan executionOutcome, 1)
	startedAt := time.Now()
	go func() {
		runErr := command.Run()
		_ = spoolWriter.Close()
		result, outcomeErr := outcomeFor(runErr, startedAt)
		result.Stdout = <-stdoutRef
		result.StdoutTruncated = <-stdoutTruncated
		if spoolErr := <-spoolErr; spoolErr != nil && spoolErr != io.ErrClosedPipe && runErr == nil {
			result.FailureCode = "output_spool_failed"
		}
		done <- executionOutcome{result: result, err: outcomeErr}
		_ = os.RemoveAll(inputDir)
	}()
	return &ctrExecution{executor: e, containerID: containerID, done: done}, nil
}

// reapOrphans deletes containers with our prefix that no live execution
// owns (hardening checklist §4.3): a worker that crashed leaves fenced-*
// containers behind; the next Prepare cleans them up before pulling.
func (e *ctrExecutor) reapOrphans(ctx context.Context) error {
	listed, err := e.runOutput(ctx, "containers", "list", "-q")
	if err != nil {
		return err
	}
	for _, id := range reapTargets(strings.Fields(strings.TrimSpace(string(listed))), e.activeSnapshot()) {
		_ = e.run(ctx, "tasks", "delete", "-f", id)
		_ = e.run(ctx, "containers", "delete", id)
	}
	return nil
}

// runOutput executes one ctr command and returns its bounded stdout.
func (e *ctrExecutor) runOutput(ctx context.Context, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, e.ctrPath, append([]string{"-n", e.namespace}, args...)...)
	var output limitedBuffer
	output.max = e.outputLimit
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("ctr %s: %w (output: %s)", strings.Join(args, " "), err, strings.TrimSpace(output.String()))
	}
	return []byte(output.String()), nil
}

func (e *ctrExecutor) Destroy(ctx context.Context, execution Execution) error {
	if err := e.run(ctx, "tasks", "delete", "-f", execution.ID()); err != nil && !notFound(err) {
		return fmt.Errorf("delete sandbox task: %w", err)
	}
	if err := e.run(ctx, "containers", "delete", execution.ID()); err != nil && !notFound(err) {
		return fmt.Errorf("delete sandbox container: %w", err)
	}
	return nil
}

// mounts builds the containerd mount flags: a read-only bind of the workload
// spec and a size-bounded tmpfs workspace. No host path other than the spec
// file is ever mounted into the sandbox.
func (e *ctrExecutor) mounts(inputPath string, workspaceBytes int64) []string {
	mounts := []string{
		"type=bind,src=" + inputPath + ",dst=/fenced/input/workload.json,options=rbind:ro",
	}
	if workspaceBytes > 0 {
		mounts = append(mounts, fmt.Sprintf("type=tmpfs,dst=/fenced/workspace,options=size=%d", workspaceBytes))
	}
	return mounts
}

func (e *ctrExecutor) environment(spec ExecutionSpec, inputPath string) []string {
	return []string{
		"FENCED_TENANT_ID=" + spec.TenantID,
		"FENCED_ATTEMPT_ID=" + spec.AttemptID,
		"FENCED_AGENT_VERSION_REF=" + spec.AgentVersionRef,
		"FENCED_INPUT_PATH=" + inputPath,
		"FENCED_WORKSPACE_PATH=/fenced/workspace",
	}
}

// run executes one ctr command with bounded captured output.
func (e *ctrExecutor) run(ctx context.Context, args ...string) error {
	command := exec.CommandContext(ctx, e.ctrPath, append([]string{"-n", e.namespace}, args...)...)
	var output limitedBuffer
	output.max = e.outputLimit
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return fmt.Errorf("ctr %s: %w (output: %s)", strings.Join(args, " "), err, strings.TrimSpace(output.String()))
	}
	return nil
}

type executionOutcome struct {
	result RunResult
	err    error
}

// ctrExecution owns one foreground `ctr run` process; when the process exits,
// the task has terminated and ctr propagates the task exit code.
type ctrExecution struct {
	executor    *ctrExecutor
	containerID string
	done        chan executionOutcome
}

func (e *ctrExecution) ID() string { return e.containerID }

func (e *ctrExecution) Wait(ctx context.Context) (RunResult, error) {
	select {
	case outcome := <-e.done:
		return outcome.result, outcome.err
	case <-ctx.Done():
		killCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = e.executor.run(killCtx, "tasks", "kill", e.containerID)
		select {
		case outcome := <-e.done:
			return outcome.result, outcome.err
		case <-time.After(60 * time.Second):
			return RunResult{}, fmt.Errorf("sandbox %s did not terminate after kill; orphan cleanup required", e.containerID)
		}
	}
}

// outcomeFor maps a finished command to a RunResult. A task that exited
// non-zero surfaces as ExitCode with a nil error; a ctr machinery failure
// surfaces as an error so the worker reports execution_failed.
func outcomeFor(err error, startedAt time.Time) (RunResult, error) {
	result := RunResult{UsageMillis: time.Since(startedAt).Milliseconds()}
	if err == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return result, err
}

// notFound reports whether a ctr failure was an object-not-found, which is
// benign during Destroy.
func notFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

// limitedBuffer is a write-only buffer that truncates at max bytes so container
// or tool output can never grow unbounded in kernel memory.
type limitedBuffer struct {
	buffer bytes.Buffer
	max    int64
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	remaining := b.max - int64(b.buffer.Len())
	if remaining <= 0 {
		return len(p), nil
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	return b.buffer.Write(p)
}

func (b *limitedBuffer) String() string { return b.buffer.String() }
