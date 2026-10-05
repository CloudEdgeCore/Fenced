//go:build linux

package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// directExecutor runs workloads directly through the runsc CLI with an OCI
// bundle, bypassing the containerd shim. It is used on hosts where the
// containerd -> runsc shim path is unavailable or hangs (e.g. WSL2), while
// keeping the same fail-closed sandbox properties: a read-only root filesystem
// mounted from the pinned image, no Linux capabilities, explicit resource
// limits, and bounded output spooling. (The struct fields are declared in
// executor.go so the non-Linux stub can name the type.)

// NewRunscDirectExecutor constructs a direct runsc executor after verifying the
// runsc binary is available. The containerd CLI (ctr) is used only to mount the
// pinned image's root filesystem; workload execution never touches the
// containerd shim.
func NewRunscDirectExecutor(options ...DirectRunscOption) (Executor, error) {
	runscPath, err := exec.LookPath("runsc")
	if err != nil {
		return nil, fmt.Errorf("runsc is required by the direct OCI/gVisor provider: %w", err)
	}
	ctrPath, err := exec.LookPath("ctr")
	if err != nil {
		return nil, fmt.Errorf("containerd CLI (ctr) is required to mount workload images: %w", err)
	}
	executor := &directExecutor{
		ctrPath: ctrPath, namespace: "fenced", runscPath: runscPath, platform: "kvm",
		rootDir: "/run/containerd/runsc/fenced", outputLimit: 1 << 20,
	}
	for _, option := range options {
		option(executor)
	}
	return executor, nil
}

// Prepare mounts the pinned image's root filesystem, writes an OCI bundle with
// a fail-closed spec, and starts `runsc run` in the foreground-equivalent
// goroutine model used by the containerd executor.
func (e *directExecutor) Prepare(ctx context.Context, spec ExecutionSpec) (Execution, error) {
	if strings.TrimSpace(spec.ImageRef) == "" {
		return nil, fmt.Errorf("OCI image reference is required")
	}
	if strings.TrimSpace(spec.AttemptID) == "" {
		return nil, fmt.Errorf("attempt ID is required")
	}
	if strings.ContainsAny(spec.AttemptID, " \t\n\"'") {
		return nil, fmt.Errorf("attempt ID is not a safe container identifier")
	}
	containerID := sandboxIDPrefix + spec.AttemptID
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

	bundleDir, err := os.MkdirTemp("", "fenced-bundle-*")
	if err != nil {
		e.unregister(containerID)
		return nil, fmt.Errorf("create bundle directory: %w", err)
	}
	inputDir, err := os.MkdirTemp("", "fenced-input-*")
	if err != nil {
		_ = os.RemoveAll(bundleDir)
		e.unregister(containerID)
		return nil, fmt.Errorf("create workload input directory: %w", err)
	}
	inputPath := filepath.Join(inputDir, "workload.json")
	if err := os.WriteFile(inputPath, spec.WorkloadSpecJSON, 0o400); err != nil {
		_ = os.RemoveAll(bundleDir)
		_ = os.RemoveAll(inputDir)
		e.unregister(containerID)
		return nil, fmt.Errorf("persist workload spec: %w", err)
	}

	rootfsDir := filepath.Join(bundleDir, "rootfs")
	if err := os.Mkdir(rootfsDir, 0o711); err != nil {
		_ = os.RemoveAll(bundleDir)
		_ = os.RemoveAll(inputDir)
		e.unregister(containerID)
		return nil, fmt.Errorf("create rootfs directory: %w", err)
	}
	if err := e.prepareRootfs(ctx, spec.ImageRef, rootfsDir); err != nil {
		_ = os.RemoveAll(bundleDir)
		_ = os.RemoveAll(inputDir)
		e.unregister(containerID)
		return nil, fmt.Errorf("prepare workload rootfs for %s: %w", spec.ImageRef, err)
	}
	if err := writeRunscSpec(filepath.Join(bundleDir, "config.json"), spec, inputPath); err != nil {
		_ = os.RemoveAll(bundleDir)
		_ = os.RemoveAll(inputDir)
		e.unregister(containerID)
		return nil, fmt.Errorf("write OCI bundle spec: %w", err)
	}

	args := []string{"--root", e.rootDir, "--platform", e.platform, "run", "--bundle", bundleDir, containerID}
	command := exec.CommandContext(ctx, e.runscPath, args...)
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
		_ = os.RemoveAll(bundleDir)
		_ = os.RemoveAll(inputDir)
	}()
	return &directExecution{executor: e, containerID: containerID, imageRef: spec.ImageRef, bundleDir: bundleDir, inputDir: inputDir, done: done}, nil
}

// Destroy deletes the sandbox container and cleans up the bundle directory.
func (e *directExecutor) Destroy(ctx context.Context, execution Execution) error {
	direct, ok := execution.(*directExecution)
	if !ok {
		return fmt.Errorf("execution is not a direct runsc execution")
	}
	deleteErr := e.runscRun(ctx, "delete", "-force", direct.containerID)
	_ = os.RemoveAll(direct.bundleDir)
	_ = os.RemoveAll(direct.inputDir)
	e.unregister(direct.containerID)
	if deleteErr != nil && !notFound(deleteErr) {
		return fmt.Errorf("delete sandbox container: %w", deleteErr)
	}
	return nil
}

// prepareRootfs mounts the pinned image read-only, copies its contents into
// targetDir, and unmounts the image. The resulting directory is a writable copy
// that the gofer can use for its filesystem store.
func (e *directExecutor) prepareRootfs(ctx context.Context, imageRef, targetDir string) error {
	mountPoint, err := os.MkdirTemp("", "fenced-rootfs-mount-*")
	if err != nil {
		return fmt.Errorf("create rootfs mount point: %w", err)
	}
	defer os.RemoveAll(mountPoint)
	if err := e.run(ctx, "images", "mount", imageRef, mountPoint); err != nil {
		return fmt.Errorf("mount image %s: %w", imageRef, err)
	}
	defer func() {
		_ = e.run(ctx, "images", "unmount", imageRef)
	}()
	// Copy the mounted rootfs so the gofer has a writable directory.
	entries, err := os.ReadDir(mountPoint)
	if err != nil {
		return fmt.Errorf("read mounted rootfs: %w", err)
	}
	for _, entry := range entries {
		src := filepath.Join(mountPoint, entry.Name())
		dst := filepath.Join(targetDir, entry.Name())
		if err := exec.Command("cp", "-a", src, dst).Run(); err != nil {
			return fmt.Errorf("copy rootfs entry %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// runscRun executes one runsc command with bounded captured output.
func (e *directExecutor) runscRun(ctx context.Context, args ...string) error {
	command := exec.CommandContext(ctx, e.runscPath, append([]string{"--root", e.rootDir}, args...)...)
	var output limitedBuffer
	output.max = e.outputLimit
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return fmt.Errorf("runsc %s: %w (output: %s)", strings.Join(args, " "), err, strings.TrimSpace(output.String()))
	}
	return nil
}

// ctrRun executes one ctr command with bounded captured output.
func (e *directExecutor) ctrRun(ctx context.Context, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, e.ctrPath, append([]string{"-n", e.namespace}, args...)...)
	var output limitedBuffer
	output.max = e.outputLimit
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("ctr %s: %w (output: %s)", strings.Join(args, " "), err, strings.TrimSpace(output.String()))
	}
	return []byte(output.String()), nil
}

// run executes one ctr command, discarding output on success.
func (e *directExecutor) run(ctx context.Context, args ...string) error {
	_, err := e.ctrRun(ctx, args...)
	return err
}

// reapOrphans deletes sandboxes with our prefix that no live execution owns:
// a worker that crashed leaves fenced-* containers behind; the next Prepare
// cleans them up.
func (e *directExecutor) reapOrphans(ctx context.Context) error {
	command := exec.CommandContext(ctx, e.runscPath, "--root", e.rootDir, "list", "-q")
	var output limitedBuffer
	output.max = e.outputLimit
	command.Stdout, command.Stderr = &output, &output
	if err := command.Run(); err != nil {
		return fmt.Errorf("runsc list: %w", err)
	}
	for _, id := range reapTargets(strings.Fields(strings.TrimSpace(output.String())), e.activeSnapshot()) {
		_ = e.runscRun(ctx, "delete", "-force", id)
	}
	return nil
}

func (e *directExecutor) register(containerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active == nil {
		e.active = map[string]struct{}{}
	}
	e.active[containerID] = struct{}{}
}

func (e *directExecutor) unregister(containerID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.active, containerID)
}

func (e *directExecutor) activeSnapshot() map[string]struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	snapshot := make(map[string]struct{}, len(e.active))
	for id := range e.active {
		snapshot[id] = struct{}{}
	}
	return snapshot
}

// directExecution owns one foreground `runsc run` process; when the process
// exits, the sandbox has terminated and runsc propagates the exit code.
type directExecution struct {
	executor    *directExecutor
	containerID string
	imageRef    string
	bundleDir   string
	inputDir    string
	done        chan executionOutcome
}

func (e *directExecution) ID() string { return e.containerID }

func (e *directExecution) Wait(ctx context.Context) (RunResult, error) {
	select {
	case outcome := <-e.done:
		return outcome.result, outcome.err
	case <-ctx.Done():
		killCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = e.executor.runscRun(killCtx, "delete", "-force", e.containerID)
		select {
		case outcome := <-e.done:
			return outcome.result, outcome.err
		case <-time.After(60 * time.Second):
			return RunResult{}, fmt.Errorf("sandbox %s did not terminate after kill; orphan cleanup required", e.containerID)
		}
	}
}

// directEnvironment builds the sandbox process environment: tenant/attempt
// identity plus the workload input path, identical to the containerd executor.
func directEnvironment(spec ExecutionSpec, inputPath string) []string {
	return []string{
		"FENCED_TENANT_ID=" + spec.TenantID,
		"FENCED_ATTEMPT_ID=" + spec.AttemptID,
		"FENCED_AGENT_VERSION_REF=" + spec.AgentVersionRef,
		"FENCED_INPUT_PATH=" + inputPath,
		"FENCED_WORKSPACE_PATH=/fenced/workspace",
	}
}

// writeRunscSpec writes a fail-closed OCI bundle spec: a read-only root
// filesystem, no Linux capabilities, no-new-privileges, the workload spec
// bind-mounted read-only, a size-bounded tmpfs workspace, and explicit CPU and
// memory resources. The structure mirrors the spec containerd generates for the
// runsc runtime so the gVisor sandbox boots without the containerd shim.
func writeRunscSpec(path string, spec ExecutionSpec, inputPath string) error {
	mounts := []map[string]any{
		{"destination": "/proc", "type": "proc", "source": "proc"},
		{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{"destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{"destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{"destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue", "options": []string{"nosuid", "noexec", "nodev"}},
		{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
		{"destination": "/run", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{"destination": "/fenced/input/workload.json", "type": "bind", "source": inputPath, "options": []string{"rbind", "ro"}},
	}
	if spec.WorkspaceBytes > 0 {
		mounts = append(mounts, map[string]any{
			"destination": "/fenced/workspace", "type": "tmpfs", "source": "tmpfs",
			"options": []string{"nosuid", "noexec", "nodev", fmt.Sprintf("size=%d", spec.WorkspaceBytes)},
		})
	}
	resources := map[string]any{
		"devices": []map[string]any{
			{"allow": false, "access": "rwm"},
			{"allow": true, "type": "c", "major": 1, "minor": 3, "access": "rwm"},
			{"allow": true, "type": "c", "major": 1, "minor": 8, "access": "rwm"},
			{"allow": true, "type": "c", "major": 1, "minor": 7, "access": "rwm"},
			{"allow": true, "type": "c", "major": 5, "minor": 0, "access": "rwm"},
			{"allow": true, "type": "c", "major": 1, "minor": 5, "access": "rwm"},
			{"allow": true, "type": "c", "major": 1, "minor": 9, "access": "rwm"},
			{"allow": true, "type": "c", "major": 5, "minor": 1, "access": "rwm"},
			{"allow": true, "type": "c", "major": 136, "access": "rwm"},
		},
	}
	if spec.CPUQuotaMillis > 0 {
		resources["cpu"] = map[string]any{"quota": spec.CPUQuotaMillis * 1000, "period": 100000}
	}
	if spec.MemoryLimitMiB > 0 {
		resources["memory"] = map[string]any{"limit": spec.MemoryLimitMiB << 20}
	}
	args := spec.Command
	if len(args) == 0 {
		args = []string{"/bin/sh"}
	}
	emptyCaps := []string{}
	document := map[string]any{
		"ociVersion": "1.0.0",
		"process": map[string]any{
			"user": map[string]any{
				"uid": 0, "gid": 0,
				"additionalGids": []int{0, 1, 2, 3, 4, 6, 10, 11, 20, 26, 27},
			},
			"args":            args,
			"env":             directEnvironment(spec, "/fenced/input/workload.json"),
			"cwd":             "/",
			"noNewPrivileges": true,
			"capabilities": map[string]any{
				"bounding": emptyCaps, "effective": emptyCaps,
				"permitted": emptyCaps, "inheritable": emptyCaps, "ambient": emptyCaps,
			},
			"rlimits": []map[string]any{{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}},
		},
		"root":     map[string]any{"path": "rootfs", "readonly": true},
		"hostname": "fenced",
		"mounts":   mounts,
		"linux": map[string]any{
			"resources": resources,
			"namespaces": []map[string]any{
				{"type": "pid"},
				{"type": "network"},
				{"type": "ipc"},
				{"type": "uts"},
				{"type": "mount"},
			},
			"cgroupsPath": "/fenced",
			"maskedPaths": []string{
				"/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys",
				"/proc/latency_stats", "/proc/timer_list", "/proc/timer_stats",
				"/proc/sched_debug", "/proc/scsi", "/sys/firmware",
			},
			"readonlyPaths": []string{
				"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger",
			},
		},
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return err
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, encoded, "", "  "); err != nil {
		return err
	}
	return os.WriteFile(path, pretty.Bytes(), 0o400)
}
