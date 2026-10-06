//go:build linux && integration

package devops_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ociImageName is the containerd image name the OCI drill runs. The CI job
// builds deploy/oci/agent-runtime/Dockerfile and imports it before running
// this test.
const ociImageName = "docker.io/library/fenced-runtime:latest"

// ociWorkerCommand returns the binary and arguments to start the OCI worker,
// wrapping with sudo when the containerd socket requires root access.
func ociWorkerCommand(bin string, args ...string) (string, []string) {
	if _, err := exec.LookPath("sudo"); err == nil {
		return "sudo", append([]string{"-n", bin}, args...)
	}
	return bin, args
}

// ociArtifactRoot creates a temporary directory for the OCI worker's artifact
// store. The worker runs via sudo, so its files are root-owned; remove them
// with sudo to avoid TempDir cleanup permission errors that Go's testing
// framework treats as test failures.
func ociArtifactRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "oci-artifacts-*")
	if err != nil {
		t.Fatalf("create artifact root: %v", err)
	}
	t.Cleanup(func() {
		if _, err := exec.LookPath("sudo"); err == nil {
			_ = exec.Command("sudo", "-n", "rm", "-rf", dir).Run()
		} else {
			_ = os.RemoveAll(dir)
		}
	})
	return dir
}

// requireOCIDrillEnvironment validates that the real gVisor/containerd
// environment is available; skips otherwise.
func requireOCIDrillEnvironment(t *testing.T) {
	t.Helper()
	if os.Getenv("FENCED_RUN_OCI_DRILL") != "1" {
		t.Skip("FENCED_RUN_OCI_DRILL is not set; the OCI/gVisor drill runs in its dedicated CI job")
	}
	// containerd CLI must be present.
	if _, err := exec.LookPath("ctr"); err != nil {
		t.Skipf("ctr (containerd CLI) not found: %v", err)
	}
	// The agent image must be imported into containerd. ctr must talk to the
	// root-owned containerd socket, so run it through sudo like the workflow
	// does (the drill CI job runs as a non-root user).
	listCommand := exec.Command("ctr", "-n", "fenced", "images", "ls", "-q")
	if _, err := exec.LookPath("sudo"); err == nil {
		listCommand = exec.Command("sudo", "-n", "ctr", "-n", "fenced", "images", "ls", "-q")
	}
	out, err := listCommand.CombinedOutput()
	if err != nil {
		t.Skipf("ctr images ls failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), ociImageName) {
		t.Skipf("agent image %s not imported into containerd:\n%s", ociImageName, out)
	}
}

// TestOCIIsolation is the A-path isolation drill: a task whose spec targets
// an OCI container is placed on the `oci` pool, executed by the real
// fenced-runtime-oci worker inside a gVisor sandbox, and completes
// successfully.
func TestOCIIsolation(t *testing.T) {
	requireOCIDrillEnvironment(t)
	h := newHarness(t, "oci-iso", false)
	ctx := context.Background()

	// Publish an agent that runs on the oci class.
	ociAgentSpec, _ := json.Marshal(map[string]any{
		"runtimeClassPolicy": map[string]any{"allowed": []string{"oci", "research-network"}, "preferred": "oci"},
		"lifecycle":          map[string]any{"maxAttempts": 3},
		"runtimes": []any{
			map[string]any{"class": "oci", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.oci/v1", "entrypoint": []string{"oci://agent-runtime"}},
			map[string]any{"class": "research-network", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.adapter-http/v1", "entrypoint": []string{"fenced-binding://devops"}},
		},
		"capabilities": map[string]any{"tools": []string{"hello.echo@1.0.0"}, "models": []any{}, "memory": []any{}, "secrets": []any{}},
		"budget":       map[string]any{"tokens": 2000, "costUsd": 0.10, "toolCalls": 8, "wallSeconds": 120}, "checkpoint": map[string]any{"mode": "logical", "schemaVersion": "hello/v1", "intervalSeconds": 30},
	})
	if _, err := h.store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: devopsTenant, Namespace: "default",
		Name: "oci-agent", Version: "1.0.0", Spec: ociAgentSpec,
	}); err != nil {
		t.Fatalf("publish oci agent: %v", err)
	}

	// Build the OCI worker binary and start it as a subprocess.
	ociWorkerBin := filepath.Join("..", "..", "..", "..", "bin", "fenced-runtime-oci")
	if _, err := os.Stat(ociWorkerBin); err != nil {
		t.Skipf("fenced-runtime-oci binary not found at %s (build with: go build ./cmd/fenced-runtime-oci)", ociWorkerBin)
	}
	artifactRoot := ociArtifactRoot(t)
	workerBin, workerArgs := ociWorkerCommand(ociWorkerBin,
		"-control-address", h.listener.Addr().String(),
		"-tenant", devopsTenant,
		"-runtime-instance-id", "oci-worker-1",
		"-artifact-root", artifactRoot,
		"-image-ref", ociImageName,
		"-skip-image-pull",
		"-dev-mode",
	)
	// When the containerd shim path is unavailable (e.g. WSL2), the
	// environment variable FENCED_RUNSC_DIRECT switches to the direct runsc
	// executor which bypasses containerd.
	if os.Getenv("FENCED_RUNSC_DIRECT") == "1" {
		workerArgs = append(workerArgs, "-runsc-direct", "-runsc-platform", "kvm")
	}
	workerCmd := exec.Command(workerBin, workerArgs...)
	workerCmd.Stderr = os.Stderr
	if err := workerCmd.Start(); err != nil {
		t.Fatalf("start oci worker: %v", err)
	}
	defer workerCmd.Process.Kill()
	t.Logf("oci worker started (pid %d)", workerCmd.Process.Pid)

	// Create a task targeting the oci pool.
	taskID := uuid.New()
	spec := map[string]any{
		"priority": 50,
		"budget":   map[string]any{"tokens": 1000, "costUsd": 0.10, "toolCalls": 4, "wallSeconds": 60},
		"placement": map[string]any{
			"runtimeClasses": []string{"oci"},
			"preferredClass": "oci",
			"region":         "cn-east",
			"cpuMillis":      250,
			"memoryMiB":      128,
			"workspaceBytes": 8388608,
			"llmConcurrency": 1,
		},
		"retryPolicy": map[string]any{"maxAttempts": 3},
	}
	specJSON, _ := json.Marshal(spec)
	if _, err := h.store.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: taskID, TenantID: devopsTenant, Namespace: "default",
		AgentVersionRef: "oci-agent@1.0.0", Goal: "oci isolation test",
		Spec: specJSON, IdempotencyKey: "oci-iso/" + taskID.String(),
	}); err != nil {
		t.Fatalf("create oci task: %v", err)
	}

	// Wait for terminal state.
	var phase string
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		if err := h.pool.QueryRow(ctx, `SELECT phase FROM tasks WHERE id = $1`, taskID).Scan(&phase); err != nil {
			t.Fatalf("get task phase: %v", err)
		}
		if phase == "SUCCEEDED" || phase == "FAILED" || phase == "CANCELLED" || phase == "TIMED_OUT" || phase == "REJECTED" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if phase != "SUCCEEDED" {
		var failureCode, failureMessage string
		if err := h.pool.QueryRow(ctx,
			`SELECT COALESCE(a.failure_code,''), COALESCE(a.failure_message,'') FROM attempts a
			 JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
			 WHERE r.task_id=$1 ORDER BY a.ordinal DESC LIMIT 1`, taskID).Scan(&failureCode, &failureMessage); err == nil {
			t.Fatalf("oci task phase = %s, want SUCCEEDED (failure=%s %s)", phase, failureCode, failureMessage)
		}
		t.Fatalf("oci task phase = %s, want SUCCEEDED", phase)
	}

	// Verify the attempt ran on the oci worker.
	var attemptClass, attemptInstance string
	if err := h.pool.QueryRow(ctx,
		`SELECT a.runtime_class, a.runtime_instance_id FROM attempts a
		 JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
		 WHERE r.task_id = $1 ORDER BY a.ordinal DESC LIMIT 1`,
		taskID).Scan(&attemptClass, &attemptInstance); err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if attemptClass != "oci" || attemptInstance != "oci-worker-1" {
		t.Fatalf("attempt = %s/%s, want oci/oci-worker-1", attemptClass, attemptInstance)
	}
	t.Logf("OCI isolation verified: task SUCCEEDED on %s (%s) inside gVisor sandbox", attemptInstance, attemptClass)
}

// TestOCICrossClassPlacement proves the kernel places a dual-class task on the
// OCI/gVisor pool when it is available, on the adapter pool when the OCI pool
// is cordoned, and back on the OCI pool when it is restored. (Mid-flight
// takeover is not exercised here: the echo container completes in
// milliseconds, so a cordon after placement would race the terminal state.)
func TestOCICrossClassPlacement(t *testing.T) {
	requireOCIDrillEnvironment(t)
	h := newHarness(t, "oci-placement", false)
	ctx := context.Background()

	// Dual-class agent: oci (container) + network (adapter).
	dualSpec, _ := json.Marshal(map[string]any{
		"runtimeClassPolicy": map[string]any{
			"allowed": []string{"oci", "research-network"}, "preferred": "oci",
		},
		"lifecycle": map[string]any{"maxAttempts": 3},
		"runtimes": []any{
			map[string]any{"class": "oci", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.oci/v1", "entrypoint": []string{"oci://agent-runtime"}},
			map[string]any{"class": "research-network", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.adapter-http/v1", "entrypoint": []string{"fenced-binding://devops"}},
		},
		"capabilities": map[string]any{"tools": []string{"hello.echo@1.0.0"}, "models": []any{}, "memory": []any{}, "secrets": []any{}},
		"budget":       map[string]any{"tokens": 2000, "costUsd": 0.10, "toolCalls": 8, "wallSeconds": 120},
		"checkpoint":   map[string]any{"mode": "logical", "schemaVersion": "hello/v1", "intervalSeconds": 30},
	})
	if _, err := h.store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: devopsTenant, Namespace: "default",
		Name: "oci-dual-agent", Version: "1.0.0", Spec: dualSpec,
	}); err != nil {
		t.Fatalf("publish dual agent: %v", err)
	}

	ociWorkerBin := filepath.Join("..", "..", "..", "..", "bin", "fenced-runtime-oci")
	if _, err := os.Stat(ociWorkerBin); err != nil {
		t.Skipf("fenced-runtime-oci binary not found")
	}
	workerBin, workerArgs := ociWorkerCommand(ociWorkerBin,
		"-control-address", h.listener.Addr().String(),
		"-tenant", devopsTenant,
		"-runtime-instance-id", "oci-worker-1",
		"-artifact-root", ociArtifactRoot(t),
		"-image-ref", ociImageName,
		"-skip-image-pull",
		"-dev-mode",
	)
	if os.Getenv("FENCED_RUNSC_DIRECT") == "1" {
		workerArgs = append(workerArgs, "-runsc-direct", "-runsc-platform", "kvm")
	}
	workerCmd := exec.Command(workerBin, workerArgs...)
	workerCmd.Stderr = os.Stderr
	if err := workerCmd.Start(); err != nil {
		t.Fatalf("start oci worker: %v", err)
	}
	defer workerCmd.Process.Kill()

	createDualTask := func(goal, idem string) uuid.UUID {
		t.Helper()
		taskID := uuid.New()
		spec := map[string]any{
			"priority": 50,
			"budget":   map[string]any{"tokens": 2000, "costUsd": 0.10, "toolCalls": 8, "wallSeconds": 120},
			"placement": map[string]any{
				"runtimeClasses": []string{"oci", "research-network"},
				"preferredClass": "oci",
				"region":         "cn-east",
				"cpuMillis":      250,
				"memoryMiB":      128,
				"workspaceBytes": 8388608,
				"llmConcurrency": 1,
			},
			"retryPolicy": map[string]any{"maxAttempts": 3},
		}
		specJSON, _ := json.Marshal(spec)
		if _, err := h.store.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: taskID, TenantID: devopsTenant, Namespace: "default",
			AgentVersionRef: "oci-dual-agent@1.0.0", Goal: goal,
			Spec: specJSON, IdempotencyKey: idem,
		}); err != nil {
			t.Fatalf("create dual task: %v", err)
		}
		return taskID
	}

	awaitClass := func(taskID uuid.UUID, wantClass string) (phase, class, instance string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for time.Now().Before(deadline) {
			if err := h.pool.QueryRow(ctx,
				`SELECT t.phase, COALESCE(a.runtime_class,''), COALESCE(a.runtime_instance_id,'') FROM tasks t
				 LEFT JOIN runs r ON r.task_id=t.id AND r.tenant_id=t.tenant_id
				 LEFT JOIN attempts a ON a.run_id=r.id AND a.tenant_id=r.tenant_id
				 WHERE t.id = $1 ORDER BY a.ordinal DESC LIMIT 1`, taskID).Scan(&phase, &class, &instance); err != nil {
				t.Fatalf("get task state: %v", err)
			}
			if phase == "SUCCEEDED" || phase == "FAILED" || phase == "CANCELLED" || phase == "TIMED_OUT" || phase == "REJECTED" {
				if phase != "SUCCEEDED" {
					var failureCode, failureMessage string
					if err := h.pool.QueryRow(ctx,
						`SELECT COALESCE(a.failure_code,''), COALESCE(a.failure_message,'') FROM attempts a
						 JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
						 WHERE r.task_id=$1 ORDER BY a.ordinal DESC LIMIT 1`, taskID).Scan(&failureCode, &failureMessage); err == nil {
						t.Fatalf("task %s phase = %s, want SUCCEEDED (failure=%s %s)", taskID, phase, failureCode, failureMessage)
					}
					t.Fatalf("task %s phase = %s, want SUCCEEDED", taskID, phase)
				}
				if class != wantClass {
					t.Fatalf("task %s class = %s, want %s", taskID, class, wantClass)
				}
				return phase, class, instance
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("task %s did not settle", taskID)
		return "", "", ""
	}

	task1 := createDualTask("oci placement: preferred", "oci-placement/preferred")
	_, class1, instance1 := awaitClass(task1, "oci")
	t.Logf("preferred placement: %s on %s (oci/gVisor)", class1, instance1)

	h.cordonPool("devops-pool-5")
	task2 := createDualTask("oci placement: cordoned", "oci-placement/cordoned")
	_, class2, instance2 := awaitClass(task2, "research-network")
	t.Logf("cross-class placement: %s on %s (research-network, oci cordoned)", class2, instance2)

	h.uncordonPool("devops-pool-5")
	task3 := createDualTask("oci placement: restored", "oci-placement/restored")
	_, class3, instance3 := awaitClass(task3, "oci")
	t.Logf("restored placement: %s on %s (oci/gVisor)", class3, instance3)
}

// TestOCIRealTakeover proves true mid-flight takeover: a long-running workload
// (sleep 30) is placed on the OCI/gVisor pool, the worker is killed mid-run,
// the lease lapses, and another runtime reclaims and completes the task.
// This is the production recovery scenario the pre-cordon placement test cannot
// cover.
func TestOCIRealTakeover(t *testing.T) {
	requireOCIDrillEnvironment(t)
	h := newHarness(t, "oci-takeover-real", false)
	ctx := context.Background()

	// Dual-class agent: oci (container) + network (adapter).
	dualSpec, _ := json.Marshal(map[string]any{
		"runtimeClassPolicy": map[string]any{
			"allowed": []string{"oci", "research-network"}, "preferred": "oci",
		},
		"lifecycle": map[string]any{"maxAttempts": 3},
		"runtimes": []any{
			map[string]any{"class": "oci", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.oci/v1", "entrypoint": []string{"oci://agent-runtime"}},
			map[string]any{"class": "research-network", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.adapter-http/v1", "entrypoint": []string{"fenced-binding://devops"}},
		},
		"capabilities": map[string]any{"tools": []string{"hello.echo@1.0.0"}, "models": []any{}, "memory": []any{}, "secrets": []any{}},
		"budget":       map[string]any{"tokens": 2000, "costUsd": 0.10, "toolCalls": 8, "wallSeconds": 120},
		"checkpoint":   map[string]any{"mode": "logical", "schemaVersion": "hello/v1", "intervalSeconds": 30},
	})
	if _, err := h.store.CreateAgentVersion(ctx, kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: devopsTenant, Namespace: "default",
		Name: "oci-dual-agent", Version: "1.0.0", Spec: dualSpec,
	}); err != nil {
		t.Fatalf("publish dual agent: %v", err)
	}

	ociWorkerBin := filepath.Join("..", "..", "..", "..", "bin", "fenced-runtime-oci")
	if _, err := os.Stat(ociWorkerBin); err != nil {
		t.Skipf("fenced-runtime-oci binary not found")
	}
	// Long-running workload: sleep 30 in the alpine image. The worker is
	// configured with the same alpine reference so the workload pin matches
	// (worker.go rejects a workload image that differs from its own config).
	const sleepImage = "docker.io/library/alpine:latest"
	workerBin, workerArgs := ociWorkerCommand(ociWorkerBin,
		"-control-address", h.listener.Addr().String(),
		"-tenant", devopsTenant,
		"-runtime-instance-id", "oci-worker-1",
		"-artifact-root", ociArtifactRoot(t),
		"-image-ref", sleepImage,
		"-skip-image-pull",
		"-dev-mode",
	)
	if os.Getenv("FENCED_RUNSC_DIRECT") == "1" {
		workerArgs = append(workerArgs, "-runsc-direct", "-runsc-platform", "kvm")
	}
	workerCmd := exec.Command(workerBin, workerArgs...)
	workerCmd.Stderr = os.Stderr
	workerCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := workerCmd.Start(); err != nil {
		t.Fatalf("start oci worker: %v", err)
	}
	defer func() { _ = syscall.Kill(-workerCmd.Process.Pid, syscall.SIGKILL) }()

	taskID := uuid.New()
	spec := map[string]any{
		"priority": 50,
		"budget":   map[string]any{"tokens": 2000, "costUsd": 0.10, "toolCalls": 8, "wallSeconds": 120},
		"image":    map[string]any{"ref": sleepImage},
		"runtime":  map[string]any{"command": []string{"sleep", "30"}},
		"placement": map[string]any{
			"runtimeClasses": []string{"oci", "research-network"},
			"preferredClass": "oci",
			"region":         "cn-east",
			"cpuMillis":      250,
			"memoryMiB":      128,
			"workspaceBytes": 8388608,
			"llmConcurrency": 1,
		},
		"retryPolicy": map[string]any{"maxAttempts": 3},
	}
	specJSON, _ := json.Marshal(spec)
	if _, err := h.store.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: taskID, TenantID: devopsTenant, Namespace: "default",
		AgentVersionRef: "oci-dual-agent@1.0.0", Goal: "oci real takeover test",
		Spec: specJSON, IdempotencyKey: "oci-takeover-real/" + taskID.String(),
	}); err != nil {
		t.Fatalf("create takeover task: %v", err)
	}

	// Wait until the attempt is RUNNING on the oci worker (mid-flight).
	runningDeadline := time.Now().Add(90 * time.Second)
	var runningPhase string
	for {
		if err := h.pool.QueryRow(ctx,
			`SELECT COALESCE(a.phase, '') FROM attempts a
			 JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
			 WHERE r.task_id = $1 AND a.runtime_instance_id = 'oci-worker-1'
			 ORDER BY a.ordinal DESC LIMIT 1`, taskID).Scan(&runningPhase); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("get attempt phase: %v", err)
		}
		if runningPhase == "RUNNING" {
			break
		}
		if runningPhase == "FAILED" || runningPhase == "SUCCEEDED" {
			t.Fatalf("task reached %s before takeover could start", runningPhase)
		}
		if time.Now().After(runningDeadline) {
			t.Fatalf("task never reached RUNNING on oci worker (phase=%s)", runningPhase)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("task RUNNING on oci worker (gVisor), starting takeover")

	// Cordon the oci pool and kill the worker mid-run via its process group
	// (covers the sudo wrapper and the real worker child).
	h.cordonPool("devops-pool-5")
	_ = syscall.Kill(-workerCmd.Process.Pid, syscall.SIGKILL)
	killTime := time.Now()

	// Expire the lease immediately so recovery reclaims the attempt.
	if _, err := h.pool.Exec(ctx, `
		UPDATE runtime_leases l SET expires_at = l.acquired_at + INTERVAL '1 microsecond'
		FROM attempts a
		JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
		WHERE a.id = l.attempt_id AND r.task_id = $1 AND l.released_at IS NULL`, taskID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// Wait for terminal; the task must be re-claimed by another runtime.
	// Note: recovery creates a new run (not a higher ordinal on the same run),
	// so we order by attempt updated_at DESC to get the most recent attempt.
	var phase, finalClass, finalInstance string
	recoverDeadline := time.Now().Add(2 * time.Minute)
	for {
		if err := h.pool.QueryRow(ctx,
			`SELECT t.phase, COALESCE(a.runtime_class,''), COALESCE(a.runtime_instance_id,'') FROM tasks t
				 LEFT JOIN runs r ON r.task_id=t.id AND r.tenant_id=t.tenant_id
				 LEFT JOIN attempts a ON a.run_id=r.id AND a.tenant_id=r.tenant_id
				 WHERE t.id = $1 ORDER BY a.updated_at DESC LIMIT 1`, taskID).Scan(&phase, &finalClass, &finalInstance); err != nil {
			t.Fatalf("get task state: %v", err)
		}
		if phase == "SUCCEEDED" || phase == "FAILED" || phase == "CANCELLED" || phase == "TIMED_OUT" || phase == "REJECTED" {
			break
		}
		if time.Now().After(recoverDeadline) {
			t.Fatalf("oci takeover did not converge (phase=%s)", phase)
		}
		time.Sleep(200 * time.Millisecond)
	}
	takeoverLatency := time.Since(killTime)

	if phase != "SUCCEEDED" {
		dumpTakeoverState(t, h.pool, taskID)
		var failureCode, failureMessage string
		if err := h.pool.QueryRow(ctx,
			`SELECT COALESCE(a.failure_code,''), COALESCE(a.failure_message,'') FROM attempts a
			 JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
			 WHERE r.task_id=$1 ORDER BY a.updated_at DESC LIMIT 1`, taskID).Scan(&failureCode, &failureMessage); err == nil {
			t.Fatalf("takeover: task phase = %s, want SUCCEEDED (failure=%s %s)", phase, failureCode, failureMessage)
		}
		t.Fatalf("takeover: task phase = %s, want SUCCEEDED", phase)
	}
	if finalClass == "oci" {
		dumpTakeoverState(t, h.pool, taskID)
		t.Fatalf("takeover failed: final attempt still on oci (%s)", finalInstance)
	}

	// Acceptance criteria (plan §4): Lost Task=0, Duplicate Final Result=0,
	// Stuck Task=0, plus recorded takeover/lease-expiry/recovery timing.
	// Lost/Stuck are implied by SUCCEEDED within the deadline above. Verify
	// exactly one attempt produced a final result (no duplicate), and that the
	// original gVisor attempt was fenced out with LEASE_EXPIRED.
	var completedCount int
	var leakedResult string
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE a.phase IN ('COMPLETED','SUCCEEDED')),
		       COALESCE(string_agg(a.runtime_instance_id, ',') FILTER (WHERE a.phase IN ('COMPLETED','SUCCEEDED')), '')
		FROM attempts a JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
		WHERE r.task_id=$1`, taskID).Scan(&completedCount, &leakedResult); err != nil {
		t.Fatalf("count final results: %v", err)
	}
	if completedCount != 1 {
		dumpTakeoverState(t, h.pool, taskID)
		t.Fatalf("duplicate/lost final result: %d attempts COMPLETED/SUCCEEDED (%s), want exactly 1", completedCount, leakedResult)
	}
	var leaseExpiredAttempts int
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM attempts a JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
		WHERE r.task_id=$1 AND a.failure_code='LEASE_EXPIRED'`, taskID).Scan(&leaseExpiredAttempts); err != nil {
		t.Fatalf("count lease-expired attempts: %v", err)
	}
	if leaseExpiredAttempts == 0 {
		dumpTakeoverState(t, h.pool, taskID)
		t.Fatalf("original gVisor attempt was not fenced out by lease expiry")
	}
	// Record lease-expiry recovery timing: when recovery released the original
	// lease (fencing) and how long the replacement attempt took to reconverge.
	var leaseReleasedAt time.Time
	if err := h.pool.QueryRow(ctx, `
			SELECT l.released_at
			FROM attempts a
			JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
			JOIN runtime_leases l ON l.attempt_id = a.id
			WHERE r.task_id=$1 AND a.failure_code='LEASE_EXPIRED' AND l.released_at IS NOT NULL
			ORDER BY a.updated_at DESC LIMIT 1`, taskID).Scan(&leaseReleasedAt); err == nil && !leaseReleasedAt.IsZero() {
		terminalAt := killTime.Add(takeoverLatency)
		t.Logf("leaseExpiry: worker killed %s, lease fenced %s (leaseExpiration=%s), reconverged %s (recoveryTime=%s)",
			killTime.Format("15:04:05.000"), leaseReleasedAt.Format("15:04:05.000"),
			leaseReleasedAt.Sub(killTime).Round(time.Millisecond),
			terminalAt.Format("15:04:05.000"),
			terminalAt.Sub(leaseReleasedAt).Round(time.Millisecond))
	}
	t.Logf("acceptance: Lost=0 Duplicate=0 Stuck=0 (original gVisor attempt fenced with LEASE_EXPIRED)")
	t.Logf("OCI real takeover verified: gVisor(oci) → %s (%s), task SUCCEEDED, takeoverLatency=%s",
		finalClass, finalInstance, takeoverLatency)
}

// dumpTakeoverState prints the full run/attempt/lease history for a takeover
// investigation, so a failed assertion shows exactly which branch recovery took.
func dumpTakeoverState(t *testing.T, pool *pgxpool.Pool, taskID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	t.Logf("--- takeover state dump for task %s ---", taskID)
	rows, err := pool.Query(ctx, `
		SELECT a.ordinal, a.phase, COALESCE(a.runtime_class,''), COALESCE(a.runtime_instance_id,''),
		       COALESCE(a.failure_code,''), COALESCE(a.failure_message,''),
		       to_char(a.created_at, 'HH24:MI:SS.US'), to_char(a.updated_at, 'HH24:MI:SS.US')
		FROM attempts a
		JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
		WHERE r.task_id=$1 ORDER BY a.ordinal ASC`, taskID)
	if err != nil {
		t.Logf("dump attempts: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ord, phase, class, inst, fcode, fmsg, created, updated string
		if err := rows.Scan(&ord, &phase, &class, &inst, &fcode, &fmsg, &created, &updated); err != nil {
			t.Logf("dump scan: %v", err)
			return
		}
		t.Logf("attempt ordinal=%s phase=%s class=%s instance=%s failure=%s/%s created=%s updated=%s",
			ord, phase, class, inst, fcode, fmsg, created, updated)
	}
	leaseRows, err := pool.Query(ctx, `
		SELECT l.attempt_id, l.released_at IS NOT NULL, COALESCE(l.release_reason,''), l.expires_at, l.acquired_at
		FROM runtime_leases l
		JOIN attempts a ON a.id = l.attempt_id
		JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
		WHERE r.task_id=$1 ORDER BY l.acquired_at ASC`, taskID)
	if err != nil {
		t.Logf("dump leases: %v", err)
		return
	}
	defer leaseRows.Close()
	for leaseRows.Next() {
		var attemptID uuid.UUID
		var released bool
		var reason string
		var expiresAt, acquiredAt time.Time
		if err := leaseRows.Scan(&attemptID, &released, &reason, &expiresAt, &acquiredAt); err != nil {
			t.Logf("lease scan: %v", err)
			return
		}
		t.Logf("lease attempt=%s released=%v reason=%s expires=%s acquired=%s",
			attemptID, released, reason, expiresAt.Format("15:04:05.000"), acquiredAt.Format("15:04:05.000"))
	}
	t.Logf("--- end takeover state dump ---")
}

var _ = strings.TrimSpace
