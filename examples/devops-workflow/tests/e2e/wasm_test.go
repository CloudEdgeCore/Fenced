//go:build integration

package devops_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

// resolveBinary resolves a tool binary, appending the platform extension when
// present (Windows executables carry .exe; CI is Linux).
func resolveBinary(t *testing.T, base string) string {
	t.Helper()
	plain := base
	if _, err := os.Stat(plain); err == nil {
		return plain
	}
	withExt := base + ".exe"
	if _, err := os.Stat(withExt); err == nil {
		return withExt
	}
	t.Skipf("binary not found at %s or %s (build with cargo build --release)", plain, withExt)
	return ""
}

// publishWasmAgent publishes a dual-class agent (wasm + adapter).
func (h *harness) publishWasmAgent(t *testing.T, name string) {
	t.Helper()
	spec, _ := json.Marshal(map[string]any{
		"runtimeClassPolicy": map[string]any{"allowed": []string{"research-wasm", "research-network"}, "preferred": "research-wasm"},
		"lifecycle":          map[string]any{"maxAttempts": 3},
		"runtimes": []any{
			map[string]any{"class": "research-wasm", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.wasm-component/v1", "entrypoint": []string{"fenced-binding://devops"}},
			map[string]any{"class": "research-network", "interface": "fenced.runtime.interface/v1", "runtimeABI": "fenced.adapter-http/v1", "entrypoint": []string{"fenced-binding://devops"}},
		},
		"capabilities": map[string]any{"tools": []string{"hello.echo@1.0.0"}, "models": []any{}, "memory": []any{}, "secrets": []any{}},
		"budget":       map[string]any{"tokens": 2000, "costUsd": 0.10, "toolCalls": 8, "wallSeconds": 120},
		"checkpoint":   map[string]any{"mode": "logical", "schemaVersion": "hello/v1", "intervalSeconds": 30},
	})
	if _, err := h.store.CreateAgentVersion(context.Background(), kernelstore.CreateAgentVersionInput{
		ID: uuid.New(), TenantID: devopsTenant, Namespace: "default",
		Name: name, Version: "1.0.0", Spec: spec,
	}); err != nil {
		t.Fatalf("publish wasm agent: %v", err)
	}
}

// startWasmWorker starts the real Wasmtime worker as a subprocess.
func (h *harness) startWasmWorker(t *testing.T, packageRoot string) *exec.Cmd {
	t.Helper()
	workerBin := resolveBinary(t, filepath.Join("..", "..", "..", "..", "target", "release", "fenced-runtime-wasm"))
	cmd := exec.Command(workerBin,
		"--control-endpoint", h.listener.Addr().String(),
		"--tenant", devopsTenant,
		"--runtime-instance-id", "wasm-worker-1",
		"--package-root", packageRoot,
		"--artifact-root", t.TempDir(),
		"--dev-mode", "true",
	)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start wasm worker: %v", err)
	}
	return cmd
}

// genWasmComponent generates the WASM conformance component into packageRoot.
func genWasmComponent(t *testing.T, packageRoot string) {
	t.Helper()
	binPath := resolveBinary(t, filepath.Join("..", "..", "..", "..", "target", "release", "component-fixture"))
	wasmPath := filepath.Join(packageRoot, "hello.wasm")
	if out, err := exec.Command(binPath, "--out", wasmPath).CombinedOutput(); err != nil {
		t.Fatalf("generate wasm: %v\n%s", err, out)
	}
}

// TestWasmtimeIsolation is the B-path isolation drill: a task whose spec
// targets a Wasmtime component is placed on the `research-wasm` pool,
// executed by the real fenced-runtime-wasm worker inside a Wasmtime
// sandbox, and completes successfully.
func TestWasmtimeIsolation(t *testing.T) {
	h := newHarness(t, "wasm-iso", false)
	ctx := context.Background()

	packageRoot := t.TempDir()
	genWasmComponent(t, packageRoot)
	workerCmd := h.startWasmWorker(t, packageRoot)
	defer workerCmd.Process.Kill()
	h.publishWasmAgent(t, "wasm-agent")
	t.Logf("wasm worker started (control %s)", h.listener.Addr().String())

	taskID := uuid.New()
	spec := map[string]any{
		"priority": 50,
		"budget":   map[string]any{"tokens": 1000, "costUsd": 0.10, "toolCalls": 4, "wallSeconds": 60},
		"placement": map[string]any{
			"runtimeClasses": []string{"research-wasm"},
			"preferredClass": "research-wasm",
			"region":         "cn-east",
			"cpuMillis":      250,
			"memoryMiB":      256,
			"workspaceBytes": 8388608,
			"llmConcurrency": 1,
		},
		"runtime":     map[string]any{"componentPath": "hello.wasm"},
		"retryPolicy": map[string]any{"maxAttempts": 3},
	}
	specJSON, _ := json.Marshal(spec)
	if _, err := h.store.CreateTask(ctx, kernelstore.CreateTaskInput{
		ID: taskID, TenantID: devopsTenant, Namespace: "default",
		AgentVersionRef: "wasm-agent@1.0.0", Goal: "wasm isolation test",
		Spec: specJSON, IdempotencyKey: "wasm-iso/" + taskID.String(),
	}); err != nil {
		t.Fatalf("create wasm task: %v", err)
	}

	var phase string
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		if err := h.pool.QueryRow(ctx, `SELECT phase FROM tasks WHERE id = $1`, taskID).Scan(&phase); err != nil {
			t.Fatalf("get task phase: %v", err)
		}
		if phase == "SUCCEEDED" || phase == "FAILED" || phase == "CANCELLED" || phase == "TIMED_OUT" || phase == "REJECTED" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if phase != "SUCCEEDED" {
		t.Fatalf("wasm task phase = %s, want SUCCEEDED", phase)
	}

	var attemptClass, attemptInstance string
	if err := h.pool.QueryRow(ctx,
		`SELECT a.runtime_class, a.runtime_instance_id FROM attempts a
		 JOIN runs r ON r.id = a.run_id AND r.tenant_id = a.tenant_id
		 WHERE r.task_id = $1 ORDER BY a.ordinal DESC LIMIT 1`,
		taskID).Scan(&attemptClass, &attemptInstance); err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if attemptClass != "research-wasm" || attemptInstance != "wasm-worker-1" {
		t.Fatalf("attempt = %s/%s, want research-wasm/wasm-worker-1", attemptClass, attemptInstance)
	}
	t.Logf("Wasmtime isolation verified: task SUCCEEDED on %s (%s) inside Wasmtime sandbox", attemptInstance, attemptClass)
}

// TestWasmtimeCrossClassPlacement proves the kernel places a dual-class task
// on the adapter pool when the Wasmtime pool is cordoned, then on the
// Wasmtime pool when it is restored. (Mid-flight takeover is exercised by the
// OCI/gVisor drill, where container startup gives a reliable window; the WASM
// echo component completes in milliseconds.)
func TestWasmtimeCrossClassPlacement(t *testing.T) {
	h := newHarness(t, "wasm-placement", false)
	ctx := context.Background()

	packageRoot := t.TempDir()
	genWasmComponent(t, packageRoot)
	workerCmd := h.startWasmWorker(t, packageRoot)
	defer workerCmd.Process.Kill()
	h.publishWasmAgent(t, "wasm-dual-agent")

	createDualTask := func(goal, idem string) uuid.UUID {
		t.Helper()
		taskID := uuid.New()
		spec := map[string]any{
			"priority": 50,
			"budget":   map[string]any{"tokens": 2000, "costUsd": 0.10, "toolCalls": 8, "wallSeconds": 120},
			"placement": map[string]any{
				"runtimeClasses": []string{"research-wasm", "research-network"},
				"preferredClass": "research-wasm",
				"region":         "cn-east",
				"cpuMillis":      250,
				"memoryMiB":      256,
				"workspaceBytes": 8388608,
				"llmConcurrency": 1,
			},
			"runtime":     map[string]any{"componentPath": "hello.wasm"},
			"retryPolicy": map[string]any{"maxAttempts": 3},
		}
		specJSON, _ := json.Marshal(spec)
		if _, err := h.store.CreateTask(ctx, kernelstore.CreateTaskInput{
			ID: taskID, TenantID: devopsTenant, Namespace: "default",
			AgentVersionRef: "wasm-dual-agent@1.0.0", Goal: goal,
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
					var failureCode, failureReason string
					if err := h.pool.QueryRow(ctx,
						`SELECT COALESCE(a.failure_code,''), COALESCE(a.failure_message,'') FROM attempts a
						 JOIN runs r ON r.id=a.run_id AND r.tenant_id=a.tenant_id
						 WHERE r.task_id=$1 ORDER BY a.ordinal DESC LIMIT 1`, taskID).Scan(&failureCode, &failureReason); err == nil {
						t.Fatalf("task %s phase = %s, want SUCCEEDED (failure=%s %s)", taskID, phase, failureCode, failureReason)
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

	task1 := createDualTask("wasm placement: preferred", "wasm-placement/preferred")
	_, class1, instance1 := awaitClass(task1, "research-wasm")
	t.Logf("preferred placement: %s on %s (research-wasm)", class1, instance1)

	h.cordonPool("devops-pool-4")
	task2 := createDualTask("wasm placement: cordoned", "wasm-placement/cordoned")
	_, class2, instance2 := awaitClass(task2, "research-network")
	t.Logf("cross-class placement: %s on %s (research-network, wasm cordoned)", class2, instance2)

	h.uncordonPool("devops-pool-4")
	task3 := createDualTask("wasm placement: restored", "wasm-placement/restored")
	_, class3, instance3 := awaitClass(task3, "research-wasm")
	t.Logf("restored placement: %s on %s (research-wasm)", class3, instance3)
}
