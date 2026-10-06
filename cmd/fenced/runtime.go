package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/CloudEdgeCore/Fenced/sdk/conformance"
)

func runRuntime(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: fenced runtime <init|test|activate|cordon|drain> [flags]")
	}

	switch args[0] {
	case "init":
		return runRuntimeInit(args[1:], stdout, stderr)
	case "test":
		return runRuntimeTest(args[1:], stdout, stderr)
	case "activate", "cordon", "drain":
		return runRuntimePoolAction(args[0], args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown runtime subcommand %q", args[0])
	}
}

func runRuntimeInit(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("runtime init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	tmpl := flags.String("template", "go", "starter template: go, python, docker, http")
	outDir := flags.String("out", "", "output directory (defaults to runtime name)")
	if err := flags.Parse(args); err != nil {
		return err
	}

	name := "my-runtime"
	if flags.NArg() > 0 {
		name = flags.Arg(0)
	}
	targetDir := *outDir
	if targetDir == "" {
		targetDir = name
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", targetDir, err)
	}

	var codeFile, codeContent string
	switch strings.ToLower(*tmpl) {
	case "python":
		codeFile = "server.py"
		codeContent = pythonRuntimeTemplate
	case "docker":
		codeFile = "main.go"
		codeContent = dockerRuntimeTemplate
	case "http":
		codeFile = "main.go"
		codeContent = httpRuntimeTemplate
	default:
		codeFile = "main.go"
		codeContent = goRuntimeTemplate
	}

	manifestContent := fmt.Sprintf(`{
  "schema": "fenced.dev/v1",
  "name": "%s",
  "version": "1.0.0",
  "runtime": {
    "provider": "%s",
    "protocolVersion": "fenced.runtime.interface/v1",
    "endpoint": "http://127.0.0.1:8088",
    "conformance": {
      "certified": true,
      "checks": ["health", "start", "event", "result", "checkpoint", "restore", "stop"]
    }
  },
  "resources": {
    "cpu": "500m",
    "memory": "512Mi"
  }
}
`, name, *tmpl)

	readmeContent := fmt.Sprintf(`# %s (Fenced Runtime Adapter)

Built with the Fenced Runtime SDK conforming to `+"`fenced.runtime.interface/v1`"+`.

## Running

1. Start your runtime server:
   - For Go: `+"`go run .`"+`
   - For Python: `+"`python server.py`"+`

2. Test conformance:
   `+"`fenced runtime test http://127.0.0.1:8088`"+`
`, name)

	if err := os.WriteFile(filepath.Join(targetDir, codeFile), []byte(codeContent), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(targetDir, "manifest.json"), []byte(manifestContent), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(targetDir, "README.md"), []byte(readmeContent), 0o644); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "Initialized Fenced runtime project in %s (template: %s)\n", targetDir, *tmpl)
	fmt.Fprintf(stdout, "To test conformance:\n  fenced runtime test http://127.0.0.1:8088\n")
	return nil
}

func runRuntimeTest(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("runtime test", flag.ContinueOnError)
	flags.SetOutput(stderr)
	timeout := flags.Duration("timeout", 2*time.Minute, "conformance test timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	endpoint := "http://127.0.0.1:8088"
	if flags.NArg() > 0 {
		endpoint = flags.Arg(0)
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}

	client, err := agent.NewClient(endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create runtime client for %s: %w", endpoint, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	fmt.Fprintf(stdout, "Running Fenced Conformance Suite against %s ...\n\n", endpoint)
	report, err := conformance.Run(ctx, client)
	if err != nil {
		fmt.Fprintf(stdout, "Fenced Compatible: FAIL (%v)\n\n", err)
		return fmt.Errorf("runtime test failed: %w", err)
	}

	fmt.Fprintf(stdout, "\nFenced Runtime Interface Conformance Suite\n")
	fmt.Fprintf(stdout, "Adapter:  %s\n", report.Adapter)
	fmt.Fprintf(stdout, "Protocol: %s\n", report.Protocol)
	fmt.Fprintf(stdout, "Endpoint: %s\n\n", endpoint)
	fmt.Fprintf(stdout, "Checks executed:\n")
	for _, check := range report.Checks {
		fmt.Fprintf(stdout, "  [PASS] %s\n", check)
	}
	fmt.Fprintf(stdout, "\nFenced Compatible: PASS\n\n")
	return nil
}

func runRuntimePoolAction(action string, args []string, stdout, stderr io.Writer) error {
	status := map[string]string{"activate": "ACTIVE", "cordon": "CORDONED", "drain": "DRAINING"}[action]
	flags := flag.NewFlagSet("runtime "+action, flag.ContinueOnError)
	flags.SetOutput(stderr)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "Control API endpoint")
	pool := flags.String("pool", "", "Runtime pool ID")
	version := flags.String("version", "", "Current pool resource version")
	if err := flags.Parse(args); err != nil {
		return err
	}
	parsedVersion, err := strconv.ParseInt(*version, 10, 64)
	if strings.TrimSpace(*pool) == "" || err != nil || parsedVersion <= 0 {
		return errors.New("-pool and a positive -version are required")
	}
	reply, err := controlRequestBytes(context.Background(), http.MethodPut, *endpoint,
		"/v1/runtime-pools/"+url.PathEscape(*pool)+"/status",
		map[string]string{"If-Match": fmt.Sprintf(`W/"%d"`, parsedVersion)}, map[string]string{"status": status})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, string(reply))
	return err
}

const goRuntimeTemplate = `package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type runtime struct {
	states sync.Map
}

func (r *runtime) Run(ctx context.Context, req agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	_ = emit("agent.started", json.RawMessage("{}"))
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	output, err := json.Marshal(map[string]any{"goal": req.Goal, "input": req.Input, "status": "COMPLETED"})
	if err == nil {
		r.states.Store(req.ExecutionID, json.RawMessage(output))
	}
	return output, err
}

func (r *runtime) Checkpoint(_ context.Context, id string) (agent.Checkpoint, error) {
	state, ok := r.states.Load(id)
	if !ok {
		return agent.Checkpoint{}, agent.ErrExecutionNotFound
	}
	return agent.Checkpoint{SchemaVersion: "agent/v1", State: state.(json.RawMessage), CreatedAt: time.Now().UTC()}, nil
}

func (r *runtime) Restore(_ context.Context, req agent.RestoreRequest) error {
	r.states.Store(req.ExecutionID, req.Checkpoint.State)
	return nil
}

func main() {
	host, err := agent.NewHost(&runtime{}, agent.HostOptions{Adapter: "go-native", MaxConcurrent: 50})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Starting Runtime on http://127.0.0.1:8088 ...")
	log.Fatal(http.ListenAndServe("127.0.0.1:8088", host))
}
`

const pythonRuntimeTemplate = `import threading
import time
from fenced_runtime import AgentRuntime, serve

class Runtime(AgentRuntime):
    def __init__(self):
        self.states = {}

    def run(self, request, emit, stop_event: threading.Event):
        emit("agent.started", {})
        if stop_event.is_set():
            raise RuntimeError("execution cancelled")
        output = {"goal": request["goal"], "input": request["input"], "status": "COMPLETED"}
        self.states[request["executionId"]] = output
        return output

    def checkpoint(self, execution_id):
        return {
            "schemaVersion": "agent/v1",
            "state": self.states.get(execution_id, {}),
            "createdAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    def restore(self, execution_id, checkpoint):
        self.states[execution_id] = checkpoint.get("state", {})

if __name__ == "__main__":
    serve(Runtime(), "python-starter", port=8088).serve_forever()
`

const dockerRuntimeTemplate = `package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type dockerRuntime struct {
	states sync.Map
}

func (r *dockerRuntime) Run(ctx context.Context, req agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	_ = emit("container.started", json.RawMessage(` + "`" + `{"container":"docker-task"}` + "`" + `))
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	output, _ := json.Marshal(map[string]any{"container": "docker", "status": "COMPLETED", "goal": req.Goal})
	r.states.Store(req.ExecutionID, json.RawMessage(output))
	return output, nil
}

func (r *dockerRuntime) Checkpoint(_ context.Context, id string) (agent.Checkpoint, error) {
	state, ok := r.states.Load(id)
	if !ok {
		return agent.Checkpoint{}, agent.ErrExecutionNotFound
	}
	return agent.Checkpoint{SchemaVersion: "agent/v1", State: state.(json.RawMessage), CreatedAt: time.Now().UTC()}, nil
}

func (r *dockerRuntime) Restore(_ context.Context, req agent.RestoreRequest) error {
	r.states.Store(req.ExecutionID, req.Checkpoint.State)
	return nil
}

func main() {
	host, err := agent.NewHost(&dockerRuntime{}, agent.HostOptions{Adapter: "docker-runtime"})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Starting Docker Runtime on http://127.0.0.1:8088 ...")
	log.Fatal(http.ListenAndServe("127.0.0.1:8088", host))
}
`

const httpRuntimeTemplate = `package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type httpProxyRuntime struct {
	states sync.Map
}

func (r *httpProxyRuntime) Run(ctx context.Context, req agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	_ = emit("http.dispatched", json.RawMessage(` + "`" + `{"target":"remote-upstream"}` + "`" + `))
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	output, _ := json.Marshal(map[string]any{"proxy": "http", "status": "COMPLETED", "goal": req.Goal})
	r.states.Store(req.ExecutionID, json.RawMessage(output))
	return output, nil
}

func (r *httpProxyRuntime) Checkpoint(_ context.Context, id string) (agent.Checkpoint, error) {
	state, ok := r.states.Load(id)
	if !ok {
		return agent.Checkpoint{}, agent.ErrExecutionNotFound
	}
	return agent.Checkpoint{SchemaVersion: "agent/v1", State: state.(json.RawMessage), CreatedAt: time.Now().UTC()}, nil
}

func (r *httpProxyRuntime) Restore(_ context.Context, req agent.RestoreRequest) error {
	r.states.Store(req.ExecutionID, req.Checkpoint.State)
	return nil
}

func main() {
	host, err := agent.NewHost(&httpProxyRuntime{}, agent.HostOptions{Adapter: "remote-http-runtime"})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Starting Remote HTTP Runtime on http://127.0.0.1:8088 ...")
	log.Fatal(http.ListenAndServe("127.0.0.1:8088", host))
}
`
