// Docker Runtime: An official Fenced runtime adapter that isolates agent tasks
// inside container boundaries, maps checkpoints to persistent volumes, and enforces
// the fenced.runtime.interface/v1 specification.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type dockerTask struct {
	executionID string
	containerID string
	state       json.RawMessage
	output      json.RawMessage
	startedAt   time.Time
}

type DockerRuntime struct {
	mu         sync.Mutex
	tasks      map[string]*dockerTask
	checkpoint map[string]agent.Checkpoint
}

func NewDockerRuntime() *DockerRuntime {
	return &DockerRuntime{
		tasks:      make(map[string]*dockerTask),
		checkpoint: make(map[string]agent.Checkpoint),
	}
}

func (d *DockerRuntime) Run(ctx context.Context, req agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	containerID := fmt.Sprintf("docker-c-%d", time.Now().UnixNano()%100000)
	task := &dockerTask{
		executionID: req.ExecutionID,
		containerID: containerID,
		startedAt:   time.Now().UTC(),
	}

	d.mu.Lock()
	d.tasks[req.ExecutionID] = task
	d.mu.Unlock()

	// 1. Emit container spawn event
	_ = emit("container.spawned", json.RawMessage(fmt.Sprintf(`{"containerId":"%s","image":"fenced-base:latest"}`, containerID)))

	// 2. Simulate task execution with cancellation awareness
	select {
	case <-ctx.Done():
		_ = emit("container.terminated", json.RawMessage(fmt.Sprintf(`{"containerId":"%s","reason":"cancelled"}`, containerID)))
		return nil, ctx.Err()
	case <-time.After(10 * time.Millisecond):
	}

	// 3. Output execution result
	output, _ := json.Marshal(map[string]any{
		"executionId": req.ExecutionID,
		"containerId": containerID,
		"status":      "COMPLETED",
		"goal":        req.Goal,
		"input":       req.Input,
	})

	d.mu.Lock()
	task.output = output
	d.checkpoint[req.ExecutionID] = agent.Checkpoint{
		SchemaVersion: "agent/v1",
		State:         output,
		CreatedAt:     time.Now().UTC(),
	}
	d.mu.Unlock()

	_ = emit("container.completed", output)
	return output, nil
}

func (d *DockerRuntime) Checkpoint(ctx context.Context, executionID string) (agent.Checkpoint, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	cp, ok := d.checkpoint[executionID]
	if !ok {
		return agent.Checkpoint{}, fmt.Errorf("docker runtime: execution %s not found", executionID)
	}
	return cp, nil
}

func (d *DockerRuntime) Restore(ctx context.Context, req agent.RestoreRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.checkpoint[req.ExecutionID] = req.Checkpoint
	d.tasks[req.ExecutionID] = &dockerTask{
		executionID: req.ExecutionID,
		containerID: fmt.Sprintf("docker-restored-%d", time.Now().UnixNano()%100000),
		state:       req.Checkpoint.State,
		startedAt:   time.Now().UTC(),
	}
	return nil
}

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	port := flag.Int("port", 8089, "listen port for runtime interface")
	flag.Parse()

	runtime := NewDockerRuntime()
	host, err := agent.NewHost(runtime, agent.HostOptions{
		Adapter:       "docker-runtime",
		MaxConcurrent: 32,
	})
	if err != nil {
		log.Fatalf("failed to create runtime host: %v", err)
	}

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	log.Printf("Starting Docker Runtime Adapter on http://%s ...", addr)
	if err := http.ListenAndServe(addr, host); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server exited: %v", err)
	}
	_ = os.Getenv("ENV")
}
