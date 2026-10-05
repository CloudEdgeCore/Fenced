package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type nativeRuntime struct {
	mu     sync.RWMutex
	states map[string]json.RawMessage
}

func (r *nativeRuntime) Run(ctx context.Context, request agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	if err := emit("native.started", json.RawMessage("{\"adapter\":\"go-native\"}")); err != nil {
		return nil, err
	}
	var input map[string]any
	if err := json.Unmarshal(request.Input, &input); err != nil {
		return nil, err
	}
	if block, _ := input["blockUntilStopped"].(bool); block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	output, err := json.Marshal(map[string]any{"goal": request.Goal, "input": input, "sdk": "go"})
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.states[request.ExecutionID] = output
	r.mu.Unlock()
	return output, nil
}

func (r *nativeRuntime) Checkpoint(_ context.Context, executionID string) (agent.Checkpoint, error) {
	r.mu.RLock()
	state := r.states[executionID]
	r.mu.RUnlock()
	if len(state) == 0 {
		return agent.Checkpoint{}, fmt.Errorf("execution %s has no state", executionID)
	}
	return agent.Checkpoint{SchemaVersion: "go-native/v1", State: state, CreatedAt: time.Now().UTC()}, nil
}

func (r *nativeRuntime) Restore(_ context.Context, request agent.RestoreRequest) error {
	if request.Checkpoint.SchemaVersion != "go-native/v1" {
		return fmt.Errorf("incompatible checkpoint schema %q", request.Checkpoint.SchemaVersion)
	}
	r.mu.Lock()
	r.states[request.ExecutionID] = request.Checkpoint.State
	r.mu.Unlock()
	return nil
}

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	listen := flag.String("listen", "127.0.0.1:8087", "Runtime Interface listen address")
	flag.Parse()
	host, err := agent.NewHost(&nativeRuntime{states: map[string]json.RawMessage{}}, agent.HostOptions{Adapter: "go-native"})
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("http://%s\n", listener.Addr())
	if err := http.Serve(listener, host); err != nil {
		log.Fatal(err)
	}
}
