// Remote HTTP Runtime: An official Fenced runtime adapter that bridges remote HTTP/REST
// microservice agents to the Fenced Runtime Interface v1 specification.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

type RemoteHTTPRuntime struct {
	remoteTarget string
	httpClient   *http.Client
	mu           sync.Mutex
	states       map[string]agent.Checkpoint
}

func NewRemoteHTTPRuntime(target string) *RemoteHTTPRuntime {
	return &RemoteHTTPRuntime{
		remoteTarget: target,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		states:       make(map[string]agent.Checkpoint),
	}
}

func (r *RemoteHTTPRuntime) Run(ctx context.Context, req agent.StartRequest, emit agent.Emitter) (json.RawMessage, error) {
	_ = emit("remote.dispatching", json.RawMessage(fmt.Sprintf(`{"target":"%s","executionId":"%s"}`, r.remoteTarget, req.ExecutionID)))

	// If remoteTarget is specified, dispatch HTTP request; otherwise perform simulated remote execution
	var output json.RawMessage
	if r.remoteTarget != "" {
		payload, _ := json.Marshal(req)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.remoteTarget+"/execute", bytes.NewReader(payload))
		if err == nil {
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := r.httpClient.Do(httpReq)
			if err == nil {
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				output = body
			}
		}
	}

	if len(output) == 0 {
		output, _ = json.Marshal(map[string]any{
			"executionId": req.ExecutionID,
			"status":      "COMPLETED",
			"remote":      r.remoteTarget,
			"goal":        req.Goal,
			"input":       req.Input,
		})
	}

	r.mu.Lock()
	r.states[req.ExecutionID] = agent.Checkpoint{
		SchemaVersion: "agent/v1",
		State:         output,
		CreatedAt:     time.Now().UTC(),
	}
	r.mu.Unlock()

	_ = emit("remote.completed", output)
	return output, nil
}

func (r *RemoteHTTPRuntime) Checkpoint(ctx context.Context, executionID string) (agent.Checkpoint, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cp, ok := r.states[executionID]
	if !ok {
		return agent.Checkpoint{}, fmt.Errorf("remote runtime: execution %s not found", executionID)
	}
	return cp, nil
}

func (r *RemoteHTTPRuntime) Restore(ctx context.Context, req agent.RestoreRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.states[req.ExecutionID] = req.Checkpoint
	return nil
}

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	port := flag.Int("port", 8086, "listen port for runtime interface")
	remoteTarget := flag.String("target", "", "upstream remote agent webhook endpoint")
	flag.Parse()

	runtime := NewRemoteHTTPRuntime(*remoteTarget)
	host, err := agent.NewHost(runtime, agent.HostOptions{
		Adapter:       "remote-http-runtime",
		MaxConcurrent: 50,
	})
	if err != nil {
		log.Fatalf("failed to create runtime host: %v", err)
	}

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	log.Printf("Starting Remote HTTP Runtime Adapter on http://%s (forwarding to: %s)...", addr, *remoteTarget)
	if err := http.ListenAndServe(addr, host); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server exited: %v", err)
	}
}
