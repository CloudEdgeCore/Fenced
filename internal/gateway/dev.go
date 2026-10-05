package gateway

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
)

// DevExecutor is the v0.1 development tool executor: it echoes the normalized
// invocation for any registered tool and enforces nothing beyond the gateway
// decision chain. It is not a sandbox and must never be deployed with real
// credentials or untrusted workloads.
type DevExecutor struct {
	MaxOutputBytes int64
}

func (e *DevExecutor) Execute(_ context.Context, request tool.ExecutionRequest) (tool.ExecutionResult, error) {
	output, err := json.Marshal(map[string]any{
		"tool": request.Descriptor.Name, "version": request.Descriptor.Version,
		"action": request.Action, "resource": request.Resource,
		"echo": json.RawMessage(request.Args),
	})
	if err != nil {
		return tool.ExecutionResult{}, fmt.Errorf("encode dev tool result: %w", err)
	}
	if e.MaxOutputBytes > 0 && int64(len(output)) > e.MaxOutputBytes {
		output = output[:e.MaxOutputBytes]
	}
	return tool.ExecutionResult{Output: output}, nil
}

// DevSecretBroker issues a fixed handle for development. It is not a real
// secret broker; the gateway redacts the handle from results, which is what
// exercises the sanitizer in dev.
type DevSecretBroker struct {
	Handle tool.SecretHandle
}

func (d *DevSecretBroker) Issue(context.Context, tool.SecretScope) (tool.SecretHandle, error) {
	if d.Handle == "" {
		return "dev-handle", nil
	}
	return d.Handle, nil
}
