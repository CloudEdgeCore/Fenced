package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
)

const maxWebhookResponseBytes = 1 << 20

// WebhookExecutor is the production tool adapter boundary. Every immutable
// tool version maps to one preconfigured HTTPS endpoint; Agent input can
// never select or modify the destination.
type WebhookExecutor struct {
	endpoints map[string]*url.URL
	client    *http.Client
}

func NewWebhookExecutor(endpoints map[string]string, client *http.Client) (*WebhookExecutor, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("at least one tool endpoint is required")
	}
	parsed := make(map[string]*url.URL, len(endpoints))
	for reference, endpoint := range endpoints {
		name, version, found := strings.Cut(reference, "@")
		if !found || strings.Contains(version, "@") || store.ValidateToolName(name) != nil || agentversion.ValidateVersion(version) != nil {
			return nil, fmt.Errorf("tool endpoint key %q must be name@version", reference)
		}
		target, err := url.Parse(endpoint)
		if err != nil || target.Scheme != "https" || target.Host == "" || target.User != nil || target.Fragment != "" || target.RawQuery != "" {
			return nil, fmt.Errorf("tool endpoint %q must be an absolute HTTPS URL without credentials, query, or fragment", reference)
		}
		parsed[reference] = target
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	copyClient := *client
	if copyClient.Timeout <= 0 || copyClient.Timeout > time.Minute {
		copyClient.Timeout = 30 * time.Second
	}
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("tool endpoint redirects are forbidden")
	}
	return &WebhookExecutor{endpoints: parsed, client: &copyClient}, nil
}

func (e *WebhookExecutor) Execute(ctx context.Context, input tool.ExecutionRequest) (tool.ExecutionResult, error) {
	reference := input.Descriptor.Name + "@" + input.Descriptor.Version
	endpoint := e.endpoints[reference]
	if endpoint == nil {
		return tool.ExecutionResult{}, fmt.Errorf("tool endpoint %q is not configured", reference)
	}
	payload, err := json.Marshal(struct {
		Action   string          `json:"action"`
		Resource string          `json:"resource"`
		Args     json.RawMessage `json:"args"`
	}{Action: input.Action, Resource: input.Resource, Args: input.Args})
	if err != nil {
		return tool.ExecutionResult{}, fmt.Errorf("encode tool request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return tool.ExecutionResult{}, fmt.Errorf("create tool request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "Fenced-Tool-Gateway/1.0")
	if input.Secret != "" {
		request.Header.Set("Fenced-Secret-Handle", string(input.Secret))
	}
	response, err := e.client.Do(request)
	if err != nil {
		return tool.ExecutionResult{}, fmt.Errorf("execute tool endpoint: %w", err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxWebhookResponseBytes+1))
	if err != nil {
		return tool.ExecutionResult{}, fmt.Errorf("read tool response: %w", err)
	}
	if len(encoded) > maxWebhookResponseBytes {
		return tool.ExecutionResult{}, fmt.Errorf("tool response exceeds %d bytes", maxWebhookResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		// Preserve the bounded HTTP classification across the Tool Gateway so
		// agents can distinguish retryable upstream failures (429/5xx) from
		// deterministic terminal outcomes (404/410/415). Response bodies stay
		// private to the gateway and never become part of the public error.
		failureCode := fmt.Sprintf("TOOL_ENDPOINT_HTTP_%d", response.StatusCode)
		return tool.ExecutionResult{FailureCode: failureCode}, fmt.Errorf("tool endpoint returned HTTP %d", response.StatusCode)
	}
	if !json.Valid(encoded) {
		return tool.ExecutionResult{}, fmt.Errorf("tool endpoint returned invalid JSON")
	}
	return tool.ExecutionResult{Output: json.RawMessage(encoded)}, nil
}
