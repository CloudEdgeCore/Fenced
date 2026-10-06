package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// HTTPEmbedder calls a preconfigured HTTPS embedding boundary. The endpoint
// is operator configuration, never Agent-controlled data.
type HTTPEmbedder struct {
	endpoint string
	token    string
	client   *http.Client
}

func NewHTTPEmbedder(endpoint, bearerToken string, client *http.Client) (*HTTPEmbedder, error) {
	target, err := url.Parse(endpoint)
	if err != nil || target.Scheme != "https" || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" {
		return nil, fmt.Errorf("embedding endpoint must be an absolute HTTPS URL without credentials, query, or fragment")
	}
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	copyClient := *client
	if copyClient.Timeout <= 0 || copyClient.Timeout > time.Minute {
		copyClient.Timeout = 20 * time.Second
	}
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("embedding redirects are forbidden") }
	return &HTTPEmbedder{endpoint: target.String(), token: bearerToken, client: &copyClient}, nil
}

func (e *HTTPEmbedder) Embed(ctx context.Context, content string) ([]float32, error) {
	if len(content) == 0 || len(content) > store.MemoryContentLimit {
		return nil, fmt.Errorf("embedding content is empty or exceeds the memory bound")
	}
	payload, err := json.Marshal(struct {
		Input string `json:"input"`
	}{Input: content})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "Fenced-Memory-Gateway/1.0")
	if e.token != "" {
		request.Header.Set("Authorization", "Bearer "+e.token)
	}
	response, err := e.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("embedding request: %w", err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding endpoint returned HTTP %d", response.StatusCode)
	}
	var result struct {
		Embedding []float32 `json:"embedding"`
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("embedding response contains trailing JSON")
	}
	if len(result.Embedding) != store.MemoryEmbeddingDimension {
		return nil, fmt.Errorf("embedding must contain %d dimensions", store.MemoryEmbeddingDimension)
	}
	for _, value := range result.Embedding {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, fmt.Errorf("embedding contains a non-finite value")
		}
	}
	return result.Embedding, nil
}
