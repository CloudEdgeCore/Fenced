package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	baseURL  string
	http     *http.Client
	protocol string
	prefix   string
}

// ValidateBaseURL checks one Runtime Interface endpoint against the wire
// contract every client and binding loader shares: an absolute http(s) URL
// with a host and no userinfo, query, fragment, or path components. It is
// the single validator so configuration fails fast at load time instead of
// at the first request.
func ValidateBaseURL(baseURL string) error {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return errors.New("runtime interface base URL must be an absolute HTTP(S) URL")
	}
	if parsed.Hostname() == "" {
		return errors.New("runtime interface base URL must include a host")
	}
	return nil
}

func NewClient(baseURL string, client *http.Client) (*Client, error) {
	if err := ValidateBaseURL(baseURL); err != nil {
		return nil, err
	}
	parsed, _ := url.Parse(strings.TrimRight(baseURL, "/"))
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{baseURL: parsed.String(), http: client, protocol: ProtocolVersion, prefix: "/v1"}, nil
}

// NewLegacyClient creates an explicit N-1 client for the deprecated alpha
// endpoint. New integrations must use NewClient, which negotiates stable v1.
func NewLegacyClient(baseURL string, client *http.Client) (*Client, error) {
	legacy, err := NewClient(baseURL, client)
	if err != nil {
		return nil, err
	}
	legacy.protocol, legacy.prefix = LegacyProtocolVersion, "/v1alpha1"
	return legacy, nil
}

func (c *Client) Health(ctx context.Context) (HealthResponse, error) {
	var response HealthResponse
	err := c.call(ctx, http.MethodGet, c.prefix+"/health", nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) Start(ctx context.Context, request StartRequest) (StartResponse, error) {
	var response StartResponse
	err := c.call(ctx, http.MethodPost, c.prefix+"/executions:start", request, http.StatusAccepted, &response)
	if httpError := new(HTTPError); errors.As(err, &httpError) && httpError.Status == http.StatusOK {
		if decodeErr := json.Unmarshal(httpError.Body, &response); decodeErr != nil {
			return StartResponse{}, decodeErr
		}
		err = nil
	}
	return response, err
}

func (c *Client) Stop(ctx context.Context, executionID string) (StopResponse, error) {
	var response StopResponse
	err := c.call(ctx, http.MethodPost, c.executionPath(executionID, ":stop"), struct{}{}, http.StatusAccepted, &response)
	return response, err
}

func (c *Client) Checkpoint(ctx context.Context, executionID string) (CheckpointResponse, error) {
	var response CheckpointResponse
	err := c.call(ctx, http.MethodPost, c.executionPath(executionID, ":checkpoint"), struct{}{}, http.StatusOK, &response)
	return response, err
}

func (c *Client) Restore(ctx context.Context, request RestoreRequest) (RestoreResponse, error) {
	var response RestoreResponse
	err := c.call(ctx, http.MethodPost, c.executionPath(request.ExecutionID, ":restore"), request, http.StatusOK, &response)
	return response, err
}

func (c *Client) Events(ctx context.Context, executionID string, after int64) (EventList, error) {
	var response EventList
	path := c.executionPath(executionID, "/events") + "?after=" + strconv.FormatInt(after, 10)
	err := c.call(ctx, http.MethodGet, path, nil, http.StatusOK, &response)
	return response, err
}

func (c *Client) Result(ctx context.Context, executionID string) (Result, bool, error) {
	var response Result
	err := c.call(ctx, http.MethodGet, c.executionPath(executionID, "/result"), nil, http.StatusOK, &response)
	if httpError := new(HTTPError); errors.As(err, &httpError) && httpError.Status == http.StatusAccepted {
		if decodeErr := json.Unmarshal(httpError.Body, &response); decodeErr != nil {
			return Result{}, false, decodeErr
		}
		return response, false, nil
	}
	return response, err == nil, err
}

func (c *Client) WaitResult(ctx context.Context, executionID string, interval time.Duration) (Result, error) {
	if interval <= 0 {
		interval = 25 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, terminal, err := c.Result(ctx, executionID)
		if err != nil {
			return Result{}, err
		}
		if terminal {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

type HTTPError struct {
	Status int
	Body   []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("runtime interface returned HTTP %d: %s", e.Status, e.Body)
}

func (c *Client) call(ctx context.Context, method, path string, body any, expected int, response any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	reply, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer reply.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(reply.Body, maxInterfaceBody+1))
	if err != nil {
		return err
	}
	if len(encoded) > maxInterfaceBody {
		return errors.New("runtime interface response exceeds 2 MiB")
	}
	if reply.Header.Get("Fenced-Runtime-Interface") != c.protocol {
		return errors.New("runtime interface protocol negotiation failed")
	}
	if reply.StatusCode != expected {
		return &HTTPError{Status: reply.StatusCode, Body: encoded}
	}
	if response == nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(response); err != nil {
		return fmt.Errorf("decode runtime response: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("runtime interface response contains trailing JSON")
	}
	return nil
}

func (c *Client) executionPath(executionID, suffix string) string {
	return c.prefix + "/executions/" + url.PathEscape(executionID) + suffix
}
