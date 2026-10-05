package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxInterfaceBody   = 2 << 20
	maxEventPayload    = 256 << 10
	maxEventPage       = 1 << 20
	maxEventsPerPage   = 256
	defaultRunDeadline = time.Hour
)

type HostOptions struct {
	Adapter          string
	MaxConcurrent    int
	EventLimit       int
	ExecutionLimit   int
	ExecutionTimeout time.Duration
	Now              func() time.Time
	TerminationGrace time.Duration
	// ForceTerminate must kill the isolated execution boundary (subprocess,
	// container or microVM) after cooperative cancellation fails. Production
	// adapters should configure it; embedded dev adapters may leave it nil.
	ForceTerminate func(executionID string) error
}

type execution struct {
	digest     [sha256.Size]byte
	cancel     context.CancelFunc
	status     string
	events     []Event
	next       int64
	result     Result
	createdAt  time.Time
	workerDone bool
}

// Host exposes one Runtime through the stable HTTP+JSON Runtime Interface.
// It supplies idempotent start, bounded event history, cancellation, logical
// checkpoint/restore, and terminal result semantics for every adapter.
type Host struct {
	runtime Runtime
	opts    HostOptions

	mu         sync.RWMutex
	executions map[string]*execution
	active     int
}

func NewHost(runtime Runtime, options HostOptions) (*Host, error) {
	if runtime == nil {
		return nil, errors.New("agent runtime is required")
	}
	if strings.TrimSpace(options.Adapter) == "" {
		return nil, errors.New("adapter identity is required")
	}
	if options.MaxConcurrent <= 0 {
		options.MaxConcurrent = 16
	}
	if options.EventLimit <= 0 {
		options.EventLimit = 1024
	}
	if options.ExecutionLimit <= 0 {
		options.ExecutionLimit = 4096
	}
	if options.ExecutionLimit < options.MaxConcurrent {
		return nil, errors.New("execution retention limit must cover maximum concurrency")
	}
	if options.ExecutionTimeout <= 0 {
		options.ExecutionTimeout = defaultRunDeadline
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.TerminationGrace <= 0 {
		options.TerminationGrace = 5 * time.Second
	}
	return &Host{runtime: runtime, opts: options, executions: map[string]*execution{}}, nil
}

func (h *Host) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	prefix, protocol, ok := negotiatedRoute(request.URL.Path)
	if !ok {
		writeProblem(writer, http.StatusNotFound, "ROUTE_NOT_FOUND", "runtime interface route not found")
		return
	}
	writer.Header().Set("Fenced-Runtime-Interface", protocol)
	switch {
	case request.Method == http.MethodGet && request.URL.Path == prefix+"/health":
		h.health(writer, protocol)
	case request.Method == http.MethodPost && request.URL.Path == prefix+"/executions:start":
		h.start(writer, request)
	case strings.HasPrefix(request.URL.Path, prefix+"/executions/"):
		h.executionRoute(writer, request, prefix)
	default:
		writeProblem(writer, http.StatusNotFound, "ROUTE_NOT_FOUND", "runtime interface route not found")
	}
}

func (h *Host) health(writer http.ResponseWriter, protocol string) {
	h.mu.RLock()
	active := h.active
	h.mu.RUnlock()
	writeJSON(writer, http.StatusOK, HealthResponse{
		Status: "SERVING", ProtocolVersions: []string{protocol}, Adapter: h.opts.Adapter,
		MaxConcurrent: h.opts.MaxConcurrent, ActiveExecutions: active,
	})
}

func (h *Host) start(writer http.ResponseWriter, request *http.Request) {
	var body StartRequest
	encoded, err := decodeStrict(writer, request, &body)
	if err != nil {
		return
	}
	if err := requireJSONFields(encoded, "executionId", "agentVersionRef", "goal", "input", "capabilities"); err != nil {
		writeProblem(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	if err := validateStart(body); err != nil {
		writeProblem(writer, http.StatusUnprocessableEntity, "INVALID_START", err.Error())
		return
	}
	canonical, err := json.Marshal(body)
	if err != nil {
		writeProblem(writer, http.StatusInternalServerError, "ENCODING_FAILED", "request could not be normalized")
		return
	}
	digest := sha256.Sum256(canonical)

	h.mu.Lock()
	if existing := h.executions[body.ExecutionID]; existing != nil {
		if existing.digest != digest {
			h.mu.Unlock()
			writeProblem(writer, http.StatusConflict, "EXECUTION_CONFLICT", ErrExecutionConflict.Error())
			return
		}
		status := existing.status
		h.mu.Unlock()
		writeJSON(writer, http.StatusOK, StartResponse{ExecutionID: body.ExecutionID, Status: status, Replayed: true})
		return
	}
	if h.active >= h.opts.MaxConcurrent {
		h.mu.Unlock()
		writeProblem(writer, http.StatusTooManyRequests, "CAPACITY_EXHAUSTED", ErrCapacityExhausted.Error())
		return
	}
	if len(h.executions) >= h.opts.ExecutionLimit && !h.evictOldestTerminalLocked() {
		h.mu.Unlock()
		writeProblem(writer, http.StatusTooManyRequests, "RETENTION_EXHAUSTED", "execution retention capacity exhausted")
		return
	}
	executionCtx, cancel := context.WithTimeout(context.Background(), h.opts.ExecutionTimeout)
	state := &execution{digest: digest, cancel: cancel, status: StatusAccepted, next: 1, createdAt: h.opts.Now().UTC()}
	h.executions[body.ExecutionID] = state
	h.active++
	h.mu.Unlock()

	go h.run(executionCtx, body, state)
	writeJSON(writer, http.StatusAccepted, StartResponse{ExecutionID: body.ExecutionID, Status: StatusAccepted})
}

func (h *Host) run(ctx context.Context, request StartRequest, state *execution) {
	h.mu.Lock()
	state.status = StatusRunning
	h.mu.Unlock()
	emit := func(eventType string, payload json.RawMessage) error {
		if strings.TrimSpace(eventType) == "" || len(eventType) > 128 {
			return errors.New("event type is required and must not exceed 128 bytes")
		}
		if len(payload) == 0 {
			payload = json.RawMessage(`{}`)
		}
		if !json.Valid(payload) || len(payload) > maxEventPayload {
			return errors.New("event payload must be bounded valid JSON")
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		if state.status != StatusRunning {
			return errors.New("events cannot be emitted after execution termination")
		}
		if len(state.events) >= h.opts.EventLimit {
			return errors.New("event history limit exceeded")
		}
		state.events = append(state.events, Event{
			Sequence: state.next, Type: eventType, Payload: bytes.Clone(payload), OccurredAt: h.opts.Now().UTC(),
		})
		state.next++
		return nil
	}
	type outcome struct {
		output json.RawMessage
		err    error
	}
	completed := make(chan outcome, 1)
	go func() {
		var output json.RawMessage
		var err error
		defer func() {
			if recover() != nil {
				err = errors.New("adapter panicked")
			}
			completed <- outcome{output: output, err: err}
		}()
		output, err = h.runtime.Run(ctx, request, emit)
	}()
	var output json.RawMessage
	var err error
	workerCompleted := false
	select {
	case result := <-completed:
		output, err, workerCompleted = result.output, result.err, true
	case <-ctx.Done():
	}
	completedAt := h.opts.Now().UTC()
	result := Result{ExecutionID: request.ExecutionID, CompletedAt: &completedAt}
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		result.Status, result.ErrorCode, result.Error = StatusFailed, "EXECUTION_TIMEOUT", "adapter execution timed out"
	case errors.Is(ctx.Err(), context.Canceled):
		result.Status, result.ErrorCode, result.Error = StatusCancelled, "EXECUTION_CANCELLED", "execution cancelled"
	case err != nil:
		result.Status, result.ErrorCode, result.Error = StatusFailed, "ADAPTER_FAILED", "adapter execution failed"
	case len(output) == 0 || !json.Valid(output) || len(output) > maxInterfaceBody:
		result.Status, result.ErrorCode, result.Error = StatusFailed, "INVALID_RESULT", "adapter returned invalid JSON"
	default:
		result.Status, result.Output = StatusSucceeded, bytes.Clone(output)
	}
	h.mu.Lock()
	state.status = result.Status
	state.result = result
	state.workerDone = workerCompleted
	if workerCompleted {
		h.active--
	}
	h.mu.Unlock()
	if !workerCompleted {
		timer := time.NewTimer(h.opts.TerminationGrace)
		select {
		case <-completed:
			timer.Stop()
		case <-timer.C:
			if h.opts.ForceTerminate != nil {
				_ = h.opts.ForceTerminate(request.ExecutionID)
			}
			<-completed
		}
		h.mu.Lock()
		state.workerDone = true
		h.active--
		h.mu.Unlock()
	}
}

func (h *Host) executionRoute(writer http.ResponseWriter, request *http.Request, prefix string) {
	remainder := strings.TrimPrefix(request.URL.Path, prefix+"/executions/")
	var executionID, action string
	switch {
	case strings.HasSuffix(remainder, ":stop"):
		executionID, action = strings.TrimSuffix(remainder, ":stop"), "stop"
	case strings.HasSuffix(remainder, ":checkpoint"):
		executionID, action = strings.TrimSuffix(remainder, ":checkpoint"), "checkpoint"
	case strings.HasSuffix(remainder, ":restore"):
		executionID, action = strings.TrimSuffix(remainder, ":restore"), "restore"
	case strings.HasSuffix(remainder, "/events/stream"):
		executionID, action = strings.TrimSuffix(remainder, "/events/stream"), "events/stream"
	case strings.HasSuffix(remainder, "/events"):
		executionID, action = strings.TrimSuffix(remainder, "/events"), "events"
	case strings.HasSuffix(remainder, "/result"):
		executionID, action = strings.TrimSuffix(remainder, "/result"), "result"
	default:
		writeProblem(writer, http.StatusNotFound, "ROUTE_NOT_FOUND", "runtime execution route not found")
		return
	}
	if executionID == "" || strings.Contains(executionID, "/") {
		writeProblem(writer, http.StatusBadRequest, "INVALID_EXECUTION_ID", "execution id is invalid")
		return
	}
	switch action {
	case "stop":
		h.stop(writer, request, executionID)
	case "checkpoint":
		h.checkpoint(writer, request, executionID)
	case "restore":
		h.restore(writer, request, executionID)
	case "events/stream":
		h.eventStream(writer, request, executionID)
	case "events":
		h.events(writer, request, executionID)
	case "result":
		h.result(writer, request, executionID)
	}
}

func negotiatedRoute(path string) (prefix, protocol string, ok bool) {
	switch {
	case path == "/v1" || strings.HasPrefix(path, "/v1/"):
		return "/v1", ProtocolVersion, true
	case path == "/v1alpha1" || strings.HasPrefix(path, "/v1alpha1/"):
		return "/v1alpha1", LegacyProtocolVersion, true
	default:
		return "", "", false
	}
}

func (h *Host) stop(writer http.ResponseWriter, request *http.Request, executionID string) {
	if request.Method != http.MethodPost {
		writeProblem(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "POST is required")
		return
	}
	h.mu.RLock()
	state := h.executions[executionID]
	h.mu.RUnlock()
	if state == nil {
		writeProblem(writer, http.StatusNotFound, "EXECUTION_NOT_FOUND", ErrExecutionNotFound.Error())
		return
	}
	state.cancel()
	h.mu.RLock()
	status := state.status
	h.mu.RUnlock()
	writeJSON(writer, http.StatusAccepted, StopResponse{ExecutionID: executionID, Status: status})
}

func (h *Host) checkpoint(writer http.ResponseWriter, request *http.Request, executionID string) {
	if request.Method != http.MethodPost {
		writeProblem(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "POST is required")
		return
	}
	if !h.exists(executionID) {
		writeProblem(writer, http.StatusNotFound, "EXECUTION_NOT_FOUND", ErrExecutionNotFound.Error())
		return
	}
	checkpointCtx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	checkpoint, err := h.runtime.Checkpoint(checkpointCtx, executionID)
	if err != nil {
		if errors.Is(checkpointCtx.Err(), context.DeadlineExceeded) {
			writeProblem(writer, http.StatusGatewayTimeout, "CHECKPOINT_TIMEOUT", "checkpoint operation timed out")
		} else {
			writeProblem(writer, http.StatusUnprocessableEntity, "CHECKPOINT_FAILED", "checkpoint operation failed")
		}
		return
	}
	if err := validateCheckpoint(checkpoint); err != nil {
		writeProblem(writer, http.StatusUnprocessableEntity, "INVALID_CHECKPOINT", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, CheckpointResponse{ExecutionID: executionID, Checkpoint: checkpoint})
}

func (h *Host) restore(writer http.ResponseWriter, request *http.Request, executionID string) {
	if request.Method != http.MethodPost {
		writeProblem(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "POST is required")
		return
	}
	var body RestoreRequest
	if _, err := decodeStrict(writer, request, &body); err != nil {
		return
	}
	if body.ExecutionID != executionID {
		writeProblem(writer, http.StatusUnprocessableEntity, "RESTORE_ID_MISMATCH", "path and body execution ids must match")
		return
	}
	if err := validateCheckpoint(body.Checkpoint); err != nil {
		writeProblem(writer, http.StatusUnprocessableEntity, "INVALID_CHECKPOINT", err.Error())
		return
	}
	restoreCtx, cancel := context.WithTimeout(request.Context(), 10*time.Second)
	defer cancel()
	if err := h.runtime.Restore(restoreCtx, body); err != nil {
		if errors.Is(restoreCtx.Err(), context.DeadlineExceeded) {
			writeProblem(writer, http.StatusGatewayTimeout, "RESTORE_TIMEOUT", "restore operation timed out")
		} else {
			writeProblem(writer, http.StatusUnprocessableEntity, "RESTORE_FAILED", "restore operation failed")
		}
		return
	}
	writeJSON(writer, http.StatusOK, RestoreResponse{ExecutionID: executionID, Restored: true})
}

func (h *Host) events(writer http.ResponseWriter, request *http.Request, executionID string) {
	if request.Method != http.MethodGet {
		writeProblem(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "GET is required")
		return
	}
	after := int64(0)
	if raw := request.URL.Query().Get("after"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			writeProblem(writer, http.StatusBadRequest, "INVALID_CURSOR", "after must be a non-negative integer")
			return
		}
		after = value
	}
	h.mu.RLock()
	state := h.executions[executionID]
	if state == nil {
		h.mu.RUnlock()
		writeProblem(writer, http.StatusNotFound, "EXECUTION_NOT_FOUND", ErrExecutionNotFound.Error())
		return
	}
	events := make([]Event, 0, len(state.events))
	pageBytes := 0
	truncated := false
	for _, event := range state.events {
		if event.Sequence > after {
			encoded, _ := json.Marshal(event)
			if len(events) >= maxEventsPerPage || pageBytes+len(encoded) > maxEventPage {
				truncated = true
				break
			}
			event.Payload = bytes.Clone(event.Payload)
			events = append(events, event)
			pageBytes += len(encoded)
		}
	}
	next := after
	if len(events) > 0 {
		next = events[len(events)-1].Sequence
	}
	h.mu.RUnlock()
	writeJSON(writer, http.StatusOK, EventList{ExecutionID: executionID, Events: events, NextAfter: next, Truncated: truncated})
}

func (h *Host) result(writer http.ResponseWriter, request *http.Request, executionID string) {
	if request.Method != http.MethodGet {
		writeProblem(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "GET is required")
		return
	}
	h.mu.RLock()
	state := h.executions[executionID]
	if state == nil {
		h.mu.RUnlock()
		writeProblem(writer, http.StatusNotFound, "EXECUTION_NOT_FOUND", ErrExecutionNotFound.Error())
		return
	}
	status := state.status
	result := state.result
	result.Output = bytes.Clone(result.Output)
	h.mu.RUnlock()
	if status == StatusAccepted || status == StatusRunning {
		writeJSON(writer, http.StatusAccepted, Result{ExecutionID: executionID, Status: status})
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (h *Host) exists(executionID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.executions[executionID] != nil
}

func (h *Host) evictOldestTerminalLocked() bool {
	var oldestID string
	var oldest time.Time
	for id, state := range h.executions {
		if state.status == StatusAccepted || state.status == StatusRunning || !state.workerDone {
			continue
		}
		if oldestID == "" || state.createdAt.Before(oldest) {
			oldestID, oldest = id, state.createdAt
		}
	}
	if oldestID == "" {
		return false
	}
	delete(h.executions, oldestID)
	return true
}

func validateStart(request StartRequest) error {
	if strings.TrimSpace(request.ExecutionID) == "" || len(request.ExecutionID) > 128 {
		return errors.New("executionId is required and must not exceed 128 bytes")
	}
	if strings.TrimSpace(request.AgentVersionRef) == "" || len(request.AgentVersionRef) > 256 {
		return errors.New("agentVersionRef is required and must not exceed 256 bytes")
	}
	if strings.TrimSpace(request.Goal) == "" || len(request.Goal) > 16<<10 {
		return errors.New("goal is required and must not exceed 16 KiB")
	}
	if len(request.Input) == 0 {
		request.Input = json.RawMessage(`{}`)
	}
	if !json.Valid(request.Input) {
		return errors.New("input must be valid JSON")
	}
	sets := []struct {
		name   string
		values []string
	}{
		{name: "tools", values: request.Capabilities.Tools},
		{name: "models", values: request.Capabilities.Models},
		{name: "memory", values: request.Capabilities.Memory},
		{name: "secrets", values: request.Capabilities.Secrets},
	}
	for _, set := range sets {
		if set.values == nil {
			return fmt.Errorf("capabilities.%s must be an explicit array", set.name)
		}
		if len(set.values) > 256 {
			return fmt.Errorf("capabilities.%s exceeds 256 grants", set.name)
		}
		seen := map[string]struct{}{}
		for _, value := range set.values {
			if strings.TrimSpace(value) == "" || len(value) > 256 {
				return fmt.Errorf("capabilities.%s contains an invalid grant", set.name)
			}
			if _, exists := seen[value]; exists {
				return fmt.Errorf("capabilities.%s contains duplicate grant %q", set.name, value)
			}
			seen[value] = struct{}{}
		}
	}
	for _, sensitivity := range request.Capabilities.MemorySensitivities {
		if sensitivity != "internal" && sensitivity != "confidential" && sensitivity != "restricted" {
			return fmt.Errorf("capabilities.memorySensitivities contains invalid tier %q", sensitivity)
		}
	}
	if err := validateOptionalGrantSet("childAgents", request.Capabilities.ChildAgents); err != nil {
		return err
	}
	if err := validateOptionalGrantSet("peers", request.Capabilities.Peers); err != nil {
		return err
	}
	if request.Capabilities.SpawnTasks && len(request.Capabilities.ChildAgents) == 0 {
		return errors.New("capabilities.childAgents requires at least one entry when spawnTasks is enabled")
	}
	return nil
}

// validateOptionalGrantSet checks a capability list that a manifest may omit.
// Unlike tools/models/memory/secrets, an absent list is valid here: absence has
// to keep meaning "this manifest predates the grant", not "malformed manifest".
// An empty list is the explicit form and denies everything.
func validateOptionalGrantSet(name string, values []string) error {
	if values == nil {
		return nil
	}
	if len(values) > 256 {
		return fmt.Errorf("capabilities.%s exceeds 256 grants", name)
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		if strings.TrimSpace(value) == "" || len(value) > 256 {
			return fmt.Errorf("capabilities.%s contains an invalid grant", name)
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("capabilities.%s contains duplicate grant %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateCheckpoint(checkpoint Checkpoint) error {
	if strings.TrimSpace(checkpoint.SchemaVersion) == "" || len(checkpoint.SchemaVersion) > 256 {
		return errors.New("checkpoint schemaVersion is required and must not exceed 256 bytes")
	}
	if len(checkpoint.State) == 0 || !json.Valid(checkpoint.State) || len(checkpoint.State) > maxInterfaceBody {
		return errors.New("checkpoint state must be bounded valid JSON")
	}
	if checkpoint.CreatedAt.IsZero() {
		return errors.New("checkpoint createdAt is required")
	}
	return nil
}

func decodeStrict(writer http.ResponseWriter, request *http.Request, target any) ([]byte, error) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxInterfaceBody)
	encoded, err := io.ReadAll(request.Body)
	if err != nil {
		writeProblem(writer, http.StatusBadRequest, "INVALID_JSON", "request body could not be read")
		return nil, err
	}
	if err := rejectDuplicateJSON(encoded); err != nil {
		writeProblem(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeProblem(writer, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeProblem(writer, http.StatusBadRequest, "INVALID_JSON", "request body must contain one JSON document")
		return nil, errors.New("trailing JSON")
	}
	return encoded, nil
}

func requireJSONFields(encoded []byte, fields ...string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		return errors.New("request body must be an object")
	}
	for _, field := range fields {
		if _, ok := object[field]; !ok {
			return fmt.Errorf("required field %q is missing", field)
		}
	}
	return nil
}

func rejectDuplicateJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanValue(decoder); err != nil {
		return errors.New("request contains invalid or duplicate JSON fields")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON document")
	}
	return nil
}

func scanValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate key %q", key)
			}
			seen[key] = struct{}{}
			if err := scanValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := scanValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return errors.New("unexpected JSON delimiter")
	}
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func writeProblem(writer http.ResponseWriter, status int, code, detail string) {
	writeJSON(writer, status, map[string]any{"code": code, "detail": detail, "status": status})
}
