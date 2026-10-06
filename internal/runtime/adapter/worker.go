// Package adapter implements the Runtime Protocol provider that delegates an
// assignment to a language/framework-neutral Agent Runtime Interface endpoint.
package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/mcp"
	"github.com/CloudEdgeCore/Fenced/internal/platform/redact"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/attemptstate"
	"github.com/CloudEdgeCore/Fenced/internal/runtime/leasekeeper"
	"github.com/CloudEdgeCore/Fenced/sdk/agent"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ProviderName        = "adapter-http"
	CheckpointSchema    = "fenced.adapter-checkpoint/v1"
	checkpointMediaType = "application/vnd.fenced.adapter-checkpoint+json"
	resultMediaType     = "application/vnd.fenced.adapter-result+json"
	controlRPCTimeout   = 15 * time.Second
	defaultPollInterval = 250 * time.Millisecond
	maxPollInterval     = 2 * time.Second
	maxResultBytes      = 2 << 20
	maxCollectedEvents  = 4096
)

type ArtifactStore interface {
	Put(context.Context, string, string, io.Reader) (store.ArtifactReference, error)
	Open(context.Context, string, store.ArtifactReference) (io.ReadCloser, error)
}

// ExecutionWindow receives the fenced execution context for the duration of
// one assignment: the runtime opens the sandbox Agent's brokered access
// (MCP) when the attempt is RUNNING and closes it on every exit path
// (default deny outside execution windows). Open returns the closer the
// worker defers, so concurrent workers sharing one Agent endpoint bind
// their identities per attempt.
type ExecutionWindow interface {
	Open(mcp.AttemptContext) func()
}

type Worker struct {
	control           runtimev1.RuntimeControlServiceClient
	artifacts         ArtifactStore
	runtime           *agent.Client
	endpoint          string
	tenantID          string
	runtimeInstanceID string
	heartbeatTTL      time.Duration
	pollInterval      time.Duration
	window            ExecutionWindow
	// bindings resolve logical manifest entrypoints (fenced-binding://…)
	// to concrete deployment endpoints, keeping mutable endpoint state out
	// of immutable AgentVersions. The resolved client is assignment-local:
	// resolveRuntime never mutates shared worker state, so a future
	// concurrent RunOnce cannot misroute another assignment's client.
	bindings   *RuntimeBindings
	httpClient *http.Client
	clients    map[string]*agent.Client
}

// WithExecutionWindow attaches the sandbox brokered-access window (the
// loopback MCP identity slot). The worker publishes the fenced identity and
// the AgentVersion capability grants for the duration of each assignment.
func (w *Worker) WithExecutionWindow(window ExecutionWindow) *Worker {
	w.window = window
	return w
}

// WithRuntimeBindings attaches the deployment's runtime binding table. When
// set, assignments whose manifest entrypoint is a logical binding reference
// (or whose version ref has an explicit binding) resolve through it; the
// worker's constructor endpoint remains the highest-priority override - a
// dedicated-worker configuration ValidateEndpointOverride guards.
func (w *Worker) WithRuntimeBindings(bindings *RuntimeBindings) *Worker {
	w.bindings = bindings
	return w
}

func NewWorker(
	control runtimev1.RuntimeControlServiceClient,
	artifacts ArtifactStore,
	endpoint, tenantID, runtimeInstanceID string,
	heartbeatTTL time.Duration,
	httpClient *http.Client,
) (*Worker, error) {
	if control == nil || artifacts == nil {
		return nil, fmt.Errorf("runtime Protocol client and artifact store are required")
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(runtimeInstanceID) == "" || heartbeatTTL <= 0 {
		return nil, fmt.Errorf("tenant, runtime instance and positive heartbeat TTL are required")
	}
	// An empty endpoint is valid when runtime bindings resolve every
	// assignment; resolveRuntime fails closed on unresolvable entrypoints.
	// The production endpoint policy applies at construction: a plaintext
	// constructor endpoint must be loopback (co-located runtime) and a
	// remote endpoint must be HTTPS.
	var client *agent.Client
	if strings.TrimSpace(endpoint) != "" {
		if err := (EndpointPolicy{}).Validate(endpoint); err != nil {
			return nil, err
		}
		if err := refuseInsecureTransport(httpClient, strings.TrimRight(endpoint, "/")); err != nil {
			return nil, err
		}
		var err error
		client, err = agent.NewClient(endpoint, httpClient)
		if err != nil {
			return nil, err
		}
	}
	return &Worker{
		control: control, artifacts: artifacts, runtime: client, endpoint: strings.TrimRight(endpoint, "/"),
		tenantID: tenantID, runtimeInstanceID: runtimeInstanceID, heartbeatTTL: heartbeatTTL,
		pollInterval: defaultPollInterval, httpClient: httpClient, clients: map[string]*agent.Client{},
	}, nil
}

// resolveRuntime picks the Runtime Interface client for one assignment.
// Priority: the worker's explicit constructor endpoint (the operator's
// per-worker override), then a runtime binding for the agent version
// (exact ref or name wildcard), then a concrete http(s) entrypoint embedded
// in the manifest. A logical binding entrypoint with no covering binding
// fails closed - an unresolved deployment reference must never silently
// fall through to another agent's endpoint.
func (w *Worker) resolveRuntime(agentVersionRef string, target agentversion.RuntimeTarget) (*agent.Client, error) {
	if w.runtime != nil && w.endpoint != "" {
		return w.runtime, nil
	}
	if binding, ok := w.bindings.ResolveBinding(agentVersionRef); ok {
		tlsConfig, identity := w.bindings.transportFor(agentVersionRef)
		return w.clientFor(binding, tlsConfig, identity)
	}
	entrypoint := target.Entrypoint[0]
	if IsLogicalEntrypoint(entrypoint) {
		return nil, fmt.Errorf("agent version %s declares logical entrypoint %q but no runtime binding resolves it",
			agentVersionRef, entrypoint)
	}
	if err := w.endpointPolicy().Validate(entrypoint); err != nil {
		return nil, fmt.Errorf("agent version %s entrypoint %q violates the runtime endpoint policy: %w",
			agentVersionRef, entrypoint, err)
	}
	if parsed, err := url.Parse(entrypoint); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Hostname() != "" {
		return w.clientFor(RuntimeBinding{Endpoint: strings.TrimRight(entrypoint, "/")}, nil, "")
	}
	return nil, fmt.Errorf("agent version %s entrypoint %q is neither bindable nor an absolute http(s) URL",
		agentVersionRef, entrypoint)
}

// endpointPolicy is the policy manifest entrypoints are validated against:
// the binding table's policy when one is loaded, otherwise production.
func (w *Worker) endpointPolicy() EndpointPolicy {
	if w.bindings != nil {
		return w.bindings.policy
	}
	return EndpointPolicy{}
}

// clientFor builds (and caches) the Runtime Interface client of one
// endpoint. A binding with TLS material gets a dedicated transport with the
// pinned server name, private trust bundle and client certificate. The
// cache key carries the TLS material fingerprint (P1-03): two bindings that
// share an endpoint and SNI but present different certificates are
// different identities and never reuse one cached client.
func (w *Worker) clientFor(binding RuntimeBinding, tlsConfig *tls.Config, identity string) (*agent.Client, error) {
	endpoint := strings.TrimRight(binding.Endpoint, "/")
	key := endpoint
	if binding.TLSServerName != "" {
		key += "|sni=" + binding.TLSServerName
	}
	if identity != "" {
		key += "|tls=" + identity
	}
	if client, ok := w.clients[key]; ok {
		return client, nil
	}
	httpClient := w.httpClient
	if tlsConfig != nil || binding.TLSServerName != "" {
		httpClient = pinnedHTTPClient(w.httpClient, tlsConfig, binding.TLSServerName)
	}
	if err := refuseInsecureTransport(httpClient, endpoint); err != nil {
		return nil, err
	}
	client, err := agent.NewClient(endpoint, httpClient)
	if err != nil {
		return nil, err
	}
	w.clients[key] = client
	return client, nil
}

// pinnedHTTPClient clones the base client's transport with the binding's
// TLS configuration (verification name, trust bundle, client certificate).
func pinnedHTTPClient(base *http.Client, tlsConfig *tls.Config, serverName string) *http.Client {
	if base == nil {
		base = &http.Client{}
	}
	transport, ok := base.Transport.(*http.Transport)
	if ok {
		transport = transport.Clone()
	} else {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	config := tlsConfig
	if config == nil {
		config = &tls.Config{}
	}
	if serverName != "" {
		config = config.Clone()
		config.ServerName = serverName
	}
	transport.TLSClientConfig = config
	return &http.Client{Transport: transport, Timeout: base.Timeout}
}

// refuseInsecureTransport fails closed when an HTTPS endpoint would be
// dialed through a transport with certificate verification disabled: an
// intercepted (man-in-the-middle) runtime endpoint must never be accepted.
// The default transport verifies server certificates and is always allowed.
func refuseInsecureTransport(httpClient *http.Client, endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" {
		return nil
	}
	if httpClient == nil {
		return nil
	}
	transport, ok := httpClient.Transport.(*http.Transport)
	if !ok {
		return nil
	}
	if transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify {
		return fmt.Errorf("runtime endpoint %s refuses a transport with TLS verification disabled", endpoint)
	}
	return nil
}

func (w *Worker) RunOnce(ctx context.Context) (processed bool, runErr error) {
	assignmentAcquired := false
	defer func() {
		// Once polling succeeded, PermissionDenied means this concrete fenced
		// assignment was lost (expired lease or a newer recovery owner). The
		// long-lived worker must continue polling instead of terminating and
		// reducing fleet capacity. A real endpoint authorization failure is
		// still surfaced by the next PollAssignment call.
		if assignmentAcquired && status.Code(runErr) == codes.PermissionDenied {
			processed, runErr = false, nil
		}
	}()
	pollCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	polled, err := w.control.PollAssignment(pollCtx, &runtimev1.PollAssignmentRequest{
		TenantId: w.tenantID, RuntimeInstanceId: w.runtimeInstanceID,
	})
	cancel()
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("poll adapter assignment: %w", err)
	}
	assignment := polled.GetAssignment()
	if assignment == nil || assignment.GetIdentity() == nil {
		return false, fmt.Errorf("runtime assignment is incomplete")
	}
	if assignment.GetIdentity().GetTenantId() != w.tenantID || assignment.GetRuntimeInstanceId() != w.runtimeInstanceID {
		return false, fmt.Errorf("runtime assignment identity does not match adapter worker")
	}
	assignmentAcquired = true
	target, capabilities, checkpointPolicy, err := w.target(assignment)
	if err != nil {
		return w.fail(ctx, assignment.GetIdentity(), assignment.GetAttemptVersion(), "adapter_manifest_invalid", err)
	}
	runtimeClient, err := w.resolveRuntime(assignment.GetAgentVersionRef(), target)
	if err != nil {
		return w.fail(ctx, assignment.GetIdentity(), assignment.GetAttemptVersion(), "adapter_endpoint_unresolved", err)
	}
	identity := assignment.GetIdentity()
	taskID, runID, attemptID, err := parseAssignmentIdentity(assignment)
	if err != nil {
		return false, fmt.Errorf("adapter assignment identity is invalid: %w", err)
	}
	version, proceed, err := w.advanceAttempt(ctx, identity, assignment.GetAttemptVersion(),
		domain.AttemptPlaced, domain.AttemptStarting, runtimev1.AttemptPhase_ATTEMPT_PHASE_STARTING)
	if err != nil {
		return false, err
	}
	if !proceed {
		return false, nil
	}
	version, proceed, err = w.advanceAttempt(ctx, identity, version,
		domain.AttemptStarting, domain.AttemptRunning, runtimev1.AttemptPhase_ATTEMPT_PHASE_RUNNING)
	if err != nil {
		return false, err
	}
	if !proceed {
		return false, nil
	}
	if w.window != nil {
		lineage := parseWorkflowLineage(assignment.GetWorkflowLineage())
		closeWindow := w.window.Open(mcp.AttemptContext{
			TenantID: identity.GetTenantId(), TaskID: taskID,
			RunID: runID, AttemptID: attemptID,
			FencingToken: identity.GetFencingToken(), AgentVersionRef: assignment.GetAgentVersionRef(),
			WorkflowID: lineage.workflowID, WorkflowVersion: lineage.workflowVersion, ParentStepName: lineage.stepName,
			AllowedModels:              cloneExplicit(capabilities.Models),
			AllowedMemoryNamespaces:    cloneExplicit(capabilities.Memory),
			AllowedMemorySensitivities: memorySensitivities(capabilities.MemorySensitivities),
			CanSpawnTasks:              capabilities.SpawnTasks,
			AllowedChildAgents:         cloneExplicit(capabilities.ChildAgents),
		})
		defer closeWindow()
	}
	heartbeat, err := w.heartbeat(ctx, assignment)
	if err != nil {
		return false, err
	}
	if heartbeat.GetCancelRequested() {
		return true, nil
	}
	keeper, executionCtx := leasekeeper.Start(ctx, leasekeeper.Options{
		Client: w.control, Identity: identity, AttemptID: identity.GetAttemptId(),
		HeartbeatTTL: w.heartbeatTTL, RPCTimeout: controlRPCTimeout,
	}, heartbeat.GetLeaseVersion(), heartbeat.GetAttemptVersion())
	defer keeper.Stop()

	if assignment.GetResumeCheckpoint() != nil {
		if checkpointPolicy.Mode != agentversion.CheckpointLogical {
			return w.fail(ctx, identity, version, "adapter_restore_failed", fmt.Errorf("checkpoint resume is disabled by the AgentVersion manifest"))
		}
		if err := w.restore(executionCtx, runtimeClient, assignment, target, checkpointPolicy); err != nil {
			return w.fail(ctx, identity, version, "adapter_restore_failed", err)
		}
	}
	start := agent.StartRequest{
		ExecutionID: identity.GetAttemptId(), AgentVersionRef: assignment.GetAgentVersionRef(),
		Goal: assignment.GetGoal(), Input: json.RawMessage(assignment.GetWorkloadSpecJson()),
		Capabilities: capabilities,
	}
	if _, err := runtimeClient.Start(executionCtx, start); err != nil {
		if keeper.Cancelled() {
			return true, nil
		}
		return w.fail(ctx, identity, version, "adapter_start_failed", err)
	}
	checkpointSequence := 0
	result, events, err := w.wait(executionCtx, runtimeClient, identity.GetAttemptId(),
		time.Duration(checkpointPolicy.IntervalSeconds)*time.Second, func() error {
			checkpointSequence++
			nextVersion, checkpointErr := w.commitLogicalCheckpoint(executionCtx, runtimeClient, assignment, target,
				checkpointPolicy, version, fmt.Sprintf("checkpoint-%d", checkpointSequence))
			if checkpointErr == nil {
				version = nextVersion
			}
			return checkpointErr
		})
	if err != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = runtimeClient.Stop(stopCtx, identity.GetAttemptId())
		stopCancel()
		if keeper.Cancelled() {
			return true, nil
		}
		if fenceErr := keeper.FenceError(); fenceErr != nil {
			return false, fenceErr
		}
		return w.fail(ctx, identity, version, "adapter_execution_failed", err)
	}
	if result.Status != agent.StatusSucceeded {
		return w.fail(ctx, identity, version, "adapter_result_"+strings.ToLower(result.Status),
			fmt.Errorf("%s: %s", result.ErrorCode, result.Error))
	}
	if keeper.Cancelled() {
		return true, nil
	}
	if checkpointPolicy.Mode == agentversion.CheckpointLogical {
		version, err = w.commitLogicalCheckpoint(executionCtx, runtimeClient, assignment, target, checkpointPolicy, version, "checkpoint-final")
		if err != nil {
			return w.fail(ctx, identity, version, "adapter_checkpoint_failed", err)
		}
	}
	eventSummary := map[string]any{"count": len(events)}
	if len(events) > 0 {
		eventSummary["lastSequence"] = events[len(events)-1].Sequence
	}
	safeOutput, ok := redact.RedactJSON(result.Output)
	if !ok {
		return w.fail(ctx, identity, version, "adapter_invalid_result", fmt.Errorf("runtime result is not valid JSON"))
	}
	resultDocument, err := json.Marshal(map[string]any{
		"agentVersionRef": assignment.GetAgentVersionRef(), "attemptId": identity.GetAttemptId(),
		"provider": ProviderName, "runtimeABI": target.RuntimeABI,
		"output": json.RawMessage(safeOutput), "eventSummary": eventSummary, "resumed": assignment.GetResumeCheckpoint() != nil,
		"checkpointMode": checkpointPolicy.Mode,
	})
	if err != nil {
		return false, err
	}
	if len(resultDocument) > maxResultBytes {
		return w.fail(ctx, identity, version, "adapter_result_too_large", fmt.Errorf("adapter result exceeds the durable artifact limit"))
	}
	resultArtifact, err := w.artifacts.Put(executionCtx, w.tenantID, resultMediaType, bytes.NewReader(resultDocument))
	if err != nil {
		return false, fmt.Errorf("persist adapter result: %w", err)
	}
	if _, err := w.control.CompleteAttempt(executionCtx, &runtimev1.CompleteAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: operationKey(assignment, "complete"), Result: artifactProto(resultArtifact),
	}); err != nil {
		return false, fmt.Errorf("complete adapter attempt: %w", err)
	}
	return true, nil
}

func memorySensitivities(configured []string) []string {
	if len(configured) == 0 {
		return []string{"internal"}
	}
	return cloneExplicit(configured)
}

func (w *Worker) target(assignment *runtimev1.Assignment) (agentversion.RuntimeTarget, agent.CapabilityGrant, agentversion.CheckpointPolicy, error) {
	var spec agentversion.Spec
	if err := json.Unmarshal(assignment.GetAgentVersionSpecJson(), &spec); err != nil {
		return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("decode immutable AgentVersion spec: %w", err)
	}
	for _, target := range spec.Runtimes {
		if target.Class != assignment.GetRuntimeClass() {
			continue
		}
		if (target.Interface != agentversion.RuntimeInterfaceV1 && target.Interface != agentversion.RuntimeInterfaceV1Alpha1) || len(target.Entrypoint) == 0 {
			return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{},
				fmt.Errorf("runtime target does not declare a supported interface and logical entrypoint")
		}
		if spec.Capabilities == nil {
			return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("capability declaration is required")
		}
		if spec.Checkpoint == nil {
			return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("checkpoint policy is required")
		}
		return target, agent.CapabilityGrant{
			Tools: cloneExplicit(spec.Capabilities.Tools), Models: cloneExplicit(spec.Capabilities.Models),
			Memory: cloneExplicit(spec.Capabilities.Memory), Secrets: cloneExplicit(spec.Capabilities.Secrets),
			MemorySensitivities: memorySensitivities(spec.Capabilities.MemorySensitivities),
			SpawnTasks:          spec.Capabilities.SpawnTasks, ChildAgents: cloneExplicit(spec.Capabilities.ChildAgents),
		}, *spec.Checkpoint, nil
	}
	return agentversion.RuntimeTarget{}, agent.CapabilityGrant{}, agentversion.CheckpointPolicy{}, fmt.Errorf("no runtime target for class %q", assignment.GetRuntimeClass())
}

func cloneExplicit(values []string) []string {
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func parseAssignmentIdentity(assignment *runtimev1.Assignment) (uuid.UUID, uuid.UUID, uuid.UUID, error) {
	values := []struct {
		name  string
		value string
	}{{"taskId", assignment.GetTaskId()}, {"runId", assignment.GetRunId()}, {"attemptId", assignment.GetIdentity().GetAttemptId()}}
	parsed := make([]uuid.UUID, len(values))
	for index, value := range values {
		id, err := uuid.Parse(value.value)
		if err != nil || id == uuid.Nil {
			return uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("%s must be a non-zero UUID", value.name)
		}
		parsed[index] = id
	}
	return parsed[0], parsed[1], parsed[2], nil
}

// wait observes one execution to its terminal state. Stream-capable
// runtimes hold a single long-lived event-stream connection (one HTTP
// request instead of a poll cycle, with the terminal frame carrying the
// result); a stream that drops mid-flight reconnects from the last consumed
// sequence (bounded attempts, no event loss or duplication) and finally
// falls back to the frozen v1 polling endpoints for v1-only runtimes,
// resuming from the stream cursor.
func (w *Worker) wait(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error) (agent.Result, []agent.Event, error) {
	streamed := w.waitStreaming(ctx, client, executionID, checkpointInterval, checkpoint)
	if !streamed.unsupported {
		return streamed.result, streamed.events, streamed.err
	}
	return w.waitPolling(ctx, client, executionID, checkpointInterval, checkpoint, streamed.events, streamed.after)
}

type streamOutcome struct {
	result      agent.Result
	events      []agent.Event
	after       int64
	unsupported bool
	err         error
}

// streamReconnects bounds mid-flight stream reconnects before the worker
// degrades to polling; streamReconnectBase/Max bound the backoff between
// attempts.
const (
	streamReconnects    = 3
	streamReconnectBase = 250 * time.Millisecond
	streamReconnectMax  = 2 * time.Second
)

func (w *Worker) waitStreaming(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error) streamOutcome {
	var events []agent.Event
	var after int64
	for attempt := 0; ; attempt++ {
		attempted := w.streamOnce(ctx, client, executionID, checkpointInterval, checkpoint, &events, &after)
		if !attempted.retry {
			return attempted.outcome
		}
		if attempt >= streamReconnects {
			// The stream keeps dropping: hand the cursor and the events
			// collected so far to the polling fallback.
			return streamOutcome{events: events, after: after, unsupported: true}
		}
		delay := streamReconnectBase << attempt
		if delay > streamReconnectMax {
			delay = streamReconnectMax
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return streamOutcome{events: events, after: after, err: ctx.Err()}
		case <-timer.C:
		}
	}
}

// streamAttempt is the outcome of one stream connection attempt; retry says
// whether the worker may reconnect from the cursor.
type streamAttempt struct {
	outcome streamOutcome
	retry   bool
}

// streamOnce holds one stream connection to its terminal frame. A dropped
// connection is retryable from the cursor; the events already collected and
// their last sequence carry across attempts.
func (w *Worker) streamOnce(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error, events *[]agent.Event, after *int64) streamAttempt {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan streamAttempt, 1)
	go func() {
		result, err := client.StreamEvents(streamCtx, executionID, *after, func(event agent.Event) error {
			if len(*events) >= maxCollectedEvents {
				return fmt.Errorf("runtime event history exceeds %d events", maxCollectedEvents)
			}
			*events = append(*events, event)
			*after = event.Sequence
			return nil
		})
		switch {
		case errors.Is(err, agent.ErrStreamingUnsupported):
			done <- streamAttempt{outcome: streamOutcome{events: *events, after: *after, unsupported: true}}
		case err != nil && ctx.Err() == nil:
			// A mid-flight disconnect resumes from the cursor without
			// re-delivering consumed events (they are observations, never
			// side-effect triggers, so a replay would be safe - but the
			// cursor keeps the history exact).
			done <- streamAttempt{outcome: streamOutcome{events: *events, after: *after, err: err}, retry: true}
		default:
			done <- streamAttempt{outcome: streamOutcome{result: result, events: *events, err: err}}
		}
	}()
	var checkpointTicker *time.Ticker
	var checkpointTick <-chan time.Time
	if checkpointInterval > 0 {
		checkpointTicker = time.NewTicker(checkpointInterval)
		checkpointTick = checkpointTicker.C
		defer checkpointTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return streamAttempt{outcome: streamOutcome{err: ctx.Err()}}
		case <-checkpointTick:
			if checkpoint == nil {
				return streamAttempt{outcome: streamOutcome{err: fmt.Errorf("periodic checkpoint callback is not configured")}}
			}
			if err := checkpoint(); err != nil {
				return streamAttempt{outcome: streamOutcome{err: fmt.Errorf("periodic checkpoint: %w", err)}}
			}
		case result := <-done:
			return result
		}
	}
}

func (w *Worker) waitPolling(ctx context.Context, client *agent.Client, executionID string, checkpointInterval time.Duration, checkpoint func() error, events []agent.Event, after int64) (agent.Result, []agent.Event, error) {
	pollDelay := w.pollInterval
	if pollDelay <= 0 {
		pollDelay = defaultPollInterval
	}
	pollTimer := time.NewTimer(0)
	defer pollTimer.Stop()
	var checkpointTicker *time.Ticker
	var checkpointTick <-chan time.Time
	if checkpointInterval > 0 {
		checkpointTicker = time.NewTicker(checkpointInterval)
		checkpointTick = checkpointTicker.C
		defer checkpointTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return agent.Result{}, nil, ctx.Err()
		case <-pollTimer.C:
		case <-checkpointTick:
			if checkpoint == nil {
				return agent.Result{}, nil, fmt.Errorf("periodic checkpoint callback is not configured")
			}
			if err := checkpoint(); err != nil {
				return agent.Result{}, nil, fmt.Errorf("periodic checkpoint: %w", err)
			}
			// Checkpoint cadence is independent from result polling. Resetting
			// the poll timer here lets a checkpoint interval shorter than the
			// adaptive poll delay postpone result observation forever.
			continue
		}
		list, err := client.Events(ctx, executionID, after)
		if err != nil {
			return agent.Result{}, nil, err
		}
		events = append(events, list.Events...)
		if len(events) > maxCollectedEvents {
			return agent.Result{}, nil, fmt.Errorf("runtime event history exceeds %d events", maxCollectedEvents)
		}
		after = list.NextAfter
		result, terminal, err := client.Result(ctx, executionID)
		if err != nil {
			return agent.Result{}, nil, err
		}
		if terminal {
			return result, events, nil
		}
		if len(list.Events) > 0 {
			pollDelay = w.pollInterval
			if pollDelay <= 0 {
				pollDelay = defaultPollInterval
			}
		} else if pollDelay < maxPollInterval {
			pollDelay *= 2
			if pollDelay > maxPollInterval {
				pollDelay = maxPollInterval
			}
		}
		pollTimer.Reset(pollDelay)
	}
}

func (w *Worker) commitLogicalCheckpoint(
	ctx context.Context,
	client *agent.Client,
	assignment *runtimev1.Assignment,
	target agentversion.RuntimeTarget,
	policy agentversion.CheckpointPolicy,
	version int64,
	operation string,
) (int64, error) {
	checkpoint, err := client.Checkpoint(ctx, assignment.GetIdentity().GetAttemptId())
	if err != nil {
		return version, err
	}
	if checkpoint.Checkpoint.SchemaVersion != policy.SchemaVersion {
		return version, fmt.Errorf("runtime checkpoint schema %q does not match manifest schema %q",
			checkpoint.Checkpoint.SchemaVersion, policy.SchemaVersion)
	}
	trimmedState := bytes.TrimSpace(checkpoint.Checkpoint.State)
	if len(trimmedState) == 0 || bytes.Equal(trimmedState, []byte("{}")) {
		// An empty checkpoint state contains no recoverable progress. Do not
		// commit an empty checkpoint that would clobber a previous confirmed
		// checkpoint.
		return version, nil
	}
	document, err := json.Marshal(checkpoint.Checkpoint)
	if err != nil {
		return version, err
	}
	artifact, err := w.artifacts.Put(ctx, w.tenantID, checkpointMediaType, bytes.NewReader(document))
	if err != nil {
		return version, fmt.Errorf("persist adapter checkpoint: %w", err)
	}
	checkpointID, err := uuid.NewV7()
	if err != nil {
		return version, err
	}
	committed, err := w.control.CommitCheckpoint(ctx, &runtimev1.CommitCheckpointRequest{
		Identity: assignment.GetIdentity(), ExpectedAttemptVersion: version,
		IdempotencyKey: operationKey(assignment, operation), CheckpointId: checkpointID.String(),
		AgentVersionRef: assignment.GetAgentVersionRef(), Provider: ProviderName,
		RuntimeAbi: target.RuntimeABI, SchemaVersion: CheckpointSchema, State: artifactProto(artifact),
	})
	if err != nil {
		return version, fmt.Errorf("commit adapter checkpoint: %w", err)
	}
	return committed.GetAttemptVersion(), nil
}

func (w *Worker) restore(ctx context.Context, client *agent.Client, assignment *runtimev1.Assignment, target agentversion.RuntimeTarget, policy agentversion.CheckpointPolicy) error {
	checkpoint := assignment.GetResumeCheckpoint()
	if checkpoint.GetAgentVersionRef() != assignment.GetAgentVersionRef() ||
		checkpoint.GetRuntimeClass() != assignment.GetRuntimeClass() ||
		checkpoint.GetProvider() != ProviderName ||
		checkpoint.GetRuntimeAbi() != target.RuntimeABI ||
		checkpoint.GetSchemaVersion() != CheckpointSchema {
		return fmt.Errorf("checkpoint is incompatible with adapter assignment")
	}
	reference, err := artifactFromProto(checkpoint.GetState())
	if err != nil {
		return err
	}
	reader, err := w.artifacts.Open(ctx, w.tenantID, reference)
	if err != nil {
		return err
	}
	defer reader.Close()
	decoder := json.NewDecoder(io.LimitReader(reader, 2<<20))
	decoder.DisallowUnknownFields()
	var sdkCheckpoint agent.Checkpoint
	if err := decoder.Decode(&sdkCheckpoint); err != nil {
		return fmt.Errorf("decode adapter checkpoint: %w", err)
	}
	if sdkCheckpoint.SchemaVersion != policy.SchemaVersion {
		return fmt.Errorf("checkpoint logical schema does not match the AgentVersion manifest")
	}
	_, err = client.Restore(ctx, agent.RestoreRequest{
		ExecutionID: assignment.GetIdentity().GetAttemptId(), Checkpoint: sdkCheckpoint,
	})
	return err
}

func (w *Worker) heartbeat(ctx context.Context, assignment *runtimev1.Assignment) (*runtimev1.HeartbeatResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()
	response, err := w.control.Heartbeat(rpcCtx, &runtimev1.HeartbeatRequest{
		Identity: assignment.GetIdentity(), ExpectedLeaseVersion: assignment.GetLeaseVersion(),
		IdempotencyKey: operationKey(assignment, "heartbeat"), RequestedTtlSeconds: int64(w.heartbeatTTL / time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("renew adapter lease: %w", err)
	}
	if response.GetCancelRequested() {
		if err := w.acknowledgeCancellation(ctx, assignment.GetIdentity(), response.GetAttemptVersion()); err != nil {
			return nil, fmt.Errorf("acknowledge adapter cancellation: %w", err)
		}
	}
	return response, nil
}

func (w *Worker) transition(
	ctx context.Context,
	identity *runtimev1.AttemptIdentity,
	version int64,
	phase runtimev1.AttemptPhase,
	failureCode, failureMessage string,
) (int64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
	defer cancel()
	response, err := w.control.TransitionAttempt(rpcCtx, &runtimev1.TransitionAttemptRequest{
		Identity: identity, ExpectedAttemptVersion: version,
		IdempotencyKey: identity.GetAttemptId() + ":" + strings.ToLower(phase.String()),
		TargetPhase:    phase, FailureCode: failureCode, FailureMessage: failureMessage,
	})
	if err != nil {
		return 0, fmt.Errorf("transition adapter attempt to %s: %w", phase, err)
	}
	return response.GetAttemptVersion(), nil
}

// advanceAttempt converges a normal lifecycle transition after an optimistic
// concurrency conflict without weakening fencing. A conflict is retried only
// while the fenced Attempt remains in the expected predecessor phase. If the
// requested transition already committed, its refreshed version is accepted;
// cancellation, terminal state, or an unrelated phase always wins and tells
// the caller not to start another execution for the same assignment.
func (w *Worker) advanceAttempt(
	ctx context.Context,
	identity *runtimev1.AttemptIdentity,
	version int64,
	from, to domain.AttemptPhase,
	protoPhase runtimev1.AttemptPhase,
) (int64, bool, error) {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		nextVersion, err := w.transition(ctx, identity, version, protoPhase, "", "")
		if err == nil {
			return nextVersion, true, nil
		}
		// The assignment lease expired or a recovery controller installed a
		// newer fenced owner. This worker no longer owns the Attempt; dropping
		// the assignment is successful convergence, not a process-fatal error.
		if status.Code(err) == codes.PermissionDenied {
			return 0, false, nil
		}
		if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return 0, false, err
		}
		current, refreshErr := attemptstate.Refresh(ctx, w.control, identity, controlRPCTimeout)
		if refreshErr != nil {
			return 0, false, fmt.Errorf("converge adapter attempt to %s: %w", protoPhase, refreshErr)
		}
		switch {
		case current.Phase == domain.AttemptCancelRequested:
			if cancelErr := w.acknowledgeCancellation(ctx, identity, current.Version); cancelErr != nil {
				return 0, false, fmt.Errorf("converge adapter cancellation: %w", cancelErr)
			}
			return current.Version, false, nil
		case current.Phase.Terminal():
			return current.Version, false, nil
		case current.Phase == to:
			return current.Version, true, nil
		case current.Phase == from:
			version = current.Version
		default:
			return current.Version, false, nil
		}
	}
}

// acknowledgeCancellation settles a cancellation observed before or during
// execution. It converges a stale Attempt version through the fenced read API
// so cancellation cannot strand an assignment between placement and the first
// heartbeat, while a terminal or unrelated control-plane state always wins.
func (w *Worker) acknowledgeCancellation(ctx context.Context, identity *runtimev1.AttemptIdentity, version int64) error {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		rpcCtx, cancel := context.WithTimeout(ctx, controlRPCTimeout)
		_, err := w.control.AcknowledgeCancellation(rpcCtx, &runtimev1.AcknowledgeCancellationRequest{
			Identity: identity, ExpectedAttemptVersion: version,
			IdempotencyKey: identity.GetAttemptId() + ":adapter:cancel",
		})
		cancel()
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return err
		}
		current, refreshErr := attemptstate.Refresh(ctx, w.control, identity, controlRPCTimeout)
		if refreshErr != nil {
			return refreshErr
		}
		if current.Phase.Terminal() || current.Phase != domain.AttemptCancelRequested {
			return nil
		}
		version = current.Version
	}
}

func (w *Worker) fail(
	ctx context.Context,
	identity *runtimev1.AttemptIdentity,
	version int64,
	code string,
	cause error,
) (bool, error) {
	const maxConflictRetries = 3
	for conflict := 0; ; conflict++ {
		if _, err := w.transition(ctx, identity, version,
			runtimev1.AttemptPhase_ATTEMPT_PHASE_FAILED, code, cause.Error()); err == nil {
			return true, nil
		} else if status.Code(err) != codes.Aborted || conflict >= maxConflictRetries {
			return false, fmt.Errorf("mark adapter attempt failed (%s): %w", code, err)
		}
		// Approval, checkpoint, cancellation, and deadline controllers may
		// legitimately advance the Attempt while execution is in flight. A
		// definite CAS rejection is safe to resolve by re-reading through the
		// same fenced identity. Cancellation/terminal state always wins.
		current, err := attemptstate.Refresh(ctx, w.control, identity, controlRPCTimeout)
		if err != nil {
			return false, fmt.Errorf("mark adapter attempt failed (%s): %w", code, err)
		}
		if current.Settled() {
			return true, nil
		}
		version = current.Version
	}
}

func operationKey(assignment *runtimev1.Assignment, operation string) string {
	return assignment.GetIdentity().GetAttemptId() + ":adapter:" + operation
}

func artifactProto(reference store.ArtifactReference) *runtimev1.ArtifactReference {
	return &runtimev1.ArtifactReference{
		Uri: reference.URI, Sha256: reference.DigestHex(), SizeBytes: reference.SizeBytes, MediaType: reference.MediaType,
	}
}

func artifactFromProto(reference *runtimev1.ArtifactReference) (store.ArtifactReference, error) {
	var result store.ArtifactReference
	if reference == nil {
		return result, fmt.Errorf("checkpoint artifact reference is required")
	}
	digest, err := hex.DecodeString(reference.GetSha256())
	if err != nil || len(digest) != sha256.Size {
		return result, fmt.Errorf("checkpoint artifact digest is invalid")
	}
	copy(result.SHA256[:], digest)
	result.URI, result.SizeBytes, result.MediaType = reference.GetUri(), reference.GetSizeBytes(), reference.GetMediaType()
	return result, result.Validate()
}

// workflowLineage is the parsed workflow origin of one assignment
// (workflow_id/step_name/version); workflowID is nil for standalone tasks.
type workflowLineage struct {
	workflowID      uuid.UUID
	stepName        string
	workflowVersion int64
}

// parseWorkflowLineage decodes the assignment's workflow lineage token.
func parseWorkflowLineage(value string) workflowLineage {
	if value == "" {
		return workflowLineage{}
	}
	parts := strings.Split(value, "/")
	if len(parts) != 3 {
		return workflowLineage{}
	}
	id, err := uuid.Parse(parts[0])
	if err != nil {
		return workflowLineage{}
	}
	version, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return workflowLineage{}
	}
	return workflowLineage{workflowID: id, stepName: parts[1], workflowVersion: version}
}
