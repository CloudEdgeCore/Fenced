package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/tool"
	"github.com/google/uuid"
	"google.golang.org/grpc/status"
)

var toolEndpointHTTPCode = regexp.MustCompile(`^TOOL_ENDPOINT_HTTP_[1-5][0-9]{2}$`)

// AttemptContext is the fenced execution identity an MCP call is bound to.
// The adapter never trusts MCP-supplied identity: it comes from the resolver,
// which in production is injected by the runtime that owns the Attempt.
type AttemptContext struct {
	TenantID        string
	TaskID          uuid.UUID
	RunID           uuid.UUID
	AttemptID       uuid.UUID
	FencingToken    int64
	AgentVersionRef string
	// WorkflowID names the workflow run this attempt's task belongs to
	// (empty for standalone tasks); the brokered spawn tool extends that
	// run with dynamic steps.
	WorkflowID uuid.UUID
	// WorkflowVersion is the observed resource version of the workflow run
	// at assignment time.
	WorkflowVersion int64
	// ParentStepName names the workflow step this attempt's task executes
	// (empty for standalone tasks); spawned steps carry it as lineage.
	ParentStepName string
	// AllowedModels is the AgentVersion's declared model capability grant;
	// the brokered model tool denies everything outside it (default deny).
	AllowedModels []string
	// AllowedMemoryNamespaces is the declared memory capability grant; the
	// brokered memory tools deny writes and reads outside it.
	AllowedMemoryNamespaces []string
	// AllowedMemorySensitivities is independent of namespace access; absent
	// grants default to internal only.
	AllowedMemorySensitivities []string
	// CanSpawnTasks is the explicit AgentVersion capability for dynamic
	// workflow expansion. It defaults to false.
	CanSpawnTasks bool
	// AllowedChildAgents is the immutable allowlist of AgentVersion refs the
	// calling version may spawn. An empty list denies every child.
	AllowedChildAgents []string
}

// IdentityResolver supplies the fenced identity for inbound MCP calls.
type IdentityResolver interface {
	Resolve(context.Context) (AttemptContext, error)
}

// ExecutionIdentityResolver resolves the identity of one open execution
// window from the execution id the agent reports; shared Agent endpoints
// use it to keep concurrent attempts correctly fenced.
type ExecutionIdentityResolver interface {
	ResolveExecution(context.Context, string) (AttemptContext, error)
}

// StaticIdentity is a fixed identity for deployments where the attempt
// context is known at construction (tests and explicitly wired dev setups).
type StaticIdentity struct {
	Context AttemptContext
}

func (s StaticIdentity) Resolve(context.Context) (AttemptContext, error) { return s.Context, nil }

// ToolInvoker is the fenced decision chain MCP calls flow through. The
// tool.Gateway satisfies it.
type ToolInvoker interface {
	InvokeTool(context.Context, tool.InvokeInput) (tool.InvokeResult, error)
	ListTools(context.Context, string) ([]store.ToolDescriptor, error)
}

type versionedToolLister interface {
	ListToolsForAgent(context.Context, string, string) ([]store.ToolDescriptor, error)
}

// ToolAdapter exposes a Tool Gateway as an MCP tools server. Mapping rules
// (documented, v0.1):
//   - MCP tool name is the descriptor name; version and risk ride in the
//     description, and calls resolve the latest granted version. tools/list
//     shows exactly one entry per name (P1-02): the resolved default version,
//     so the model never sees two same-named tools, while an explicit
//     "name@version" reference still pins any granted older version.
//   - action defaults to the descriptor's first declared action ("invoke"
//     when none), resource to the first resource pattern ("tool:<name>"
//     when none).
//   - the idempotency key is derived deterministically from the attempt, the
//     resolved tool version and the canonical arguments, so identical calls
//     replay instead of repeating an external side effect while different
//     versions of one tool never share replay semantics (at-least-once +
//     idempotent consumer).
type ToolAdapter struct {
	invoker  ToolInvoker
	identity IdentityResolver
}

func NewToolAdapter(invoker ToolInvoker, identity IdentityResolver) *ToolAdapter {
	return &ToolAdapter{invoker: invoker, identity: identity}
}

// ListTools implements tools/list.
func (a *ToolAdapter) ListTools(ctx context.Context, params json.RawMessage) (any, *Error) {
	if err := rejectUnknownFields(params, new(any), true); err != nil {
		return nil, invalidParams("unknown cursor field")
	}
	identity, err := resolveAttemptIdentity(ctx, a.identity)
	if err != nil {
		return nil, unauthorizedError(err)
	}
	descriptors, err := a.listTools(ctx, identity)
	if err != nil {
		return nil, &Error{Code: codeInternalError, Message: "list tools"}
	}
	// P1-02: the model-facing listing shows exactly one entry per name —
	// the same latest granted version a bare-name call resolves to — while
	// CallTool still accepts explicit "name@version" pins of older granted
	// versions.
	tools := make([]map[string]any, 0, len(descriptors))
	for _, descriptor := range tool.LatestVersionPerName(descriptors) {
		tools = append(tools, map[string]any{
			"name":        descriptor.Name,
			"description": fmt.Sprintf("%s (version %s, sideEffectRisk %s)", descriptor.Name, descriptor.Version, descriptor.SideEffectRisk),
			"inputSchema": json.RawMessage(descriptor.ParamsSchema),
		})
	}
	return map[string]any{"tools": tools}, nil
}

// unauthorizedError reports a call outside any fenced execution window.
func unauthorizedError(err error) *Error {
	return &Error{Code: codeUnauthorized, Message: "no fenced attempt identity", Data: err.Error()}
}

// CallTool implements tools/call.
func (a *ToolAdapter) CallTool(ctx context.Context, params json.RawMessage) (any, *Error) {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments,omitempty"`
	}
	if err := rejectUnknownFields(params, &call, false); err != nil {
		return nil, invalidParams("name and arguments are required")
	}
	if strings.TrimSpace(call.Name) == "" {
		return nil, invalidParams("name is required")
	}
	if len(call.Arguments) > 0 && !json.Valid(call.Arguments) {
		return nil, invalidParams("arguments must be a JSON object")
	}
	identity, err := resolveAttemptIdentity(ctx, a.identity)
	if err != nil {
		// Default deny: without a fenced attempt identity the call is
		// refused as a tool outcome, not a protocol failure.
		return toolErrorResult("no fenced attempt identity"), nil
	}
	descriptors, err := a.listTools(ctx, identity)
	if err != nil {
		return nil, &Error{Code: codeInternalError, Message: "resolve tool descriptor"}
	}
	// A "name@version" reference pins one exact granted version (P1-08);
	// a bare name resolves the latest granted version.
	toolName, toolVersion := call.Name, ""
	if name, version, pinned := strings.Cut(call.Name, "@"); pinned {
		toolName, toolVersion = name, version
	}
	descriptor, ok := resolveToolVersion(descriptors, toolName, toolVersion)
	if !ok {
		return nil, invalidParams("unknown tool: " + call.Name)
	}
	action := firstOr(descriptor.Actions, "invoke")
	resource := firstOr(descriptor.ResourcePatterns, "tool:"+descriptor.Name)

	args := call.Arguments
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	result, err := a.invoker.InvokeTool(ctx, tool.InvokeInput{
		TenantID: identity.TenantID, TaskID: identity.TaskID, RunID: identity.RunID,
		AttemptID: identity.AttemptID, FencingToken: identity.FencingToken,
		AgentVersionRef: identity.AgentVersionRef,
		ToolName:        descriptor.Name, ToolVersion: descriptor.Version, Action: action, Resource: resource,
		Args: args, IdempotencyKey: mcpIdempotencyKey(identity, descriptor.Name, descriptor.Version, args),
	})
	if err != nil {
		failureCode := "TOOL_INVOCATION_FAILED"
		// The gateway exposes only a bounded machine code in the gRPC status;
		// never reflect arbitrary downstream error text into MCP responses.
		if grpcStatus, ok := status.FromError(err); ok && toolEndpointHTTPCode.MatchString(grpcStatus.Message()) {
			failureCode = grpcStatus.Message()
		}
		return toolErrorResult(failureCode), nil
	}
	switch result.Outcome {
	case tool.OutcomeExecuted, tool.OutcomeReplayed:
		return textResult(result.Result, false), nil
	case tool.OutcomeDenied:
		denied, _ := json.Marshal(map[string]any{"denied": true, "denyReasons": result.DenyReasons})
		return textResult(denied, true), nil
	case tool.OutcomeRequiresApproval:
		text, _ := json.Marshal(map[string]any{
			"requiresApproval": true,
			"approvalId":       result.ApprovalID.String(),
		})
		return textResult(text, true), nil
	default:
		return toolErrorResult("unexpected outcome: " + string(result.Outcome)), nil
	}
}

func (a *ToolAdapter) listTools(ctx context.Context, identity AttemptContext) ([]store.ToolDescriptor, error) {
	if versioned, ok := a.invoker.(versionedToolLister); ok {
		return versioned.ListToolsForAgent(ctx, identity.TenantID, identity.AgentVersionRef)
	}
	return a.invoker.ListTools(ctx, identity.TenantID)
}

// textResult wraps an output document as an MCP text content item.
func textResult(text []byte, isError bool) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": string(text)}},
		"isError": isError,
	}
}

func toolErrorResult(message string) map[string]any {
	text, _ := json.Marshal(map[string]any{"error": message})
	return textResult(text, true)
}

// mcpIdempotencyKey derives a deterministic key from the attempt identity,
// the resolved tool version, and the canonical arguments, so identical MCP
// calls replay instead of repeating an external side effect (P1-01). The
// tool version is folded into the digest: the same arguments against two
// versions of one tool must never share idempotency semantics, because the
// tool_calls unique scope is (tenant, attempt, tool_name, idempotency_key)
// and a bare-name call can resolve to a different version than an earlier
// pinned call within the same attempt.
func mcpIdempotencyKey(identity AttemptContext, toolName, toolVersion string, args json.RawMessage) string {
	var document any
	if err := json.Unmarshal(args, &document); err != nil {
		document = args
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		canonical = args
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(toolName))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(toolVersion))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(canonical)
	return fmt.Sprintf("mcp/%s/%s/%s", identity.AttemptID, toolName, hex.EncodeToString(digest.Sum(nil)))
}

func latestDescriptor(descriptors []store.ToolDescriptor, name string) (store.ToolDescriptor, bool) {
	var latest store.ToolDescriptor
	found := false
	for _, descriptor := range descriptors {
		if descriptor.Name == name && (!found || tool.CompareToolVersions(descriptor.Version, latest.Version) > 0) {
			latest, found = descriptor, true
		}
	}
	return latest, found
}

// resolveToolVersion resolves a tool reference against the granted
// descriptors: the latest granted version when no version is pinned, the
// exact version when one is (a pinned reference to a version the agent is
// not granted resolves to nothing).
func resolveToolVersion(descriptors []store.ToolDescriptor, name, version string) (store.ToolDescriptor, bool) {
	if version == "" {
		return latestDescriptor(descriptors, name)
	}
	for _, descriptor := range descriptors {
		if descriptor.Name == name && descriptor.Version == version {
			return descriptor, true
		}
	}
	return store.ToolDescriptor{}, false
}

func firstOr(values []string, fallback string) string {
	if len(values) == 0 {
		return fallback
	}
	return values[0]
}

// rejectUnknownFields strictly decodes a JSON-RPC params document, rejecting
// unknown fields, duplicate keys and trailing values.
func rejectUnknownFields(params json.RawMessage, target any, allowEmpty bool) error {
	if len(params) == 0 {
		if allowEmpty {
			return nil
		}
		return fmt.Errorf("params are required")
	}
	decoder := json.NewDecoder(bytes.NewReader(params))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureEOF(decoder)
}
