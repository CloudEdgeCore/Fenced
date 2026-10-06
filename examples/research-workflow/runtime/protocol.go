// Package research implements the seven agent roles of the multi-agent
// research workflow as one Fenced-native runtime. Every role executes
// inside the fenced attempt context the platform injects: models are invoked
// through the brokered fenced.model.invoke system tool, side effects go
// through tenant tools behind the Tool Gateway, artifacts land in namespaced
// memory, and dynamic fan-out happens through fenced.task.spawn - the
// runtime never talks to providers, stores or the kernel directly.
package research

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// MCPClient is the brokered tool surface of one attempt. The execution id is
// the attempt-scoped window key the runtime worker opened for this agent;
// every call carries it so concurrent attempts stay fenced.
type MCPClient interface {
	CallTool(ctx context.Context, executionID, name string, args any) (json.RawMessage, error)
}

// Models binds the three logical capability tiers to concrete gateway model
// references for this deployment.
type Models struct {
	Fast      string
	Reader    string
	Reasoning string
}

// ChatMessage mirrors the brokered message shape.
type ChatMessage struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCallID string     `json:"toolCallId,omitempty"`
	ToolCalls  []ChatCall `json:"toolCalls,omitempty"`
}

type ChatCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ChatTurn is one model invocation result.
type ChatTurn struct {
	Content   string
	ToolCalls []ChatCall
}

// Research roles return structured analyses and reports that routinely exceed
// the gateway's conservative 512-token default. Reserve an explicit bounded
// completion envelope so valid JSON is not cut off mid-document.
const (
	researchMaxOutputTokens = 4096
	writerMaxOutputTokens   = 2048
)

// InvokeModel runs one governed model invocation through the broker,
// resolving the logical tier to this deployment's reference first.
func InvokeModel(ctx context.Context, mcp MCPClient, executionID, modelRef string, messages []ChatMessage) (ChatTurn, error) {
	return invokeModelWithLimit(ctx, mcp, executionID, modelRef, messages, researchMaxOutputTokens)
}

func invokeModelWithLimit(ctx context.Context, mcp MCPClient, executionID, modelRef string, messages []ChatMessage, maxOutputTokens int) (ChatTurn, error) {
	if maxOutputTokens <= 0 || maxOutputTokens > researchMaxOutputTokens {
		return ChatTurn{}, fmt.Errorf("model maxOutputTokens %d is outside (0,%d]", maxOutputTokens, researchMaxOutputTokens)
	}
	response, err := mcp.CallTool(ctx, executionID, "fenced.model.invoke", map[string]any{
		"modelRef":        modelRef,
		"messages":        messages,
		"maxOutputTokens": maxOutputTokens,
		"stream":          false,
	})
	if err != nil {
		return ChatTurn{}, fmt.Errorf("model invoke: %w", err)
	}
	var document struct {
		Content           string     `json:"content"`
		ToolCalls         []ChatCall `json:"toolCalls"`
		FinishReason      string     `json:"finishReason"`
		Status            string     `json:"status"`
		ProviderRequestID string     `json:"providerRequestId"`
		Error             string     `json:"error"`
	}
	if err := json.Unmarshal(response, &document); err != nil {
		return ChatTurn{}, fmt.Errorf("decode model result: %w", err)
	}
	if document.Error != "" {
		return ChatTurn{}, fmt.Errorf("model outcome %s: %s", document.Status, document.Error)
	}
	if document.FinishReason == "length" || document.FinishReason == "max_tokens" {
		return ChatTurn{}, fmt.Errorf("model output truncated at %d tokens", maxOutputTokens)
	}
	return ChatTurn{Content: document.Content, ToolCalls: document.ToolCalls}, nil
}

// CallTenantTool invokes a tenant tool by name through the broker and fails
// when the tool reports an error outcome (denials, endpoint failures).
func CallTenantTool(ctx context.Context, mcp MCPClient, executionID, name string, args any) (json.RawMessage, error) {
	raw, err := mcp.CallTool(ctx, executionID, name, args)
	if err != nil {
		return nil, fmt.Errorf("tool %s: %w", name, err)
	}
	var probe struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &probe) == nil && probe.Error != "" {
		return nil, fmt.Errorf("tool %s failed: %s", name, probe.Error)
	}
	return raw, nil
}

// PutMemory writes one record into the workflow's namespace tree.
func PutMemory(ctx context.Context, mcp MCPClient, executionID, namespace, key, contentType string, value any) error {
	content, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode memory content: %w", err)
	}
	_, err = CallTenantTool(ctx, mcp, executionID, "fenced.memory.put", map[string]any{
		"namespace": namespace, "key": key, "contentType": contentType, "content": string(content),
	})
	return err
}

// MemoryRecord mirrors the brokered memory search result row.
type MemoryRecord struct {
	ID        string `json:"id"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Content   string `json:"content"`
}

// SearchMemory retrieves records from one namespace (empty namespace lets
// the grant decide).
func SearchMemory(ctx context.Context, mcp MCPClient, executionID, namespace, query string, limit int) ([]MemoryRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	raw, err := CallTenantTool(ctx, mcp, executionID, "fenced.memory.search", map[string]any{
		"query": query, "namespace": namespace, "limit": limit,
	})
	if err != nil {
		return nil, err
	}
	var page struct {
		Records []MemoryRecord `json:"records"`
	}
	if err := json.Unmarshal(raw, &page); err != nil {
		return nil, fmt.Errorf("decode memory records: %w", err)
	}
	return page.Records, nil
}

// childRoleClass maps each spawnable child role to the runtime class it must
// run on (design plan §2.1 role mapping). The reference deployment expresses
// role affinity at spawn time: the child task's placement overlay narrows the
// workflow's declared classes to the child's class, and the scheduler places
// it on the matching pool.
var childRoleClass = map[string]string{
	"search": "research-network",
	"reader": "research-sandbox",
}

// childPlacementSpec renders the spawn spec overlay (a placement narrowing)
// for one child agent version. Unknown roles get no overlay and inherit the
// workflow default placement.
func childPlacementSpec(childRef string) map[string]any {
	class, ok := childRoleClass[RoleFromRef(childRef)]
	if !ok {
		return nil
	}
	return map[string]any{
		"placement": map[string]any{
			"runtimeClasses": []string{class},
			"preferredClass": class,
		},
	}
}

// SpawnChild extends the running workflow with one dynamic step. The child's
// placement overlay is derived from its role so the child always lands on
// its runtime class regardless of the spawning parent.
func SpawnChild(ctx context.Context, mcp MCPClient, executionID, name, childRef, goal string, maxAttempts int) error {
	args := map[string]any{
		"name": name, "goal": goal, "agentVersionRef": childRef, "maxAttempts": maxAttempts,
	}
	if spec := childPlacementSpec(childRef); spec != nil {
		args["spec"] = spec
	}
	raw, err := mcp.CallTool(ctx, executionID, "fenced.task.spawn", args)
	var outcome struct {
		Outcome string `json:"outcome"`
		Message string `json:"message"`
	}
	decoded := json.Unmarshal(raw, &outcome) == nil && outcome.Outcome != ""
	if decoded && outcome.Outcome != "created" && outcome.Outcome != "replayed" {
		return &SpawnOutcomeError{Name: name, Outcome: outcome.Outcome, Message: outcome.Message}
	}
	if err != nil {
		// MCP tool denials deliberately return both a structured payload and an
		// error outcome. Decode the payload before wrapping the transport error
		// so stable spawn codes survive the HTTP/JSON-RPC boundary.
		return fmt.Errorf("spawn %s: %w", name, err)
	}
	return nil
}

// SpawnOutcomeError preserves the stable denial code so callers can treat
// idempotent name conflicts differently from policy or budget denials.
type SpawnOutcomeError struct {
	Name    string
	Outcome string
	Message string
}

func (e *SpawnOutcomeError) Error() string {
	return fmt.Sprintf("spawn %s denied: %s (%s)", e.Name, e.Outcome, e.Message)
}

// Envelope is the structured task payload every role finds inside its goal.
// The application renders it into each static step's goal and parents embed
// it into spawned children's goals; the kernel transports it verbatim.
type Envelope struct {
	Role     string     `json:"role"`
	Goal     string     `json:"goal"`
	Workflow string     `json:"workflowId"`
	Round    int        `json:"round,omitempty"`
	Question *Question  `json:"question,omitempty"`
	Source   *SourceHit `json:"source,omitempty"`
	Verdict  string     `json:"upstreamVerdict,omitempty"`
}

// Question is one planner decomposition unit (schema-aligned field names).
type Question struct {
	ID            string   `json:"id"`
	Question      string   `json:"question"`
	Priority      int      `json:"priority"`
	SearchQueries []string `json:"searchQueries"`
}

// SourceHit is one discovered source.
type SourceHit struct {
	SourceID           string `json:"sourceId"`
	Title              string `json:"title"`
	URL                string `json:"url"`
	Snippet            string `json:"snippet,omitempty"`
	Query              string `json:"query,omitempty"`
	ResearchQuestionID string `json:"researchQuestionId,omitempty"`
	QuestionText       string `json:"questionText,omitempty"`
}

// FetchedDocument mirrors the web.fetch tool response payload.
type FetchedDocument struct {
	SourceID  string `json:"sourceId"`
	Title     string `json:"title"`
	URL       string `json:"url"`
	Content   string `json:"content"`
	FetchedAt string `json:"fetchedAt"`
}

const envelopePrefix = "FENCED-RESEARCH/v1 "

// EnvelopePrefix exposes the goal envelope marker to application publishers.
func EnvelopePrefix() string { return envelopePrefix }

// ParseEnvelope decodes the structured payload from a goal, tolerating plain
// goals by falling back to the role implied by the agent version reference.
// The kernel may append rendered upstream-result blocks AFTER the envelope,
// so decoding stops at the first JSON value instead of whole-payload parse.
func ParseEnvelope(goal, versionRef string) Envelope {
	if payload, ok := strings.CutPrefix(goal, envelopePrefix); ok {
		var envelope Envelope
		decoder := json.NewDecoder(strings.NewReader(payload))
		if err := decoder.Decode(&envelope); err == nil && envelope.Role != "" {
			envelope.Goal = goal
			return envelope
		}
	}
	return Envelope{Role: RoleFromRef(versionRef), Goal: goal}
}

// RoleFromRef maps an agent version reference onto its role name
// ("research-reader@1" -> "reader").
func RoleFromRef(versionRef string) string {
	name, _, _ := strings.Cut(versionRef, "@")
	name = strings.TrimPrefix(name, "research-")
	switch {
	case name == "planner", name == "search", name == "reader", name == "collector",
		name == "analyst", name == "critic", name == "writer":
		return name
	case name == "citation-validator":
		return "validator"
	default:
		return ""
	}
}

// ExtractUpstreamOutputs pulls the "Upstream result [step]: <json>" blocks
// the workflow renderer appends to declared-step goals, so downstream roles
// can consume dependency outputs without extra transport.
func ExtractUpstreamOutputs(goal string) map[string]string {
	outputs := map[string]string{}
	marker := "\n\nUpstream result ["
	for {
		start := strings.Index(goal, marker)
		if start < 0 {
			return outputs
		}
		rest := goal[start+len(marker):]
		end := strings.Index(rest, "]:\n")
		if end < 0 {
			return outputs
		}
		name := rest[:end]
		body := rest[end+3:]
		next := strings.Index(body, marker)
		if next >= 0 {
			outputs[name] = strings.TrimSpace(body[:next])
			goal = body[next:]
			continue
		}
		outputs[name] = strings.TrimSpace(body)
		return outputs
	}
}
