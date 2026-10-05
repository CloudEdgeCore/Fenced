// Package policy evaluates default-deny Rego policies inside the kernel
// process. Policy decisions are enforced outside the LLM and outside per-call
// application code; the input document is constructed by trusted kernel code,
// and every failure mode (missing tenant policy, evaluation error, no rule
// match) denies by default.
package policy

import (
	"context"
	_ "embed"
	"fmt"
	"sort"

	"github.com/open-policy-agent/opa/v1/rego"
)

// Revision identifies the embedded policy module set. It is recorded with
// every decision so outcomes remain auditable across policy updates.
const Revision = "2026-09-19/v1"

//go:embed fenced.rego
var moduleSource string

// TenantPolicy is the tenant-attribute data the policy rules evaluate.
type TenantPolicy struct {
	MaxPriority int `json:"max_priority"`
	// AllowedTools lists the tool names this tenant may invoke. Empty means
	// every tool is denied (default deny).
	AllowedTools []string `json:"allowed_tools"`
	// AllowedModels lists the provider/model references this tenant may call.
	// Empty means every model is denied (default deny).
	AllowedModels []string `json:"allowed_models"`
	// ApprovalRequiredRisk is the tool risk level that requires human
	// approval, for example "high".
	ApprovalRequiredRisk string `json:"approval_required_risk"`
	// AllowedPeers lists the AgentVersion references this tenant's agents may
	// send IPC messages to. Matching is exact. Empty means every send is
	// denied (default deny), so a tenant that has not opted in cannot message
	// anyone even if its AgentVersions declare peer grants.
	AllowedPeers []string `json:"allowed_peers"`
}

// TenantPolicies maps tenant IDs to their policy data. A tenant without an
// entry is denied by default.
type TenantPolicies map[string]TenantPolicy

// TaskContext carries the bounded task attributes the v1alpha1 policy reads.
type TaskContext struct {
	Priority int `json:"priority"`
}

// ToolContext is the versioned typed document the tool rules evaluate. It is
// constructed by trusted kernel code from the registered descriptor and the
// fenced invocation identity — never from raw request JSON.
type ToolContext struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Risk     string `json:"risk"`
}

// ModelContext is the typed document the model rules evaluate. Name is the
// canonical provider/model reference.
type ModelContext struct {
	Name string `json:"name"`
}

// PeerContext is the typed document the peer rules evaluate. To is the
// receiving AgentVersion reference taken from the fenced send, never from raw
// request JSON.
//
// The message kind is deliberately absent rather than passed along unused: a
// field no rule reads would suggest kinds are policy-checked when they are
// not. Kinds have no vocabulary yet.
type PeerContext struct {
	To string `json:"to"`
}

// Decision is the machine-readable policy outcome.
type Decision struct {
	Allow bool
	// RequiresApproval is set when the policy gates the action on human
	// approval; it is meaningful for tool decisions.
	RequiresApproval bool
	DenyReasons      []string
}

// Engine evaluates the embedded policy against tenant data. Construction
// compiles the module and fails closed at startup on any policy defect.
type Engine struct {
	admission preparedQuery
	tool      preparedQuery
	model     preparedQuery
	peer      preparedQuery
	tenants   TenantPolicies
}

type preparedQuery struct {
	query rego.PreparedEvalQuery
}

func New(tenants TenantPolicies) (*Engine, error) {
	return newWithModule(moduleSource, tenants)
}

func newWithModule(source string, tenants TenantPolicies) (*Engine, error) {
	admission, err := prepare(source, "data.fenced.policy")
	if err != nil {
		return nil, fmt.Errorf("prepare admission policy: %w", err)
	}
	tool, err := prepare(source, "data.fenced.policy.tool")
	if err != nil {
		return nil, fmt.Errorf("prepare tool policy: %w", err)
	}
	model, err := prepare(source, "data.fenced.policy.model")
	if err != nil {
		return nil, fmt.Errorf("prepare model policy: %w", err)
	}
	peer, err := prepare(source, "data.fenced.policy.peer")
	if err != nil {
		return nil, fmt.Errorf("prepare peer policy: %w", err)
	}
	return &Engine{admission: admission, tool: tool, model: model, peer: peer, tenants: tenants}, nil
}

func prepare(source, query string) (preparedQuery, error) {
	prepared, err := rego.New(
		rego.Query(query),
		rego.Module("fenced.rego", source),
	).PrepareForEval(context.Background())
	if err != nil {
		return preparedQuery{}, err
	}
	return preparedQuery{query: prepared}, nil
}

func (e *Engine) Evaluate(ctx context.Context, tenantID string, task TaskContext) Decision {
	tenant, ok := e.tenants[tenantID]
	if !ok {
		return Decision{DenyReasons: []string{"TENANT_POLICY_NOT_FOUND"}}
	}
	return evaluate(ctx, e.admission, map[string]any{
		"task":   map[string]any{"priority": task.Priority},
		"tenant": tenantDocument(tenant),
	})
}

// EvaluateTool decides whether a tool invocation is allowed, denied, or
// gated on human approval for the tenant.
func (e *Engine) EvaluateTool(ctx context.Context, tenantID string, tool ToolContext) Decision {
	tenant, ok := e.tenants[tenantID]
	if !ok {
		return Decision{DenyReasons: []string{"TENANT_POLICY_NOT_FOUND"}}
	}
	decision := evaluate(ctx, e.tool, map[string]any{
		"tool": map[string]any{
			"name": tool.Name, "version": tool.Version, "action": tool.Action,
			"resource": tool.Resource, "risk": tool.Risk,
		},
		"tenant": tenantDocument(tenant),
	})
	return decision
}

// EvaluateModel decides whether a model reference may be called by the tenant.
func (e *Engine) EvaluateModel(ctx context.Context, tenantID string, model ModelContext) Decision {
	tenant, ok := e.tenants[tenantID]
	if !ok {
		return Decision{DenyReasons: []string{"TENANT_POLICY_NOT_FOUND"}}
	}
	return evaluate(ctx, e.model, map[string]any{
		"model":  map[string]any{"name": model.Name},
		"tenant": tenantDocument(tenant),
	})
}

// EvaluatePeer decides whether the tenant may send an IPC message to a
// receiving AgentVersion reference. It answers the tenant-wide question only;
// whether the sending AgentVersion was granted that peer is a capability
// decision, enforced separately and first.
func (e *Engine) EvaluatePeer(ctx context.Context, tenantID string, peer PeerContext) Decision {
	tenant, ok := e.tenants[tenantID]
	if !ok {
		return Decision{DenyReasons: []string{"TENANT_POLICY_NOT_FOUND"}}
	}
	return evaluate(ctx, e.peer, map[string]any{
		"peer":   map[string]any{"to": peer.To},
		"tenant": tenantDocument(tenant),
	})
}

func tenantDocument(tenant TenantPolicy) map[string]any {
	return map[string]any{
		"max_priority": tenant.MaxPriority, "allowed_tools": tenant.AllowedTools,
		"allowed_models":         tenant.AllowedModels,
		"allowed_peers":          tenant.AllowedPeers,
		"approval_required_risk": tenant.ApprovalRequiredRisk,
	}
}

func evaluate(ctx context.Context, prepared preparedQuery, input map[string]any) Decision {
	resultSet, err := prepared.query.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return Decision{DenyReasons: []string{"POLICY_EVALUATION_FAILED"}}
	}
	if len(resultSet) == 0 || len(resultSet[0].Expressions) == 0 {
		return Decision{DenyReasons: []string{"DEFAULT_DENY"}}
	}
	document, ok := resultSet[0].Expressions[0].Value.(map[string]any)
	if !ok {
		return Decision{DenyReasons: []string{"DEFAULT_DENY"}}
	}
	allowed, _ := document["allow"].(bool)
	reasons := stringSet(document["deny_reasons"])
	if !allowed && len(reasons) == 0 {
		reasons = []string{"DEFAULT_DENY"}
	}
	requiresApproval := len(stringSet(document["requires_approval"])) > 0
	return Decision{Allow: allowed, RequiresApproval: requiresApproval, DenyReasons: reasons}
}

func stringSet(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	var reasons []string
	for _, item := range items {
		if text, ok := item.(string); ok {
			reasons = append(reasons, text)
		}
	}
	sort.Strings(reasons)
	return reasons
}
