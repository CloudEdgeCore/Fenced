// Package admission evaluates deterministic pre-scheduling requirements.
package admission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
	"github.com/google/uuid"
)

const EvaluatorVersion = "builtin/v1"

type Limits struct {
	RuntimeClasses    []string
	MaxTokens         int64
	MaxCostMicroUSD   money.MicroUSD
	MaxToolCalls      int64
	MaxWallSeconds    int64
	MaxCPU            int64
	MaxMemory         int64
	MaxLLMConcurrency int
	// ContainerClasses are runtime classes that execute untrusted workloads
	// in a sandboxed container (oci, microvm). Admission requires every
	// task on these classes to declare explicit CPU, memory and workspace
	// limits — zero values are never allowed (hardening checklist §4.1).
	ContainerClasses []string
}

type Decision struct {
	Admit      bool
	ReasonCode string
	Reasons    []store.AdmissionReason
}

type Engine struct {
	limits Limits
	now    func() time.Time
}

func New(limits Limits) *Engine {
	return &Engine{limits: limits, now: func() time.Time { return time.Now().UTC() }}
}

func (e *Engine) Evaluate(task store.Task) Decision {
	spec, err := workload.Decode(task.Spec)
	if err != nil {
		return rejected("SPEC_INVALID", "spec", err.Error())
	}
	var reasons []store.AdmissionReason
	if spec.Priority < 0 || spec.Priority > 100 {
		reasons = append(reasons, reason("PRIORITY_OUT_OF_RANGE", "priority", "priority must be between 0 and 100"))
	}
	if spec.Deadline != nil && !spec.Deadline.After(e.now()) {
		reasons = append(reasons, reason("DEADLINE_EXPIRED", "deadline", "deadline must be in the future"))
	}
	if len(spec.Placement.RuntimeClasses) == 0 {
		reasons = append(reasons, reason("RUNTIME_CLASS_REQUIRED", "placement.runtimeClasses", "at least one runtime class is required"))
	}
	for _, runtimeClass := range spec.Placement.RuntimeClasses {
		if !slices.Contains(e.limits.RuntimeClasses, runtimeClass) {
			reasons = append(reasons, reason("RUNTIME_CLASS_DENIED", "placement.runtimeClasses", fmt.Sprintf("runtime class %q is not admitted", runtimeClass)))
		}
	}
	if strings.TrimSpace(spec.Placement.Region) == "" {
		reasons = append(reasons, reason("REGION_REQUIRED", "placement.region", "placement region is required"))
	}
	if spec.Runtime != nil {
		if err := spec.Runtime.ValidateCommand(); err != nil {
			reasons = append(reasons, reason("RUNTIME_COMMAND_INVALID", "runtime.command", err.Error()))
		}
	}
	checkPositiveLimit(&reasons, "BUDGET_TOKENS_INVALID", "budget.tokens", spec.Budget.Tokens, e.limits.MaxTokens)
	checkPositiveLimit(&reasons, "BUDGET_TOOL_CALLS_INVALID", "budget.toolCalls", spec.Budget.ToolCalls, e.limits.MaxToolCalls)
	checkPositiveLimit(&reasons, "BUDGET_WALL_TIME_INVALID", "budget.wallSeconds", spec.Budget.WallSeconds, e.limits.MaxWallSeconds)
	if spec.Budget.CostMicroUSD <= 0 || (e.limits.MaxCostMicroUSD > 0 && spec.Budget.CostMicroUSD > e.limits.MaxCostMicroUSD) {
		reasons = append(reasons, reason("BUDGET_COST_INVALID", "budget.costUsd", "cost budget must be positive and within the tenant limit"))
	}
	checkPositiveLimit(&reasons, "CPU_REQUEST_INVALID", "placement.cpuMillis", spec.Placement.CPU, e.limits.MaxCPU)
	checkPositiveLimit(&reasons, "MEMORY_REQUEST_INVALID", "placement.memoryMiB", spec.Placement.Memory, e.limits.MaxMemory)
	if spec.Placement.LLMConcurrency <= 0 || (e.limits.MaxLLMConcurrency > 0 && spec.Placement.LLMConcurrency > e.limits.MaxLLMConcurrency) {
		reasons = append(reasons, reason("LLM_CONCURRENCY_INVALID", "placement.llmConcurrency", "LLM concurrency must be positive and within the tenant limit"))
	}
	// Container classes must declare explicit sandbox limits; a zero value
	// means "unlimited" to the executor and is never admitted (hardening
	// checklist §4.1).
	for _, runtimeClass := range spec.Placement.RuntimeClasses {
		if !slices.Contains(e.limits.ContainerClasses, runtimeClass) {
			continue
		}
		if spec.Placement.CPU <= 0 {
			reasons = append(reasons, reason("CONTAINER_CPU_REQUIRED", "placement.cpuMillis",
				fmt.Sprintf("runtime class %q requires an explicit positive cpuMillis", runtimeClass)))
		}
		if spec.Placement.Memory <= 0 {
			reasons = append(reasons, reason("CONTAINER_MEMORY_REQUIRED", "placement.memoryMiB",
				fmt.Sprintf("runtime class %q requires an explicit positive memoryMiB", runtimeClass)))
		}
		if spec.Placement.WorkspaceBytes <= 0 {
			reasons = append(reasons, reason("CONTAINER_WORKSPACE_REQUIRED", "placement.workspaceBytes",
				fmt.Sprintf("runtime class %q requires an explicit positive workspaceBytes", runtimeClass)))
		}
	}
	if maxAttempts := spec.RetryPolicy.EffectiveMaxAttempts(); maxAttempts < 1 || maxAttempts > 10 {
		reasons = append(reasons, reason("RETRY_ATTEMPTS_INVALID", "retryPolicy.maxAttempts", "max attempts must be between 1 and 10"))
	}
	if len(reasons) != 0 {
		return Decision{ReasonCode: reasons[0].Code, Reasons: reasons}
	}
	return Decision{Admit: true, ReasonCode: "ADMISSION_PASSED", Reasons: []store.AdmissionReason{{Code: "ADMISSION_PASSED", Message: "all v1 admission checks passed"}}}
}

type Controller struct {
	store    store.ControlStore
	engine   *Engine
	policy   *policy.Engine
	ownerID  string
	batch    int
	claimTTL time.Duration
	// parallel bounds concurrent per-task processing within one batch (P1);
	// 1 disables parallelism.
	parallel int
	// quotas is the optional tenant aggregate consumption quota store
	// (v0.6). When nil, tenant quotas are not enforced by this controller.
	quotas store.TenantQuotaStore
	// namespaceEnforcer evaluates namespace lifecycle and per-namespace quotas.
	namespaceEnforcer *namespace.Enforcer
	// shardIndex / shardCount are the tenant-consistent claim shard
	// (ADR-016); zero count disables sharding.
	shardIndex int
	shardCount int
}

func NewController(repository store.ControlStore, engine *Engine, policyEngine *policy.Engine, ownerID string, batch int, claimTTL time.Duration) *Controller {
	return &Controller{store: repository, engine: engine, policy: policyEngine, ownerID: ownerID, batch: batch, claimTTL: claimTTL, parallel: 4}
}

// WithParallelism bounds concurrent per-task processing within one reconcile
// batch (default 4; 1 = serial).
func WithParallelism(workers int) func(*Controller) {
	return func(c *Controller) {
		if workers > 0 {
			c.parallel = workers
		}
	}
}

// WithTenantQuotas installs the tenant aggregate consumption quota gate
// (v0.6): a task is admitted only while the tenant's current window usage
// plus the task's own budget ceiling stays within every configured limit.
// The store re-checks the same gate atomically at commit time, so concurrent
// settlements cannot slip past the read-only check here.
func WithTenantQuotas(quotaStore store.TenantQuotaStore) func(*Controller) {
	return func(c *Controller) { c.quotas = quotaStore }
}

// WithNamespaceEnforcer installs the namespace and per-namespace resource quota gate.
func WithNamespaceEnforcer(enforcer *namespace.Enforcer) func(*Controller) {
	return func(c *Controller) { c.namespaceEnforcer = enforcer }
}

// WithShard confines this instance to one tenant-consistent claim shard
// (ADR-016): it claims only tasks whose tenant maps to index of count.
// Count 0 disables sharding. Every controller instance must share the same
// count.
func WithShard(index, count int) func(*Controller) {
	return func(c *Controller) { c.shardIndex, c.shardCount = index, count }
}

// Reconcile claims and admits queued tasks. Transient transaction conflicts
// are retried with bounded backoff (ADR-002); the claim TTL covers permanent
// failures.
func (c *Controller) Reconcile(ctx context.Context) (int, error) {
	return store.RetryRetryable(ctx, func() (int, error) { return c.reconcileOnce(ctx) })
}

func (c *Controller) reconcileOnce(ctx context.Context) (int, error) {
	claims, err := c.store.ClaimTasks(ctx, store.ClaimTasksInput{
		Kind: store.ControllerAdmission, Phase: "QUEUED", OwnerID: c.ownerID, Limit: c.batch, TTL: c.claimTTL,
		ShardIndex: c.shardIndex, ShardCount: c.shardCount,
	})
	if err != nil {
		return 0, err
	}
	// Per-task parallelism (P1): claims are independent rows, so a bounded
	// worker set processes them concurrently; retryable errors abort the
	// batch, non-retryable per-task failures are isolated per claim.
	processed, err := c.processClaims(ctx, claims)
	if err != nil {
		return processed, err
	}
	return processed, nil
}

func (c *Controller) processClaims(ctx context.Context, claims []store.TaskClaim) (int, error) {
	if c.parallel <= 1 || len(claims) <= 1 {
		processed := 0
		for _, claim := range claims {
			ok, err := c.processClaim(ctx, claim)
			if err != nil {
				return processed, err
			}
			if ok {
				processed++
			}
		}
		return processed, nil
	}
	processed := 0
	var mu sync.Mutex
	var batchErr error
	semaphore := make(chan struct{}, c.parallel)
	var wg sync.WaitGroup
	for _, claim := range claims {
		claim := claim
		mu.Lock()
		aborted := batchErr != nil
		mu.Unlock()
		if aborted {
			break
		}
		wg.Add(1)
		semaphore <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-semaphore }()
			ok, err := c.processClaim(ctx, claim)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if batchErr == nil {
					batchErr = err
				}
				return
			}
			if ok {
				processed++
			}
		}()
	}
	wg.Wait()
	return processed, batchErr
}

// processClaim runs one claim through the decision chain, returning whether
// the claim was processed. Non-retryable failures are isolated per claim
// (claim released, logged); retryable failures abort the batch.
func (c *Controller) processClaim(ctx context.Context, claim store.TaskClaim) (bool, error) {
	decision, versionID, budget, err := c.decide(ctx, claim)
	if err != nil {
		if store.IsRetryableTransaction(err) {
			return false, err
		}
		// Per-task error isolation: a task that cannot be decided must
		// not block the rest of the queue. The claim is released so the
		// next round re-evaluates it.
		_ = c.store.ReleaseTaskClaim(ctx, claim)
		slog.Error("admission decision failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", err)
		return false, nil
	}
	_, err = c.store.DecideAdmission(ctx, store.DecideAdmissionInput{
		TaskID: claim.Task.ID, TenantID: claim.Task.TenantID, OwnerID: claim.OwnerID,
		ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: claim.Task.ResourceVersion,
		Admit: decision.Admit, ReasonCode: decision.ReasonCode, Reasons: decision.Reasons,
		EvaluatorVersion: EvaluatorVersion, AgentVersionID: versionID, Budget: budget,
		PolicyRevision: policy.Revision,
	})
	if err != nil {
		if store.IsRetryableTransaction(err) {
			return false, err
		}
		// A stale claim (another controller moved the task) or an
		// unexpected per-task failure: release and continue.
		_ = c.store.ReleaseTaskClaim(ctx, claim)
		slog.Error("admission commit failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", err)
		return false, nil
	}
	return true, nil
}

// decide resolves the task's agent version reference and evaluates both the
// bounded workload spec and the published version policy. The resolved version
// is bound to the task regardless of the outcome so that the decision record
// always documents what the task referenced; the decoded budget reservation is
// passed along so that admitted tasks hold a ledger.
func (c *Controller) decide(ctx context.Context, claim store.TaskClaim) (Decision, *uuid.UUID, *store.TaskBudget, error) {
	version, err := c.store.GetAgentVersionByRef(ctx, claim.Task.TenantID, claim.Task.AgentVersionRef)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAgentVersionRefInvalid):
			return rejected("AGENT_VERSION_REF_INVALID", "agentVersionRef", "agent version reference must be a published name@version"), nil, nil, nil
		case errors.Is(err, store.ErrNotFound):
			return rejected("AGENT_VERSION_NOT_FOUND", "agentVersionRef", "agent version is not published in this tenant"), nil, nil, nil
		default:
			return Decision{}, nil, nil, err
		}
	}
	spec, err := workload.Decode(claim.Task.Spec)
	if err != nil {
		// The engine reports SPEC_INVALID for the same failure; budget
		// reservation is irrelevant for a spec that cannot be decoded.
		spec = workload.Spec{}
	}
	var budget *store.TaskBudget
	if !spec.Budget.Zero() {
		value := store.TaskBudget{
			Tokens: spec.Budget.Tokens, CostMicroUSD: spec.Budget.CostMicroUSD,
			ToolCalls: spec.Budget.ToolCalls, WallSeconds: spec.Budget.WallSeconds,
		}
		budget = &value
	}
	decision := c.engine.Evaluate(claim.Task)
	if !decision.Admit {
		return decision, &version.ID, budget, nil
	}
	if reasons := checkVersionPolicy(claim.Task.Spec, version); len(reasons) != 0 {
		return Decision{ReasonCode: reasons[0].Code, Reasons: reasons}, &version.ID, budget, nil
	}
	// The Rego policy engine is the tenant-attribute gate: it runs after the
	// bounded spec checks and denies by default on any failure.
	policyDecision := c.policy.Evaluate(ctx, claim.Task.TenantID, policy.TaskContext{Priority: spec.Priority})
	if !policyDecision.Allow {
		reasons := make([]store.AdmissionReason, 0, len(policyDecision.DenyReasons))
		for _, code := range policyDecision.DenyReasons {
			reasons = append(reasons, reason(code, "policy", "rego policy denied the task"))
		}
		return Decision{ReasonCode: "POLICY_DENIED", Reasons: reasons}, &version.ID, budget, nil
	}
	// Tenant aggregate consumption quota (v0.6/v0.8): with a configured
	// window quota, the task is admitted only while the current window's
	// settled consumption plus the reserved ceilings of in-flight tasks plus
	// the task's own ceiling stays within every limit. This read-only pass
	// produces the recorded rejection; the store re-checks and reserves
	// atomically at commit, so a concurrent admission or settlement cannot
	// slip in between the read and the decision.
	if c.quotas != nil {
		quota, err := c.quotas.GetTenantQuota(ctx, claim.Task.TenantID)
		switch {
		case errors.Is(err, store.ErrNotFound):
			// No quota configured: unlimited.
		case err != nil:
			return Decision{}, nil, nil, err
		default:
			usage, err := c.quotas.GetTenantQuotaUsage(ctx, claim.Task.TenantID, time.Now().UTC())
			if err != nil {
				return Decision{}, nil, nil, err
			}
			var ceiling store.TaskBudget
			if budget != nil {
				ceiling = *budget
			}
			if store.QuotaReservationExceeded(quota.Limits, usage.Consumed, usage.Reserved, ceiling) {
				return Decision{ReasonCode: "TENANT_QUOTA_EXCEEDED", Reasons: []store.AdmissionReason{
					reason("TENANT_QUOTA_EXCEEDED", "tenant", "tenant aggregate consumption quota would be exceeded"),
				}}, &version.ID, budget, nil
			}
		}
	}
	// Namespace lifecycle and per-namespace resource quota (Stage ⑤):
	if c.namespaceEnforcer != nil {
		targetNS := claim.Task.Namespace
		if targetNS == "" {
			targetNS = namespace.DefaultNamespace
		}
		var ceiling store.TaskBudget
		if budget != nil {
			ceiling = *budget
		}
		if err := c.namespaceEnforcer.CheckTaskAdmission(ctx, claim.Task.TenantID, targetNS, ceiling); err != nil {
			if errors.Is(err, namespace.ErrNamespaceTerminating) {
				return Decision{ReasonCode: "NAMESPACE_TERMINATING", Reasons: []store.AdmissionReason{
					reason("NAMESPACE_TERMINATING", "namespace", err.Error()),
				}}, &version.ID, budget, nil
			}
			if errors.Is(err, namespace.ErrQuotaExceeded) {
				return Decision{ReasonCode: "NAMESPACE_QUOTA_EXCEEDED", Reasons: []store.AdmissionReason{
					reason("NAMESPACE_QUOTA_EXCEEDED", "quota", err.Error()),
				}}, &version.ID, budget, nil
			}
			return Decision{ReasonCode: "NAMESPACE_DENIED", Reasons: []store.AdmissionReason{
				reason("NAMESPACE_DENIED", "namespace", err.Error()),
			}}, &version.ID, budget, nil
		}
	}
	return decision, &version.ID, budget, nil
}

// checkVersionPolicy enforces the runtime-class and image policy published
// with the agent version. An absent policy is permissive: the engine-level
// limits remain authoritative. A published image pin is mandatory for the
// task (ADR-010): the task must pin exactly the same image.
func checkVersionPolicy(raw json.RawMessage, version store.AgentVersion) []store.AdmissionReason {
	spec, err := workload.Decode(raw)
	if err != nil {
		return []store.AdmissionReason{reason("AGENT_VERSION_POLICY_INVALID", "agentVersionRef", "task spec could not be decoded against the published agent version")}
	}
	var policy agentversion.Spec
	if err := json.Unmarshal(version.Spec, &policy); err != nil {
		return []store.AdmissionReason{reason("AGENT_VERSION_POLICY_INVALID", "agentVersionRef", "published agent version policy is invalid")}
	}
	var reasons []store.AdmissionReason
	for _, runtimeClass := range spec.Placement.RuntimeClasses {
		if len(policy.RuntimeClassPolicy.Allowed) > 0 && !slices.Contains(policy.RuntimeClassPolicy.Allowed, runtimeClass) {
			reasons = append(reasons, reason("RUNTIME_CLASS_NOT_ALLOWED", "placement.runtimeClasses",
				fmt.Sprintf("runtime class %q is not allowed by agent version %s", runtimeClass, version.Ref())))
		}
	}
	if spec.Image != nil {
		if err := spec.Image.Validate(); err != nil {
			reasons = append(reasons, reason("RUNTIME_IMAGE_INVALID", "image", err.Error()))
		}
	}
	if policy.Image != nil {
		switch {
		case spec.Image == nil:
			reasons = append(reasons, reason("RUNTIME_IMAGE_REQUIRED", "image",
				fmt.Sprintf("agent version %s pins image %q; the task must pin the same image", version.Ref(), policy.Image.Ref)))
		case policy.Image.Ref != spec.Image.Ref || policy.Image.Digest != spec.Image.Digest:
			reasons = append(reasons, reason("RUNTIME_IMAGE_MISMATCH", "image",
				fmt.Sprintf("task pins %q but agent version %s pins %q", spec.Image.Canonical(), version.Ref(), imageCanonical(policy.Image))))
		}
	}
	return reasons
}

// imageCanonical renders the version-level pin like workload.Image.Canonical.
func imageCanonical(image *agentversion.Image) string {
	if image == nil {
		return ""
	}
	if image.Digest != "" {
		return image.Ref + "@" + image.Digest
	}
	return image.Ref
}

func rejected(code, field, message string) Decision {
	return Decision{ReasonCode: code, Reasons: []store.AdmissionReason{reason(code, field, message)}}
}

func reason(code, field, message string) store.AdmissionReason {
	return store.AdmissionReason{Code: code, Field: field, Message: message}
}

func checkPositiveLimit(reasons *[]store.AdmissionReason, code, field string, value, limit int64) {
	if value <= 0 || (limit > 0 && value > limit) {
		*reasons = append(*reasons, reason(code, field, "value must be positive and within the tenant limit"))
	}
}
