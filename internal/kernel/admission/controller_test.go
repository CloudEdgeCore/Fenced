package admission

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/namespace"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/policy"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

func TestControllerRejectsUnpublishedAgentVersion(t *testing.T) {
	repository := &fakeControlStore{}
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":70,"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	decision := repository.lastDecision()
	if decision.Admit || decision.ReasonCode != "AGENT_VERSION_NOT_FOUND" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.AgentVersionID != nil {
		t.Fatalf("unpublished version was bound: %v", decision.AgentVersionID)
	}
}

func TestControllerRejectsRuntimeClassNotAllowedByVersion(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["wasm"]}}`)
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":70,"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	decision := repository.lastDecision()
	if decision.Admit || decision.ReasonCode != "RUNTIME_CLASS_NOT_ALLOWED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.AgentVersionID == nil {
		t.Fatal("resolved version was not bound to the task")
	}
}

func TestControllerAdmitsAndBindsResolvedAgentVersion(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":70,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	decision := repository.lastDecision()
	if !decision.Admit || decision.ReasonCode != "ADMISSION_PASSED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.AgentVersionID == nil || decision.AgentVersionID.String() != repository.versionID("agent@1") {
		t.Fatalf("version was not bound correctly: %v", decision.AgentVersionID)
	}
	if decision.Budget == nil || decision.Budget.Tokens != 500 || decision.Budget.CostMicroUSD != money.MustFromUSD(2) ||
		decision.Budget.ToolCalls != 10 || decision.Budget.WallSeconds != 60 {
		t.Fatalf("task budget was not reserved: %+v", decision.Budget)
	}
	if decision.PolicyRevision != policy.Revision {
		t.Fatalf("policy revision = %q, want %q", decision.PolicyRevision, policy.Revision)
	}
}

func TestControllerDeniesAboveTenantPolicyMaximum(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 60), "admission-1", 10, time.Minute)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":70,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	decision := repository.lastDecision()
	if decision.Admit || decision.ReasonCode != "POLICY_DENIED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	found := false
	for _, reason := range decision.Reasons {
		if reason.Code == "TASK_PRIORITY_EXCEEDS_TENANT_MAX" {
			found = true
		}
	}
	if !found {
		t.Fatalf("policy deny reason missing: %+v", decision.Reasons)
	}
	if decision.PolicyRevision != policy.Revision {
		t.Fatalf("policy revision = %q, want %q", decision.PolicyRevision, policy.Revision)
	}
}

func TestControllerDeniesTenantWithoutPolicyData(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	policyEngine, err := policy.New(policy.TenantPolicies{})
	if err != nil {
		t.Fatalf("prepare empty policy engine: %v", err)
	}
	controller := NewController(repository, New(testLimits()), policyEngine, "admission-1", 10, time.Minute)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":1,"costUsd":1,"toolCalls":1,"wallSeconds":1},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	decision := repository.lastDecision()
	if decision.Admit || decision.ReasonCode != "POLICY_DENIED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if len(decision.Reasons) != 1 || decision.Reasons[0].Code != "TENANT_POLICY_NOT_FOUND" {
		t.Fatalf("unexpected deny reasons: %+v", decision.Reasons)
	}
}

// TestControllerIsolatesPoisonedTask proves per-task error isolation: a task
// whose admission commit fails must not block the rest of the batch.
func TestControllerIsolatesPoisonedTask(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	good := taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)
	poisoned := taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)
	poisoned.ID = uuid.New()
	repository.claims = []store.TaskClaim{{Task: poisoned}, {Task: good}}
	repository.failTaskID = poisoned.ID

	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	processed, err := controller.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil (poisoned task must be isolated)", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1 (only the healthy task)", processed)
	}
	decision := repository.lastDecision()
	if decision.TaskID != good.ID {
		t.Fatalf("last decision was for the poisoned task: %s", decision.TaskID)
	}
	if repository.released != 1 {
		t.Fatalf("released claims = %d, want 1 (poisoned claim released)", repository.released)
	}
}

// TestControllerProcessesBatchConcurrently proves per-task parallelism (P1):
// a batch of independent claims is processed by a bounded worker set, and
// slow per-task commits overlap.
func TestControllerProcessesBatchConcurrently(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	const claimsCount = 8
	for i := 0; i < claimsCount; i++ {
		repository.claims = append(repository.claims, store.TaskClaim{Task: taskClaim("agent@1", `{
			"priority":1,"deadline":"2099-08-14T12:00:00Z",
			"budget":{"tokens":1,"costUsd":1,"toolCalls":1,"wallSeconds":1},
			"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
		}`)})
	}
	repository.slow = 50 * time.Millisecond
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 50, time.Minute)

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != claimsCount {
		t.Fatalf("Reconcile() = %d, %v; want %d processed", processed, err, claimsCount)
	}
	if repository.maxConcurrent() < 2 {
		t.Fatalf("max concurrent commits = %d, want >= 2 (claims processed serially)", repository.maxConcurrent())
	}

	// Serial mode processes the same batch without overlap.
	repository2 := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	for i := 0; i < claimsCount; i++ {
		repository2.claims = append(repository2.claims, store.TaskClaim{Task: taskClaim("agent@1", `{
			"priority":1,"deadline":"2099-08-14T12:00:00Z",
			"budget":{"tokens":1,"costUsd":1,"toolCalls":1,"wallSeconds":1},
			"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
		}`)})
	}
	repository2.slow = 20 * time.Millisecond
	serial := NewController(repository2, New(testLimits()), newTestPolicy(t, 100), "admission-1", 50, time.Minute)
	WithParallelism(1)(serial)
	if processed, err := serial.Reconcile(context.Background()); err != nil || processed != claimsCount {
		t.Fatalf("serial Reconcile() = %d, %v", processed, err)
	}
	if repository2.maxConcurrent() > 1 {
		t.Fatalf("serial mode overlapped commits: max=%d", repository2.maxConcurrent())
	}
}

func newTestPolicy(t *testing.T, maxPriority int) *policy.Engine {
	t.Helper()
	engine, err := policy.New(policy.TenantPolicies{"tenant-a": {MaxPriority: maxPriority}})
	if err != nil {
		t.Fatalf("prepare test policy engine: %v", err)
	}
	return engine
}

func TestControllerPassesNoBudgetForUnboundedTasks(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":70,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":0,"costUsd":0,"toolCalls":0,"wallSeconds":0},
		"placement":{"runtimeClasses":["oci"],"preferredClass":"oci","region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	if decision := repository.lastDecision(); decision.Admit && decision.Budget != nil {
		t.Fatalf("zero budget was reserved: %+v", decision.Budget)
	}
}

// --- tenant aggregate consumption quota (v0.6) ---

func TestControllerRejectsTenantOverQuota(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	quotas := &fakeQuotaStore{
		quota: store.TenantQuota{TenantID: "tenant-a", WindowSeconds: 86400, Limits: store.TaskBudget{Tokens: 100, CostMicroUSD: money.MustFromUSD(2)}},
		usage: store.TenantWindowUsage{TenantID: "tenant-a", Consumed: store.TaskBudget{Tokens: 90, CostMicroUSD: money.MustFromUSD(1)}},
	}
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	WithTenantQuotas(quotas)(controller)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	decision := repository.lastDecision()
	if decision.Admit || decision.ReasonCode != "TENANT_QUOTA_EXCEEDED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if decision.Budget == nil || decision.Budget.Tokens != 500 {
		t.Fatalf("quota rejection lost the task budget: %+v", decision.Budget)
	}
}

func TestControllerAdmitsWithinTenantQuota(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	quotas := &fakeQuotaStore{
		quota: store.TenantQuota{TenantID: "tenant-a", WindowSeconds: 86400, Limits: store.TaskBudget{Tokens: 1000, CostMicroUSD: money.MustFromUSD(10)}},
		usage: store.TenantWindowUsage{TenantID: "tenant-a", Consumed: store.TaskBudget{Tokens: 90, CostMicroUSD: money.MustFromUSD(1)}},
	}
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	WithTenantQuotas(quotas)(controller)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	if decision := repository.lastDecision(); !decision.Admit || decision.ReasonCode != "ADMISSION_PASSED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

// TestControllerRejectsWhenReservedCeilingsFillTheWindow proves the v0.8
// reservation gate at the controller level: reserved ceilings of in-flight
// tasks count against the limit exactly like consumed usage.
func TestControllerRejectsWhenReservedCeilingsFillTheWindow(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	quotas := &fakeQuotaStore{
		quota: store.TenantQuota{TenantID: "tenant-a", WindowSeconds: 86400, Limits: store.TaskBudget{Tokens: 100}},
		usage: store.TenantWindowUsage{
			TenantID: "tenant-a",
			Consumed: store.TaskBudget{Tokens: 0},
			Reserved: store.TaskBudget{Tokens: 60},
		},
	}
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	WithTenantQuotas(quotas)(controller)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":60,"costUsd":1,"toolCalls":1,"wallSeconds":1},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	if decision := repository.lastDecision(); decision.Admit || decision.ReasonCode != "TENANT_QUOTA_EXCEEDED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}

	// The same window admits when the reservation leaves room.
	quotas.usage.Reserved = store.TaskBudget{Tokens: 30}
	repository2 := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	controller2 := NewController(repository2, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	WithTenantQuotas(quotas)(controller2)
	repository2.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":60,"costUsd":1,"toolCalls":1,"wallSeconds":1},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}
	if _, err := controller2.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if decision := repository2.lastDecision(); !decision.Admit {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

// TestControllerAdmitsWithoutConfiguredQuota proves the quota gate is a
// no-op when the quota store reports no configuration for the tenant.
func TestControllerAdmitsWithoutConfiguredQuota(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	quotas := &fakeQuotaStore{noQuota: true}
	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	WithTenantQuotas(quotas)(controller)
	repository.claims = []store.TaskClaim{{Task: taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":500,"costUsd":2,"toolCalls":10,"wallSeconds":60},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)}}

	processed, err := controller.Reconcile(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("Reconcile() = %d, %v", processed, err)
	}
	if decision := repository.lastDecision(); !decision.Admit || decision.ReasonCode != "ADMISSION_PASSED" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
}

func TestControllerNamespaceEnforcer(t *testing.T) {
	repository := newFakeWithVersion("tenant-a", "agent@1", `{"runtimeClassPolicy":{"allowed":["oci","wasm"]}}`)
	nsStore := namespace.NewMemoryStore()
	enforcer := namespace.NewEnforcer(nsStore)

	_ = nsStore.CreateNamespace(context.Background(), &namespace.Namespace{
		TenantID: "tenant-a",
		Name:     "term-ns",
		Phase:    namespace.NamespacePhaseTerminating,
	})
	_ = nsStore.CreateNamespace(context.Background(), &namespace.Namespace{
		TenantID: "tenant-a",
		Name:     "quota-ns",
		Phase:    namespace.NamespacePhaseActive,
		Quota: namespace.ResourceQuota{
			MaxTasks: 1,
		},
	})

	controller := NewController(repository, New(testLimits()), newTestPolicy(t, 100), "admission-1", 10, time.Minute)
	WithNamespaceEnforcer(enforcer)(controller)

	// 1. Terminating namespace is rejected
	termTask := taskClaim("agent@1", `{
		"priority":1,"deadline":"2099-08-14T12:00:00Z",
		"budget":{"tokens":100,"costUsd":1,"toolCalls":1,"wallSeconds":1},
		"placement":{"runtimeClasses":["oci"],"region":"cn-east","cpuMillis":100,"memoryMiB":128,"llmConcurrency":1}
	}`)
	termTask.Namespace = "term-ns"
	repository.claims = []store.TaskClaim{{Task: termTask}}

	_, err := controller.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() err = %v", err)
	}
	if decision := repository.lastDecision(); decision.Admit || decision.ReasonCode != "NAMESPACE_TERMINATING" {
		t.Fatalf("expected NAMESPACE_TERMINATING, got %+v", decision)
	}

	// 2. Namespace with exhausted quota is rejected
	_ = nsStore.RecordUsageDelta(context.Background(), "tenant-a", "quota-ns", namespace.ResourceUsageDelta{ActiveTasksDelta: 1})
	quotaTask := termTask
	quotaTask.Namespace = "quota-ns"
	repository.claims = []store.TaskClaim{{Task: quotaTask}}

	_, err = controller.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() err = %v", err)
	}
	if decision := repository.lastDecision(); decision.Admit || decision.ReasonCode != "NAMESPACE_QUOTA_EXCEEDED" {
		t.Fatalf("expected NAMESPACE_QUOTA_EXCEEDED, got %+v", decision)
	}
}

// fakeQuotaStore is a scriptable store.TenantQuotaStore for controller tests.
type fakeQuotaStore struct {
	mu       sync.Mutex
	quota    store.TenantQuota
	usage    store.TenantWindowUsage
	noQuota  bool
	usageErr error
}

func (f *fakeQuotaStore) SetTenantQuota(context.Context, store.SetTenantQuotaInput) (store.TenantQuota, error) {
	return store.TenantQuota{}, nil
}

func (f *fakeQuotaStore) GetTenantQuota(context.Context, string) (store.TenantQuota, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noQuota {
		return store.TenantQuota{}, store.ErrNotFound
	}
	return f.quota, nil
}

func (f *fakeQuotaStore) DeleteTenantQuota(context.Context, string) error { return nil }

func (f *fakeQuotaStore) GetTenantQuotaUsage(context.Context, string, time.Time) (store.TenantWindowUsage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.usageErr != nil {
		return store.TenantWindowUsage{}, f.usageErr
	}
	return f.usage, nil
}

func taskClaim(ref, spec string) store.Task {
	return store.Task{
		ID: uuid.New(), TenantID: "tenant-a", Namespace: "default", AgentVersionRef: ref,
		Goal: "test", Spec: json.RawMessage(spec), Phase: domain.TaskQueued, ResourceVersion: 1,
	}
}

type fakeControlStore struct {
	mu         sync.Mutex
	versions   map[string]store.AgentVersion
	claims     []store.TaskClaim
	decisions  []store.DecideAdmissionInput
	failTaskID uuid.UUID
	released   int
	slow       time.Duration
	active     int
	peak       int
}

func (f *fakeControlStore) maxConcurrent() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peak
}

func newFakeWithVersion(tenantID, ref, spec string) *fakeControlStore {
	repository := &fakeControlStore{versions: map[string]store.AgentVersion{}}
	namespace, name, version, err := agentversion.ParseRef(ref)
	if err != nil {
		panic(err)
	}
	repository.versions[tenantID+"/"+ref] = store.AgentVersion{
		ID: uuid.New(), TenantID: tenantID, Namespace: namespace, Name: name, Version: version,
		Spec: json.RawMessage(spec), ResourceVersion: 1,
	}
	return repository
}

func (f *fakeControlStore) versionID(ref string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.versions["tenant-a/"+ref].ID.String()
}

func (f *fakeControlStore) lastDecision() store.DecideAdmissionInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.decisions[len(f.decisions)-1]
}

func (f *fakeControlStore) GetAgentVersionByRef(_ context.Context, tenantID, ref string) (store.AgentVersion, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	version, ok := f.versions[tenantID+"/"+ref]
	if !ok {
		return store.AgentVersion{}, store.ErrNotFound
	}
	return version, nil
}

func (f *fakeControlStore) ClaimTasks(context.Context, store.ClaimTasksInput) ([]store.TaskClaim, error) {
	return f.claims, nil
}

func (f *fakeControlStore) DecideAdmission(_ context.Context, in store.DecideAdmissionInput) (store.Task, error) {
	f.mu.Lock()
	f.active++
	if f.active > f.peak {
		f.peak = f.active
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if f.slow > 0 {
		time.Sleep(f.slow)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failTaskID != uuid.Nil && in.TaskID == f.failTaskID {
		return store.Task{}, errors.New("poisoned task commit failure")
	}
	f.decisions = append(f.decisions, in)
	return store.Task{Phase: domain.TaskAdmitted, ResourceVersion: in.ExpectedTaskVersion + 1}, nil
}

func (f *fakeControlStore) ReleaseTaskClaim(context.Context, store.TaskClaim) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released++
	return nil
}

func (f *fakeControlStore) DeferTaskSchedule(context.Context, store.DeferTaskScheduleInput) (store.Task, error) {
	return store.Task{}, nil
}

func (f *fakeControlStore) GetTask(context.Context, string, uuid.UUID) (store.Task, error) {
	return store.Task{}, store.ErrNotFound
}

func (f *fakeControlStore) ScheduleTask(context.Context, store.ScheduleTaskInput) (store.AttemptLease, error) {
	return store.AttemptLease{}, nil
}

func (f *fakeControlStore) ClaimOutbox(context.Context, store.ClaimOutboxInput) ([]store.OutboxEvent, error) {
	return nil, nil
}

func (f *fakeControlStore) MarkOutboxPublished(context.Context, uuid.UUID, string, int64, time.Time) error {
	return nil
}

func (f *fakeControlStore) MarkOutboxFailed(context.Context, uuid.UUID, string, int64, string, time.Time) error {
	return nil
}
