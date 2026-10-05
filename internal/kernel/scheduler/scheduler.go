// Package scheduler provides deterministic, explainable runtime placement.
package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/workload"
	"github.com/CloudEdgeCore/Fenced/internal/platform/agentmetrics"
	"github.com/google/uuid"
)

var ErrNoPlacement = errors.New("no eligible runtime placement")
var ErrInvalidPoolSet = errors.New("runtime pool set is invalid")

type RuntimePool struct {
	ID                string   `json:"id"`
	TenantIDs         []string `json:"tenantIds"`
	RuntimeClass      string   `json:"runtimeClass"`
	RuntimeInstanceID string   `json:"runtimeInstanceId"`
	Region            string   `json:"region"`
	DataResidency     string   `json:"dataResidency"`
	Ready             bool     `json:"ready"`
	// Status separates health from operator intent. Empty/ACTIVE accepts new
	// work; CORDONED and DRAINING preserve existing leases but reject placement.
	Status            string `json:"status,omitempty"`
	FailureDomain     string `json:"failureDomain,omitempty"`
	AvailableCPU      int64  `json:"availableCpuMillis"`
	AvailableMemory   int64  `json:"availableMemoryMiB"`
	AvailableLLMSlots int    `json:"availableLlmSlots"`
	// CapacityLedgerMissing marks a pool whose durable capacity ledger was
	// never registered by the operator. Such pools fail placement closed
	// instead of being treated as fully idle. Zero (the default) keeps
	// statically configured pools schedulable.
	CapacityLedgerMissing bool     `json:"capacityLedgerMissing,omitempty"`
	ArtifactRegions       []string `json:"artifactRegions"`
	CostWeight            float64  `json:"costWeight"`
	// Operators lists the operator subjects granted cordon/drain/activate
	// authority on this pool. Usage visibility comes from TenantIDs; the
	// two authorities are deliberately independent so a tenant of a shared
	// pool cannot operate it.
	Operators []string `json:"operators,omitempty"`
}

type ScoreComponent struct {
	Name   string  `json:"name"`
	Value  float64 `json:"value"`
	Reason string  `json:"reason"`
}

type Placement struct {
	Pool       RuntimePool      `json:"pool"`
	Score      float64          `json:"score"`
	Components []ScoreComponent `json:"components"`
}

type Rejection struct {
	PoolID  string   `json:"poolId"`
	Reasons []string `json:"reasons"`
}

type Result struct {
	Placement Placement `json:"placement"`
	// Candidates is the full score-ranked placement list. Placement is the
	// first entry; the scheduler walks the remaining entries in order when
	// a higher-ranked pool loses the transactional capacity reservation.
	Candidates []Placement `json:"candidates"`
	Rejected   []Rejection `json:"rejected"`
}

func Select(spec workload.Spec, pools []RuntimePool) (Result, error) {
	return selectForTask(spec, pools, uuid.Nil)
}

// selectForTask uses rendezvous hashing only to break equal-score ties. This
// preserves explainable placement scores while preventing every task from
// hot-spotting the lexicographically first identical pool.
func selectForTask(spec workload.Spec, pools []RuntimePool, taskID uuid.UUID) (Result, error) {
	var candidates []Placement
	result := Result{}
	seen := make(map[string]struct{}, len(pools))
	for _, pool := range pools {
		var rejected []string
		if strings.TrimSpace(pool.ID) == "" || strings.TrimSpace(pool.RuntimeInstanceID) == "" {
			return result, fmt.Errorf("%w: pool ID and runtime instance ID are required", ErrInvalidPoolSet)
		}
		if _, exists := seen[pool.ID]; exists {
			return result, fmt.Errorf("%w: duplicate pool ID %q", ErrInvalidPoolSet, pool.ID)
		}
		seen[pool.ID] = struct{}{}
		if math.IsNaN(pool.CostWeight) || math.IsInf(pool.CostWeight, 0) || pool.CostWeight < 0 {
			return result, fmt.Errorf("%w: pool %q costWeight must be finite and non-negative", ErrInvalidPoolSet, pool.ID)
		}
		if pool.AvailableCPU < 0 || pool.AvailableMemory < 0 || pool.AvailableLLMSlots < 0 {
			return result, fmt.Errorf("%w: pool %q capacities must be non-negative", ErrInvalidPoolSet, pool.ID)
		}
		if !pool.Ready {
			rejected = append(rejected, "POOL_NOT_READY")
		}
		switch strings.ToUpper(strings.TrimSpace(pool.Status)) {
		case "", "ACTIVE":
		case "CORDONED":
			rejected = append(rejected, "POOL_CORDONED")
		case "DRAINING":
			rejected = append(rejected, "POOL_DRAINING")
		default:
			return result, fmt.Errorf("%w: pool %q has unknown status %q", ErrInvalidPoolSet, pool.ID, pool.Status)
		}
		if pool.FailureDomain != "" && slices.Contains(spec.Placement.AvoidFailureDomains, pool.FailureDomain) {
			rejected = append(rejected, "FAILURE_DOMAIN_ANTI_AFFINITY")
		}
		if !slices.Contains(spec.Placement.RuntimeClasses, pool.RuntimeClass) {
			rejected = append(rejected, "RUNTIME_CLASS_MISMATCH")
		}
		if pool.Region != spec.Placement.Region {
			rejected = append(rejected, "REGION_MISMATCH")
		}
		if spec.Placement.DataResidency != "" && pool.DataResidency != spec.Placement.DataResidency {
			rejected = append(rejected, "DATA_RESIDENCY_MISMATCH")
		}
		if pool.CapacityLedgerMissing {
			rejected = append(rejected, "CAPACITY_LEDGER_MISSING")
		}
		if pool.AvailableCPU < spec.Placement.CPU {
			rejected = append(rejected, "CPU_EXHAUSTED")
		}
		if pool.AvailableMemory < spec.Placement.Memory {
			rejected = append(rejected, "MEMORY_EXHAUSTED")
		}
		if pool.AvailableLLMSlots < spec.Placement.LLMConcurrency {
			rejected = append(rejected, "LLM_CAPACITY_EXHAUSTED")
		}
		if len(rejected) != 0 {
			result.Rejected = append(result.Rejected, Rejection{PoolID: pool.ID, Reasons: rejected})
			continue
		}
		components := score(spec, pool)
		var total float64
		for _, component := range components {
			total += component.Value
		}
		candidates = append(candidates, Placement{Pool: pool, Score: total, Components: components})
	}
	if len(candidates) == 0 {
		sort.Slice(result.Rejected, func(i, j int) bool { return result.Rejected[i].PoolID < result.Rejected[j].PoolID })
		return result, ErrNoPlacement
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			if taskID != uuid.Nil {
				left := placementTieHash(taskID, candidates[i].Pool.ID)
				right := placementTieHash(taskID, candidates[j].Pool.ID)
				if comparison := bytes.Compare(left[:], right[:]); comparison != 0 {
					return comparison > 0
				}
			}
			return candidates[i].Pool.ID < candidates[j].Pool.ID
		}
		return candidates[i].Score > candidates[j].Score
	})
	result.Placement = candidates[0]
	result.Candidates = candidates
	sort.Slice(result.Rejected, func(i, j int) bool { return result.Rejected[i].PoolID < result.Rejected[j].PoolID })
	return result, nil
}

func placementTieHash(taskID uuid.UUID, poolID string) [sha256.Size]byte {
	payload := make([]byte, 0, len(taskID)+len(poolID))
	payload = append(payload, taskID[:]...)
	payload = append(payload, poolID...)
	return sha256.Sum256(payload)
}

func score(spec workload.Spec, pool RuntimePool) []ScoreComponent {
	preferred := 0.0
	if pool.RuntimeClass == spec.Placement.PreferredClass {
		preferred = 30
	}
	locality := 0.0
	if spec.Placement.ArtifactRegion != "" && slices.Contains(pool.ArtifactRegions, spec.Placement.ArtifactRegion) {
		locality = 20
	}
	cpuHeadroom := ratio(pool.AvailableCPU-spec.Placement.CPU, pool.AvailableCPU) * 20
	memoryHeadroom := ratio(pool.AvailableMemory-spec.Placement.Memory, pool.AvailableMemory) * 15
	llmHeadroom := ratio(int64(pool.AvailableLLMSlots-spec.Placement.LLMConcurrency), int64(pool.AvailableLLMSlots)) * 15
	cost := -math.Max(0, pool.CostWeight) * 10
	return []ScoreComponent{
		{Name: "preferredRuntimeClass", Value: preferred, Reason: "preferred runtime class match"},
		{Name: "artifactLocality", Value: locality, Reason: "artifact region locality"},
		{Name: "cpuHeadroom", Value: cpuHeadroom, Reason: "remaining CPU capacity"},
		{Name: "memoryHeadroom", Value: memoryHeadroom, Reason: "remaining memory capacity"},
		{Name: "llmHeadroom", Value: llmHeadroom, Reason: "remaining LLM concurrency"},
		{Name: "cost", Value: cost, Reason: "normalized runtime cost penalty"},
	}
}

func ratio(numerator, denominator int64) float64 {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	return math.Min(1, float64(numerator)/float64(denominator))
}

type PoolSource interface {
	ListRuntimePools(context.Context, string) ([]RuntimePool, error)
}

// StaticPoolSource is an in-memory pool list for tests and local development.
// It is NOT a production capacity authority: it reports pools directly from a
// fixed slice and never consults the durable capacity ledger, so wiring it into
// a server bypasses the reservation invariant that keeps concurrent placement
// from over-committing a pool. Production builds the pool source from the
// durable registry (LeaseAwarePoolSource over the store); GuardProductionPoolSource
// fails a non-dev server that is handed a StaticPoolSource.
type StaticPoolSource []RuntimePool

func (s StaticPoolSource) ListRuntimePools(_ context.Context, tenantID string) ([]RuntimePool, error) {
	var pools []RuntimePool
	for _, pool := range s {
		if slices.Contains(pool.TenantIDs, tenantID) {
			pools = append(pools, pool)
		}
	}
	return pools, nil
}

// GuardProductionPoolSource rejects a StaticPoolSource on a production startup
// path. The scheduler's authoritative capacity total lives in the durable
// capacity ledger; a StaticPoolSource sidesteps it, so a non-dev server that is
// (now or in future) handed one - directly or wrapped by a LeaseAwarePoolSource
// - fails closed rather than silently scheduling against unenforced capacity.
// devMode mirrors the -dev-mode acknowledgment that already gates seeding the
// mutable registry from a flat file.
func GuardProductionPoolSource(source PoolSource, devMode bool) error {
	if devMode {
		return nil
	}
	for {
		switch typed := source.(type) {
		case StaticPoolSource:
			return fmt.Errorf("StaticPoolSource is dev/test only and bypasses the durable capacity ledger; build the pool source from the runtime registry or pass -dev-mode")
		case *LeaseAwarePoolSource:
			source = typed.static // unwrap and inspect the underlying source
		default:
			return nil
		}
	}
}

// LeaseAwarePoolSource overlays lease-derived runtime health onto a static
// pool configuration: a pool is ready only when its static
// configuration says ready AND its runtime instance is presumed alive by
// recent lease heartbeats. Placement therefore rejects pools whose worker
// stopped renewing its lease instead of scheduling a task that would be
// stranded until lease-expiry recovery. An instance ID missing from the
// health result is treated as unhealthy (fail-closed).
type LeaseAwarePoolSource struct {
	static    PoolSource
	health    store.PoolHealthStore
	freshness time.Duration
}

func NewLeaseAwarePoolSource(static PoolSource, health store.PoolHealthStore, freshness time.Duration) *LeaseAwarePoolSource {
	return &LeaseAwarePoolSource{static: static, health: health, freshness: freshness}
}

func (s *LeaseAwarePoolSource) ListRuntimePools(ctx context.Context, tenantID string) ([]RuntimePool, error) {
	pools, err := s.static.ListRuntimePools(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	var instanceIDs []string
	for _, pool := range pools {
		if pool.Ready {
			instanceIDs = append(instanceIDs, pool.RuntimeInstanceID)
		}
	}
	health, err := s.health.PoolInstanceHealth(ctx, instanceIDs, time.Now().UTC(), s.freshness)
	if err != nil {
		return nil, err
	}
	for i := range pools {
		if !pools[i].Ready {
			continue
		}
		alive, ok := health[pools[i].RuntimeInstanceID]
		if !ok || !alive {
			pools[i].Ready = false
		}
	}
	return pools, nil
}

type Controller struct {
	store    store.ControlStore
	pools    PoolSource
	ownerID  string
	batch    int
	claimTTL time.Duration
	leaseTTL time.Duration
	newID    func() uuid.UUID
	// parallel bounds concurrent per-task processing within one batch (P1);
	// 1 disables parallelism.
	parallel int
	// shardIndex / shardCount are the tenant-consistent claim shard
	// (ADR-016); zero count disables sharding.
	shardIndex int
	shardCount int
}

func NewController(repository store.ControlStore, pools PoolSource, ownerID string, batch int, claimTTL, leaseTTL time.Duration) *Controller {
	return &Controller{store: repository, pools: pools, ownerID: ownerID, batch: batch,
		claimTTL: claimTTL, leaseTTL: leaseTTL, newID: newUUIDv7, parallel: 4}
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

// WithShard confines this instance to one tenant-consistent claim shard
// (ADR-016): it claims only tasks whose tenant maps to index of count.
// Count 0 disables sharding. Every controller instance must share the same
// count.
func WithShard(index, count int) func(*Controller) {
	return func(c *Controller) { c.shardIndex, c.shardCount = index, count }
}

// Reconcile claims and schedules admitted tasks. Transient transaction
// conflicts are retried with bounded backoff (ADR-002).
func (c *Controller) Reconcile(ctx context.Context) (int, error) {
	return store.RetryRetryable(ctx, func() (int, error) { return c.reconcileOnce(ctx) })
}

func (c *Controller) reconcileOnce(ctx context.Context) (int, error) {
	claims, err := c.store.ClaimTasks(ctx, store.ClaimTasksInput{
		Kind: store.ControllerScheduling, Phase: "ADMITTED", OwnerID: c.ownerID, Limit: c.batch, TTL: c.claimTTL,
		ShardIndex: c.shardIndex, ShardCount: c.shardCount,
	})
	if err != nil {
		return 0, err
	}
	agentmetrics.SchedulerClaims(ctx, len(claims))
	agentmetrics.QueueDepth(ctx, "scheduler_claim_batch", int64(len(claims)))
	return c.processClaims(ctx, claims)
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

// processClaim runs one claim through placement, returning whether the claim
// was scheduled. Non-retryable failures are isolated per claim; retryable
// failures abort the batch.
func (c *Controller) processClaim(ctx context.Context, claim store.TaskClaim) (bool, error) {
	spec, err := workload.Decode(claim.Task.Spec)
	if err != nil {
		if store.IsRetryableTransaction(err) {
			return false, err
		}
		// Per-task error isolation: an undecodable admitted task must not
		// block the rest of the batch. The claim is released so the task
		// is re-evaluated (or handled by lifecycle timeout) later.
		_ = c.store.ReleaseTaskClaim(ctx, claim)
		slog.Error("schedule decode failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", err)
		return false, nil
	}
	pools, err := c.pools.ListRuntimePools(ctx, claim.Task.TenantID)
	if err != nil {
		if store.IsRetryableTransaction(err) {
			return false, err
		}
		_ = c.store.ReleaseTaskClaim(ctx, claim)
		slog.Error("runtime pool list failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", err)
		return false, nil
	}
	selection, err := selectForTask(spec, pools, claim.Task.ID)
	if err != nil {
		if errors.Is(err, ErrNoPlacement) {
			rejection, encodeErr := json.Marshal(selection.Rejected)
			if encodeErr != nil {
				return false, fmt.Errorf("encode placement rejection: %w", encodeErr)
			}
			// O6: release the claim immediately and defer the next attempt
			// with exponential backoff instead of pinning the claim until
			// its TTL. The deferral lives on the task, so every controller
			// instance honors the same backoff and the task is claimable
			// again the moment it elapses.
			_, deferErr := c.store.DeferTaskSchedule(ctx, store.DeferTaskScheduleInput{
				TaskID: claim.Task.ID, TenantID: claim.Task.TenantID, OwnerID: claim.OwnerID,
				ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: claim.Task.ResourceVersion,
				Until:     time.Now().UTC().Add(scheduleBackoff(claim.Task.ScheduleRetryCount, claim.Task.ID)),
				Rejection: rejection,
			})
			if deferErr != nil {
				if store.IsRetryableTransaction(deferErr) {
					return false, deferErr
				}
				// The deferral commit failed (stale claim or unexpected
				// store failure): release the claim so the task is not
				// pinned, and continue.
				_ = c.store.ReleaseTaskClaim(ctx, claim)
				slog.Error("schedule deferral failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", deferErr)
			}
			agentmetrics.SchedulerDeferral(ctx)
			agentmetrics.SchedulerOutcome(ctx, "placement_deferred")
			return false, nil
		}
		_ = c.store.ReleaseTaskClaim(ctx, claim)
		slog.Error("placement selection failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", err)
		return false, nil
	}
	// Walk the ranked candidates in order. A pool that filled between the
	// read-only placement pass and the transactional reservation only
	// eliminates itself; the next candidate is tried in the same reconcile
	// pass. The task is deferred with the accumulated per-candidate
	// diagnostics only after every candidate lost the race.
	capacityRaces := append([]Rejection(nil), selection.Rejected...)
	for _, candidate := range selection.Candidates {
		pool := candidate.Pool
		_, err = c.store.ScheduleTask(ctx, store.ScheduleTaskInput{
			TaskID: claim.Task.ID, TenantID: claim.Task.TenantID, OwnerID: claim.OwnerID,
			ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: claim.Task.ResourceVersion,
			RunID: c.newID(), AttemptID: c.newID(), LeaseID: c.newID(), RuntimePoolID: pool.ID,
			RuntimeClass: pool.RuntimeClass, RuntimeInstanceID: pool.RuntimeInstanceID, LeaseTTL: c.leaseTTL,
			RequestedCPU:    spec.Placement.CPU,
			RequestedMemory: spec.Placement.Memory, RequestedLLMSlots: spec.Placement.LLMConcurrency,
		})
		if err == nil {
			agentmetrics.SchedulerOutcome(ctx, "scheduled")
			return true, nil
		}
		if store.IsRetryableTransaction(err) {
			return false, err
		}
		if errors.Is(err, store.ErrCapacityExhausted) {
			capacityRaces = append(capacityRaces, Rejection{
				PoolID: pool.ID, Reasons: []string{"CAPACITY_RESERVATION_RACE"},
			})
			continue
		}
		// A stale claim or a per-task schedule failure: release and
		// continue so one task cannot starve the batch.
		_ = c.store.ReleaseTaskClaim(ctx, claim)
		slog.Error("schedule commit failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", err)
		agentmetrics.SchedulerOutcome(ctx, "schedule_failed")
		return false, nil
	}
	// Every candidate lost the capacity race. Persist the same bounded
	// backoff used before fallback existed instead of immediately
	// reclaiming the task in a hot loop.
	rejection, encodeErr := json.Marshal(capacityRaces)
	if encodeErr != nil {
		return false, fmt.Errorf("encode capacity rejection: %w", encodeErr)
	}
	_, deferErr := c.store.DeferTaskSchedule(ctx, store.DeferTaskScheduleInput{
		TaskID: claim.Task.ID, TenantID: claim.Task.TenantID, OwnerID: claim.OwnerID,
		ClaimFencingToken: claim.FencingToken, ExpectedTaskVersion: claim.Task.ResourceVersion,
		Until:     time.Now().UTC().Add(capacityBackoff(claim.Task.ScheduleRetryCount, claim.Task.ID)),
		Rejection: rejection,
	})
	if deferErr != nil {
		if store.IsRetryableTransaction(deferErr) {
			return false, deferErr
		}
		_ = c.store.ReleaseTaskClaim(ctx, claim)
		slog.Error("capacity deferral failed for task; isolated", "task", claim.Task.ID, "tenant", claim.Task.TenantID, "error", deferErr)
	}
	agentmetrics.SchedulerDeferral(ctx)
	agentmetrics.SchedulerOutcome(ctx, "placement_deferred")
	return false, nil
}

func newUUIDv7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New()
	}
	return id
}

// scheduleBackoff returns the exponential backoff for a task's next
// scheduling attempt after a no-placement (O6): 5s doubled per consecutive
// deferral, capped at 5 minutes. The retry count comes from the task row, so
// the progression survives controller restarts and is shared across
// instances.
func scheduleBackoff(retries int64, taskIDs ...uuid.UUID) time.Duration {
	const (
		base = 5 * time.Second
		max  = 5 * time.Minute
	)
	if retries < 0 {
		retries = 0
	}
	if retries > 6 {
		retries = 6
	}
	backoff := base << retries
	if backoff > max {
		return max
	}
	// Stable per-task jitter spreads deferred tasks without making replay
	// diagnostics nondeterministic. The window is capped at 25%.
	if len(taskIDs) > 0 && taskIDs[0] != uuid.Nil {
		window := backoff / 4
		if window > 0 {
			seed := int64(taskIDs[0][0])<<8 | int64(taskIDs[0][1])
			backoff += time.Duration(seed) % window
			if backoff > max {
				backoff = max
			}
		}
	}
	return backoff
}

// capacityBackoff is intentionally much shorter than a structural
// no-placement backoff. An atomic reservation race means a suitable pool
// exists and capacity is actively turning over; exponential multi-minute
// delays would strand runnable tasks after the fleet becomes idle.
func capacityBackoff(retries int64, taskIDs ...uuid.UUID) time.Duration {
	const (
		base = 100 * time.Millisecond
		max  = time.Second
	)
	if retries < 0 {
		retries = 0
	}
	if retries > 4 {
		retries = 4
	}
	backoff := base << retries
	if backoff > max {
		backoff = max
	}
	if len(taskIDs) > 0 && taskIDs[0] != uuid.Nil {
		window := backoff / 4
		seed := int64(taskIDs[0][0])<<8 | int64(taskIDs[0][1])
		backoff += time.Duration(seed) % window
		if backoff > max {
			backoff = max
		}
	}
	return backoff
}
