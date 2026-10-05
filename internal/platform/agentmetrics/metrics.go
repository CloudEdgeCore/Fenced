// Package agentmetrics owns Fenced operational metrics. Cardinality is kept
// bounded deliberately: tenant/task/workflow identifiers belong on traces and
// logs, never metric attributes.
package agentmetrics

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var instruments struct {
	sync.Once
	schedulerClaims        metric.Int64Counter
	schedulerOutcomes      metric.Int64Counter
	workflowClaims         metric.Int64Counter
	workflowOutcomes       metric.Int64Counter
	spawnOutcomes          metric.Int64Counter
	budgetEvents           metric.Int64Counter
	accountingDrift        metric.Int64Counter
	queueDepth             metric.Int64Histogram
	firstTokenLatency      metric.Float64Histogram
	reconcileDuration      metric.Float64Histogram
	schedulerDeferrals     metric.Int64Counter
	leaseExpirations       metric.Int64Counter
	runtimeTakeovers       metric.Int64Counter
	dbTransactionRetries   metric.Int64Counter
	dbTransactionConflicts metric.Int64Counter
}

func initInstruments() {
	instruments.Do(func() {
		meter := otel.Meter("fenced.dev/kernel")
		instruments.schedulerClaims, _ = meter.Int64Counter("fenced.scheduler.claims")
		instruments.schedulerOutcomes, _ = meter.Int64Counter("fenced.scheduler.outcomes")
		instruments.workflowClaims, _ = meter.Int64Counter("fenced.orchestrator.claims")
		instruments.workflowOutcomes, _ = meter.Int64Counter("fenced.orchestrator.outcomes")
		instruments.spawnOutcomes, _ = meter.Int64Counter("fenced.workflow.spawn.outcomes")
		instruments.budgetEvents, _ = meter.Int64Counter("fenced.budget.events")
		instruments.accountingDrift, _ = meter.Int64Counter("fenced.accounting.reconciliation.drift")
		instruments.queueDepth, _ = meter.Int64Histogram("fenced.queue.depth")
		instruments.firstTokenLatency, _ = meter.Float64Histogram("fenced.model.first_token_latency_ms",
			metric.WithUnit("ms"),
			metric.WithDescription("model streaming time-to-first-token, measured at the kernel invoker"))
		instruments.reconcileDuration, _ = meter.Float64Histogram("fenced.orchestrator.reconcile.duration",
			metric.WithUnit("ms"),
			metric.WithDescription("duration of one workflow orchestrator reconcile round"))
		instruments.schedulerDeferrals, _ = meter.Int64Counter("fenced.scheduler.deferral.total",
			metric.WithDescription("scheduler deferrals due to capacity or lease contention"))
		instruments.leaseExpirations, _ = meter.Int64Counter("fenced.lease.expiration.total",
			metric.WithDescription("runtime leases observed expired by the recovery controller"))
		instruments.runtimeTakeovers, _ = meter.Int64Counter("fenced.runtime.takeover.total",
			metric.WithDescription("expired attempts successfully recovered onto a new run"))
		instruments.dbTransactionRetries, _ = meter.Int64Counter("fenced.db.transaction.retry.total",
			metric.WithDescription("retryable database transactions retried by the store"))
		instruments.dbTransactionConflicts, _ = meter.Int64Counter("fenced.db.transaction.conflict.total",
			metric.WithDescription("database transactions that aborted on serialization/version conflict"))
	})
}

func SchedulerClaims(ctx context.Context, count int) {
	initInstruments()
	instruments.schedulerClaims.Add(ctx, int64(count))
}

func SchedulerOutcome(ctx context.Context, outcome string) {
	initInstruments()
	instruments.schedulerOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func WorkflowClaims(ctx context.Context, count int) {
	initInstruments()
	instruments.workflowClaims.Add(ctx, int64(count))
}

func WorkflowOutcome(ctx context.Context, outcome string) {
	initInstruments()
	instruments.workflowOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func SpawnOutcome(ctx context.Context, outcome string) {
	initInstruments()
	instruments.spawnOutcomes.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func BudgetEvent(ctx context.Context, event, dimension string) {
	initInstruments()
	instruments.budgetEvents.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", event), attribute.String("dimension", dimension)))
}

func AccountingDrift(ctx context.Context, ledger string, count int64) {
	if count <= 0 {
		return
	}
	initInstruments()
	instruments.accountingDrift.Add(ctx, count, metric.WithAttributes(attribute.String("ledger", ledger)))
}

func QueueDepth(ctx context.Context, queue string, depth int64) {
	initInstruments()
	instruments.queueDepth.Record(ctx, depth, metric.WithAttributes(attribute.String("queue", queue)))
}

// ModelFirstTokenLatency records streaming time-to-first-token in milliseconds,
// attributed by provider only (bounded cardinality). Recorded at the kernel
// invoker so it is captured regardless of any downstream delta consumer.
func ModelFirstTokenLatency(ctx context.Context, provider string, ms float64) {
	initInstruments()
	instruments.firstTokenLatency.Record(ctx, ms, metric.WithAttributes(attribute.String("provider", provider)))
}

// OrchestratorReconcileDuration records the wall-clock time of one workflow
// orchestrate round in milliseconds.
func OrchestratorReconcileDuration(ctx context.Context, ms float64) {
	initInstruments()
	instruments.reconcileDuration.Record(ctx, ms)
}

// SchedulerDeferral increments the total number of scheduler deferrals caused
// by capacity exhaustion, lease contention, or other transient placement
// failures.
func SchedulerDeferral(ctx context.Context) {
	initInstruments()
	instruments.schedulerDeferrals.Add(ctx, 1)
}

// LeaseExpiration increments the total number of runtime lease expirations
// observed by the recovery controller.
func LeaseExpiration(ctx context.Context) {
	initInstruments()
	instruments.leaseExpirations.Add(ctx, 1)
}

// RuntimeTakeover increments the total number of expired attempts that the
// recovery controller successfully recovered onto a new run.
func RuntimeTakeover(ctx context.Context) {
	initInstruments()
	instruments.runtimeTakeovers.Add(ctx, 1)
}

// DBTransactionRetry increments the total number of retryable database
// transactions that were retried by the store layer.
func DBTransactionRetry(ctx context.Context) {
	initInstruments()
	instruments.dbTransactionRetries.Add(ctx, 1)
}

// DBTransactionConflict increments the total number of database transactions
// that aborted on serialization or version conflict.
func DBTransactionConflict(ctx context.Context) {
	initInstruments()
	instruments.dbTransactionConflicts.Add(ctx, 1)
}
