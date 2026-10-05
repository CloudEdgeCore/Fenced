package store

import (
	"context"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/observability"
)

// MetricsStore exposes the aggregated platform observability surface (§Phase-7
// core metrics), consistent with the data the store's AggregateMetrics method
// produces.
type MetricsStore interface {
	AggregateMetrics(context.Context, string, time.Time) (*observability.Metrics, error)
}
