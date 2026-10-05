package syscall

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics defines the interface for collecting syscall metrics.
type Metrics interface {
	RecordSyscall(ctx context.Context, syscall SyscallNumber, code SyscallErrorCode, duration time.Duration)
	TotalCount() int64
	SuccessCount() int64
	ErrorCount() int64
}

// StandardMetrics collects Prometheus/OpenTelemetry metrics and in-memory counters.
type StandardMetrics struct {
	totalCalls      atomic.Int64
	successfulCalls atomic.Int64
	failedCalls     atomic.Int64

	otelCallsCounter  metric.Int64Counter
	otelDurationHist  metric.Float64Histogram
	otelErrorsCounter metric.Int64Counter
}

// NewStandardMetrics initializes OpenTelemetry instruments and atomic counters.
func NewStandardMetrics() *StandardMetrics {
	m := &StandardMetrics{}
	meter := otel.Meter("fenced.kernel.syscall")

	m.otelCallsCounter, _ = meter.Int64Counter("syscall_total")
	m.otelDurationHist, _ = meter.Float64Histogram("syscall_duration_seconds")
	m.otelErrorsCounter, _ = meter.Int64Counter("syscall_errors_total")

	return m
}

// RecordSyscall records the execution of a system call.
func (m *StandardMetrics) RecordSyscall(ctx context.Context, syscall SyscallNumber, code SyscallErrorCode, duration time.Duration) {
	m.totalCalls.Add(1)
	if code == SyscallOK {
		m.successfulCalls.Add(1)
	} else {
		m.failedCalls.Add(1)
	}

	attrs := []attribute.KeyValue{
		attribute.String("syscall", syscall.String()),
		attribute.String("category", syscall.Category()),
		attribute.String("error_code", code.StandardName()),
	}

	if m.otelCallsCounter != nil {
		m.otelCallsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.otelDurationHist != nil {
		m.otelDurationHist.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}
	if code != SyscallOK && m.otelErrorsCounter != nil {
		m.otelErrorsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// TotalCount returns total number of syscall invocations recorded.
func (m *StandardMetrics) TotalCount() int64 {
	return m.totalCalls.Load()
}

// SuccessCount returns number of successful syscall invocations.
func (m *StandardMetrics) SuccessCount() int64 {
	return m.successfulCalls.Load()
}

// ErrorCount returns number of failed syscall invocations.
func (m *StandardMetrics) ErrorCount() int64 {
	return m.failedCalls.Load()
}
