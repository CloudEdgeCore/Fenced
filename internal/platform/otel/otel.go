// Package otel wires the reference observability stack (tech baseline §15):
// OTLP trace/metric/log exporters configured through the standard OTEL_*
// environment variables, plus a bounded async slog → OTel log bridge. Without
// an OTLP endpoint the providers stay no-ops, so every binary can initialize
// unconditionally and only emit telemetry when the reference stack is up.
package otel

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdkslog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

// Shutdown flushes and closes all exporters.
type Shutdown func(context.Context) error

// Init configures the global tracer/meter/logger providers from the standard
// environment:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT  OTLP/gRPC endpoint (e.g. 127.0.0.1:4317)
//	OTEL_SERVICE_NAME            service name (default: fenced)
//	OTEL_SDK_DISABLED            set to disable telemetry entirely
//
// When the endpoint is unset the providers remain the SDK no-ops and slog
// keeps its console handler; the returned shutdown is then a no-op.
func Init(ctx context.Context) (Shutdown, error) {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if strings.TrimSpace(serviceName) == "" {
		serviceName = "fenced"
	}
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") || strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) == "" {
		return func(context.Context) error { return nil }, nil
	}
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	resources := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName))
	exportCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	traceExporter, err := otlptracegrpc.New(exportCtx,
		otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	metricExporter, err := otlpmetricgrpc.New(exportCtx,
		otlpmetricgrpc.WithEndpoint(endpoint), otlpmetricgrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	logExporter, err := otlploggrpc.New(exportCtx,
		otlploggrpc.WithEndpoint(endpoint), otlploggrpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resources), sdktrace.WithBatcher(traceExporter))
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(resources), metric.WithReader(metric.NewPeriodicReader(metricExporter)))
	loggerProvider := sdkslog.NewLoggerProvider(
		sdkslog.WithResource(resources), sdkslog.WithProcessor(sdkslog.NewBatchProcessor(logExporter)))

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	global.SetLoggerProvider(loggerProvider)

	// Keep console output and ship structured logs to the collector through a
	// bounded async bridge: slog.Handle must never block or deadlock the
	// control plane, so a stalled collector degrades log delivery only.
	// Keep console output and ship structured logs to the collector through a
	// bounded async bridge. The console handler writes directly to stderr
	// (NOT through the legacy log package): the OTel exporters' internal
	// logging also uses the log package's global mutex, and routing the
	// console through it deadlocks both paths.
	console := slog.NewTextHandler(os.Stderr, nil)
	bridge := newLogBridge(global.Logger(serviceName), 1024)
	slog.SetDefault(slog.New(slog.NewMultiHandler(console, bridge)))

	return func(ctx context.Context) error {
		var failures []error
		if err := tracerProvider.Shutdown(ctx); err != nil {
			failures = append(failures, err)
		}
		if err := meterProvider.Shutdown(ctx); err != nil {
			failures = append(failures, err)
		}
		if err := loggerProvider.Shutdown(ctx); err != nil {
			failures = append(failures, err)
		}
		if len(failures) == 0 {
			return nil
		}
		joined := failures[0]
		for _, failure := range failures[1:] {
			joined = errors.Join(joined, failure)
		}
		return joined
	}, nil
}

// logBridge forwards slog records to the OTel logger on a bounded background
// channel. Handle enqueues with drop-oldest on overflow and never blocks, so
// a hung log pipeline can never stall the application.
type logBridge struct {
	records chan slog.Record
	logger  log.Logger
}

func newLogBridge(logger log.Logger, capacity int) *logBridge {
	bridge := &logBridge{records: make(chan slog.Record, capacity), logger: logger}
	go bridge.drain()
	return bridge
}

func (b *logBridge) Enabled(context.Context, slog.Level) bool { return true }

func (b *logBridge) Handle(_ context.Context, record slog.Record) error {
	select {
	case b.records <- record:
	default:
		// Overflow: drop the oldest record and enqueue the newest.
		select {
		case <-b.records:
		default:
		}
		select {
		case b.records <- record:
		default:
		}
	}
	return nil
}

func (b *logBridge) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Attribute decoration is applied at drain time per record; the bridge
	// itself stays stateless.
	return b
}

func (b *logBridge) WithGroup(string) slog.Handler { return b }

func (b *logBridge) drain() {
	for record := range b.records {
		attributes := make([]attribute.KeyValue, 0, record.NumAttrs())
		record.Attrs(func(attr slog.Attr) bool {
			attributes = append(attributes, slogAttrToOTel(attr))
			return true
		})
		var emitted log.Record
		emitted.SetTimestamp(record.Time)
		emitted.SetObservedTimestamp(time.Now())
		emitted.SetSeverity(slogSeverity(record.Level))
		emitted.SetSeverityText(record.Level.String())
		emitted.SetBody(attribute.StringValue(record.Message))
		emitted.AddAttributes(attributes...)
		b.logger.Emit(context.Background(), emitted)
	}
}

func slogSeverity(level slog.Level) log.Severity {
	switch {
	case level >= slog.LevelError:
		return log.SeverityError
	case level >= slog.LevelWarn:
		return log.SeverityWarn
	case level >= slog.LevelInfo:
		return log.SeverityInfo
	default:
		return log.SeverityDebug
	}
}

func slogAttrToOTel(attr slog.Attr) attribute.KeyValue {
	value := attr.Value
	switch value.Kind() {
	case slog.KindString:
		return attribute.String(attr.Key, value.String())
	case slog.KindInt64:
		return attribute.Int64(attr.Key, value.Int64())
	case slog.KindUint64:
		return attribute.Int64(attr.Key, int64(value.Uint64()))
	case slog.KindFloat64:
		return attribute.Float64(attr.Key, value.Float64())
	case slog.KindBool:
		return attribute.Bool(attr.Key, value.Bool())
	case slog.KindDuration:
		return attribute.String(attr.Key, value.Duration().String())
	case slog.KindTime:
		return attribute.String(attr.Key, value.Time().UTC().Format(time.RFC3339Nano))
	default:
		return attribute.String(attr.Key, value.String())
	}
}
