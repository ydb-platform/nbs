package tracing

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tracing_config "github.com/ydb-platform/nbs/cloud/tasks/tracing/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	otel_resource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////

const (
	tracerName = "yc-disk-manager"
)

////////////////////////////////////////////////////////////////////////////////

func StartSpan(
	ctx context.Context,
	spanName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {

	return otel.Tracer(tracerName).Start(ctx, spanName, opts...)
}

////////////////////////////////////////////////////////////////////////////////

func newTraceExporter(
	ctx context.Context,
	config *tracing_config.TracingConfig,
) (*otlptrace.Exporter, error) {
	return otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(fmt.Sprintf(
			"localhost:%v",
			config.ExporterPort)),
		otlptracegrpc.WithInsecure(),
	)
}

////////////////////////////////////////////////////////////////////////////////

func InitOpentelemetryTracing(
	ctx context.Context,
	config *tracing_config.TracingConfig,
) (shutdown func(context.Context) error, err error) {

	traceExporter, err := newTraceExporter(ctx, config)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf("Failed to create exporter: %w", err)
	}

	serviceName := *config.ServiceName
	fmt.Printf("InitOpentelemetryTracing config.ServiceName: %v\n", serviceName)
	resource := otel_resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
	)

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(resource),
	)
	otel.SetTracerProvider(tracerProvider)

	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tracerProvider.Shutdown, nil
}
