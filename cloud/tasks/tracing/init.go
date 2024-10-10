package tracing

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tracing_config "github.com/ydb-platform/nbs/cloud/tasks/tracing/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
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
	tracerName = "disk-manager"
)

////////////////////////////////////////////////////////////////////////////////

func StartSpan(
	ctx context.Context,
	spanName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {

	return otel.Tracer(tracerName).Start(ctx, spanName, opts...)
}

func StartSpanWithSampling(
	ctx context.Context,
	spanName string,
	sampled bool,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {

	opts = append(opts, WithAttributes(newSampledAttribute(sampled)))
	return StartSpan(ctx, spanName, opts...)
}

////////////////////////////////////////////////////////////////////////////////

func SetError(span trace.Span, err *error) {
	if *err != nil {
		span.SetStatus(codes.Error, (*err).Error())
	}
}

////////////////////////////////////////////////////////////////////////////////

func newTraceExporter(
	ctx context.Context,
	config *tracing_config.TracingConfig,
) (*otlptrace.Exporter, error) {

	return otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint(
			fmt.Sprintf("localhost:%v", *config.Port),
		),
		otlptracegrpc.WithInsecure(),
	)
}

func newParentBasedSampler() sdktrace.Sampler {
	rootSampler := newSampler()
	// If a parent span is not sampled, then the child span will not be sampled
	// also. If a parent span is sampled, then the rootSampler is used to make
	// sampling decision about the child span.
	return sdktrace.ParentBased(
		rootSampler,
		sdktrace.WithLocalParentSampled(rootSampler),
		sdktrace.WithRemoteParentSampled(rootSampler),
	)
}

////////////////////////////////////////////////////////////////////////////////

func InitTracing(
	ctx context.Context,
	config *tracing_config.TracingConfig,
) (shutdown func(context.Context) error, err error) {

	traceExporter, err := newTraceExporter(ctx, config)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf("failed to create trace exporter: %w", err)
	}

	resource := otel_resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(*config.ServiceName),
	)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithSampler(newParentBasedSampler()),
		sdktrace.WithResource(resource),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tracerProvider.Shutdown, nil
}
