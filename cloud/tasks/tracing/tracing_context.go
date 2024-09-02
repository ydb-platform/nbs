package tracing

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

////////////////////////////////////////////////////////////////////////////////

const (
	traceparentHeaderKey = "traceparent"
	tracestateHeaderKey  = "tracestate"
)

////////////////////////////////////////////////////////////////////////////////

// Gets traceparent and tracestate headers from incoming grpc metadata.
func GetTracingContext(ctx context.Context) context.Context {
	tracingContext := headers.GetFromIncomingContext(ctx, []string{
		traceparentHeaderKey,
		tracestateHeaderKey,
	})

	traceparent, ok := tracingContext[traceparentHeaderKey]
	if !ok || len(traceparent) == 0 {
		logging.Debug(ctx, "No traceparent in incoming tracing context")
		return ctx
	}

	prop := otel.GetTextMapPropagator()
	return prop.Extract(ctx, propagation.MapCarrier(tracingContext))
}

// Sets traceparent and tracestate headers in grpc metadata
// (both incoming and outgoing).
// If these fields are already present in grpc metadata,
// their values will be overwritten.
func SetTracingContext(ctx context.Context) context.Context {
	mapCarrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, mapCarrier)

	tracingContextHeaders := make(map[string]string)

	traceparent, ok := mapCarrier[traceparentHeaderKey]
	if !ok {
		logging.Debug(ctx, "No traceparent in current tracing context")
		return ctx
	}
	tracingContextHeaders[traceparentHeaderKey] = traceparent

	tracestate, ok := mapCarrier[tracestateHeaderKey]
	if ok {
		tracingContextHeaders[tracestateHeaderKey] = tracestate
	}

	return headers.Replace(ctx, tracingContextHeaders)
}
