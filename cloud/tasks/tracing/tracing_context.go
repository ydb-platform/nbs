package tracing

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

////////////////////////////////////////////////////////////////////////////////

// Gets traceparent and tracestate headers from incoming grpc metadata.
func GetTracingContext(ctx context.Context) context.Context {
	tracingContext := headers.GetFromIncomingContext(ctx, []string{
		headers.TraceparentHeaderKey,
		headers.TracestateHeaderKey,
	})

	traceparent, ok := tracingContext[headers.TraceparentHeaderKey]
	if !ok || len(traceparent) == 0 {
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

	traceparent, ok := mapCarrier[headers.TraceparentHeaderKey]
	if !ok {
		return ctx
	}
	tracingContextHeaders[headers.TraceparentHeaderKey] = traceparent

	tracestate, ok := mapCarrier[headers.TracestateHeaderKey]
	if ok {
		tracingContextHeaders[headers.TracestateHeaderKey] = tracestate
	}
	return headers.Replace(ctx, tracingContextHeaders)
}
