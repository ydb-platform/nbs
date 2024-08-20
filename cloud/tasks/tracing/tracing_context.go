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

// Extracts traceparent and tracestate headers from grpc metadata.
func ExtractTracingContext(
	ctx context.Context,
) context.Context {

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

// Injects (appends) traceparent and tracestate headers to grpc metadata
func InjectTracingContext(
	ctx context.Context,
) context.Context {
	mapCarrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, mapCarrier)

	tracingContextHeaders := make(map[string]string)

	traceparent, ok := mapCarrier[traceparentHeaderKey]
	if !ok {
		return ctx
	}
	tracingContextHeaders[traceparentHeaderKey] = traceparent // TODO:_ or just pass mapCarrier to headers.Append?

	tracestate, ok := mapCarrier[tracestateHeaderKey]
	if ok {
		tracingContextHeaders[tracestateHeaderKey] = tracestate
	}

	return headers.Append(ctx, tracingContextHeaders)
}
