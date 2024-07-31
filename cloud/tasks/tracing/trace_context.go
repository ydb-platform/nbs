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
func ExtractTraceContext(
	ctx context.Context,
) context.Context {

	traceContext := headers.GetTraceContext(ctx, []string{
		traceparentHeaderKey,
		tracestateHeaderKey,
	})

	traceparent, ok := traceContext[traceparentHeaderKey]
	if !ok || len(traceparent) == 0 {
		logging.Debug(ctx, "No traceparent in incoming trace context")
		return ctx
	}

	prop := otel.GetTextMapPropagator()
	return prop.Extract(ctx, propagation.MapCarrier(traceContext))
}
