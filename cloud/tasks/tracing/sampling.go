package tracing

import (
	"fmt"

	tracing_config "github.com/ydb-platform/nbs/cloud/tasks/tracing/config"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	shouldSampleAttributeKey = "should_sample"
)

func NewSampler(config *tracing_config.SamplingConfig) sdktrace.Sampler {
	fmt.Println("SAMPLER: creating new sampler")
	return &sampler{config}
}

type sampler struct {
	config *tracing_config.SamplingConfig
}

// TODO:_ maybe we don't even need sampling with should_sample flag?
// TODO:_ this flag looks strange, we can don't start span instead passing this flag.
// TODO:_ BUT! think about children spans. They should see that parent is not sampled.
func (s *sampler) ShouldSample(
	params sdktrace.SamplingParameters,
) sdktrace.SamplingResult {

	// TODO:_ need any parameters here?
	fmt.Printf(
		"hmm: %v, noob: %v\n",
		*s.config.Hmm,
		*s.config.Noob,
	)

	if !hasShouldSampleAttribute(params) {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}

	// if parentReachedChildrenLimit(params) {
	// 	return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	// }

	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample} // TODO:_ should fill tracestate field?
}

// func shouldSampleCheckParent(
// 	params sdktrace.SamplingParameters,
// ) {
// }

func (s *sampler) Description() string {
	return "AAAAAAAAAA" // TODO:_ description
}

func hasShouldSampleAttribute(
	params sdktrace.SamplingParameters,
) bool {

	for _, attr := range params.Attributes {
		if attr.Key == shouldSampleAttributeKey {
			// TODO:_ how to check that this is bool?
			return attr.Value.AsBool()
		}
	}
	return true
}

// TODO:_ this need spanMetadata stored in context (see compute's example)
// TODO:_ is we don't do anything with eternal tasks, tracing service will drop spans.
// TODO:_ but we can't be sure about other backends.
//
// func parentReachedChildrenLimit(
// 	params sdktrace.SamplingParameters,
// ) bool {
//
// 	parentSpanContext := trace.SpanContextFromContext(params.ParentContext)
//
// 	if parentSpanContext.HasSpanID() && !parentSpanContext.IsSampled() {
// 		return false
// 	}
//
// 	/// ????
// 	if v := params.ParentContext.Value(spanMetadataKey{}); v != nil {
// 		parentSpanMetadata, ok := v.(*spanMetadata)
// 		if !ok {
// 			// TODO:_ maybe no panic here? Panic because of tracing sounds not good.
// 			panic("unexpected type of value in context by spanMetadataKey")
// 		}
// 	}
//
// 	return false
//
// 	else if v := p.ParentContext.Value(spanMetadataKey{}); v != nil {
// 		// check parent rate&children
// 		parent, ok := v.(*spanMetadata)
// 		if !ok {
// 			panic("unexpected span metadata")
// 		}
// 		now := time.Now()
// 		parent.Lock()
// 		last := parent.last
// 		chilren := parent.children
// 		parent.last = now
// 		parent.Unlock()
// 		if chilren > s.burstCount {
// 			desicion = tracesdk.Drop
// 			trace.SpanFromContext(p.ParentContext).SetAttributes(attribute.Bool("limited-size", true))
// 		}
// 		if s.limitRate != 0 && now.Sub(last).Seconds() < 1/float64(s.limitRate) {
// 			desicion = tracesdk.Drop
// 			trace.SpanFromContext(p.ParentContext).SetAttributes(attribute.Bool("limited-rate", true))
// 		}
// 	}
// 	return tracesdk.SamplingResult{
// 		Decision:   desicion,
// 		Tracestate: psc.TraceState(),
// 	}
// }
