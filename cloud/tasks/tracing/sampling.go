package tracing

// TODO:_ check style of imports
import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

////////////////////////////////////////////////////////////////////////////////

// TODO:_ comments about how this option interfere with parents and maybe other options?
const (
	sampledAttributeKey = "should_sample" // TODO:_ naming: is name 'sampled' confusing?
)

////////////////////////////////////////////////////////////////////////////////

type sampler struct{}

func newSampler() sdktrace.Sampler {
	return &sampler{}
}

////////////////////////////////////////////////////////////////////////////////

func (s *sampler) ShouldSample(
	params sdktrace.SamplingParameters,
) sdktrace.SamplingResult {

	if !hasShouldSampleAttribute(params) {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
}

func (s *sampler) Description() string {
	return "AAAAAAAAAA" // TODO:_ description
}

////////////////////////////////////////////////////////////////////////////////

func makeShouldSampleAttribute(sampled bool) attribute.KeyValue { // TODO:_ naming: make?
	return attribute.Bool(sampledAttributeKey, sampled)
}

func hasShouldSampleAttribute(params sdktrace.SamplingParameters) bool {
	for _, attr := range params.Attributes {
		if attr.Key == sampledAttributeKey {
			// TODO:_ ensure that this is bool?
			return attr.Value.AsBool()
		}
	}
	return true
}
