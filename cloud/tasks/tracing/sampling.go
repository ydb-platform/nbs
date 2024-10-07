package tracing

import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

////////////////////////////////////////////////////////////////////////////////

const (
	sampledAttributeKey = "sampled"
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
	return "Basic cloud.tasks sampler. " +
		"Uses attribute 'sampled' of the span for sampling decision."

}

////////////////////////////////////////////////////////////////////////////////

func newShouldSampleAttribute(sampled bool) attribute.KeyValue {
	return attribute.Bool(sampledAttributeKey, sampled)
}

func hasShouldSampleAttribute(params sdktrace.SamplingParameters) bool {
	for _, attr := range params.Attributes {
		if attr.Key == sampledAttributeKey {
			return attr.Value.AsBool()
		}
	}
	return true
}
