package tracing

import (
	"fmt"

	tracing_config "github.com/ydb-platform/nbs/cloud/tasks/tracing/config"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TODO:_ rename file: sampler.go -> sampling.go?

// TODO:_ ???
// const (
// 	taskGenerationKey = "task_generation" // TODO:_ naming?
// 	taskIdKey         = "task_id"         // TODO:_ naming?
// )

func NewSampler(config *tracing_config.SamplingConfig) sdktrace.Sampler {
	fmt.Println("SAMPLER: creating new sampler")
	return &sampler{config}
}

type sampler struct {
	config *tracing_config.SamplingConfig
}

func (s *sampler) ShouldSample(
	params sdktrace.SamplingParameters,
) sdktrace.SamplingResult {
	// TODO:_
	aaa := params.Attributes[0].Value.AsString()
	fmt.Println(aaa)
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
}

// func shouldSampleCheckParent(
// 	params sdktrace.SamplingParameters,
// ) {
// }

func (s *sampler) Description() string {
	return "AAAAAAAAAA" // TODO:_ description
}
