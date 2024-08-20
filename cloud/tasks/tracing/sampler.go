package tracing

import (
	"fmt"
	"hash/crc32"

	tracing_config "github.com/ydb-platform/nbs/cloud/tasks/tracing/config"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TODO:_ this sampler knows about tasks. Move it out from tracing directory.

// TODO:_ should these constants be in another file? Or maybe we don't need them?
const (
	taskGenerationKey = "task_generation" // TODO:_ naming?
	taskIdKey         = "task_id"         // TODO:_ naming?
)

func NewSampler(config *tracing_config.SamplingConfig) sdktrace.Sampler {
	fmt.Println("SAMPLER: creating new sampler")
	return &sampler{
		softBarrier:    *config.SoftBarrier,
		hardBarrier:    *config.HardBarrier,
		softPercentage: *config.SoftPercentage,
	}
}

type sampler struct {
	softBarrier    uint64
	hardBarrier    uint64
	softPercentage uint32
}

func (s *sampler) ShouldSample(
	params sdktrace.SamplingParameters,
) sdktrace.SamplingResult {
	fmt.Println("SAMPLER: AAAAAAAAAAAA")
	// TODO:_ parse normally
	// TODO:_ !!! handle the case when there is no task id and generation !!!
	if len(params.Attributes) < 2 {
		fmt.Println("SAMPLER: Not enough attributes")
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
	}
	taskID := params.Attributes[0].Value.AsString()                // TODO:_ integer type hell
	taskGeneration := uint64(params.Attributes[1].Value.AsInt64()) // TODO:_ naming: generationID?
	regular := params.Attributes[3].Value.AsBool()                 // TODO:_ naming: generationID?
	fmt.Printf("SAMPLER: taskID = %v\n", taskID)
	fmt.Printf("SAMPLER: taskGeneration = %v\n", taskGeneration)
	fmt.Printf("SAMPLER: regular = %v\n", regular)

	if regular {
		fmt.Println("SAMPLER: task is regular, no span")
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}

	if taskGeneration > s.hardBarrier {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	if taskGeneration > s.softBarrier {
		hash := crc32.ChecksumIEEE([]byte(taskID + string(taskGeneration))) // TODO:_ make normal string, not string of one rune?
		if hash%100 >= s.softPercentage {
			return sdktrace.SamplingResult{Decision: sdktrace.Drop}
		}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
}

func (s *sampler) Description() string {
	return "AAAAAAAAAA" // TODO:_ description
}
