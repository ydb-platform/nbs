package metrics

import (
	"time"

	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Metrics interface {
	StatOperation(name string) func(err *error)
	OnChunkCompressed(
		origSize int,
		compressedSize int,
		duration time.Duration,
		compression string,
	)
	OnChunkDecompressed(
		duration time.Duration,
		compression string,
	)
}

func New(registry common_metrics.Registry, storageType string) Metrics {
	return &storageMetricsImpl{
		registry:                registry,
		operationMetrics:        make(map[string]*operationStats),
		chunkCompressionMetrics: make(map[string]chunkCompressionMetrics),
		storageType:             storageType,
	}
}
