package metrics

import (
	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Metrics interface {
	StatCall(name string) func(err *error)
}

func New(registry common_metrics.Registry, storageType string) Metrics {
	return &storageMetricsImpl{
		registry:         registry,
		operationMetrics: make(map[string]*operationStats),
		storageType:      storageType,
	}
}
