package metrics

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Metrics interface {
	OnError(err error)
	StatRequest(name string) func(err *error)
}

func NewClientMetrics(registry metrics.Registry) Metrics {
	return &clientMetrics{
		registry:         registry,
		underlyingErrors: registry.Counter("underlying_errors"),
		requestStats:     make(map[string]*requestStats),
	}
}

func NewSessionMetrics(
	registry metrics.Registry,
	tags map[string]string,
) Metrics {

	return &clientMetrics{
		registry:         registry.WithTags(tags),
		underlyingErrors: registry.Counter("underlying_errors"),
		requestStats:     make(map[string]*requestStats),
	}
}
