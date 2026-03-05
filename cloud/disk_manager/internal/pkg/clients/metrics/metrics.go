package metrics

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Metrics interface {
	OnError(err error)
	StatRequest(name string) func(err *error)
}

func New(
	registry metrics.Registry,
	tags map[string]string,
) Metrics {

	registry = registry.WithTags(tags)

	return &clientMetrics{
		registry:         registry,
		underlyingErrors: registry.Counter("underlying_errors"),
		requestStats:     make(map[string]*requestStats),
	}
}
