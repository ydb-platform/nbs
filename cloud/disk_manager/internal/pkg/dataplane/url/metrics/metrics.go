package metrics

import common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"

////////////////////////////////////////////////////////////////////////////////

type Metrics interface {
	StatRequest(request string) func(*error)
	OnHttpStatus(request string, status int)

	OnRequestSize(size uint64)
	OnCacheHit()
}

func New(registry common_metrics.Registry) Metrics {
	return newMetricsImpl(registry)
}
