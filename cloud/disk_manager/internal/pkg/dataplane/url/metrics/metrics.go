package metrics

import (
	"net/http"

	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Metrics interface {
	StatRequest(method string) func(**http.Response, *error)

	OnRequestSize(size uint64)
	OnCacheHit()
}

func New(registry common_metrics.Registry) Metrics {
	return newMetricsImpl(registry)
}
