package metrics

import (
	"sync"
	"time"

	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type operationStats struct {
	count     common_metrics.Counter
	histogram common_metrics.Timer
	errors    common_metrics.Counter
}

func (s *operationStats) onCount() {
	s.count.Inc()
}

func (s *operationStats) recordDuration(duration time.Duration) {
	s.histogram.RecordDuration(duration)
}

func (s *operationStats) onError() {
	s.errors.Inc()
}

////////////////////////////////////////////////////////////////////////////////

func operationDurationBuckets() common_metrics.DurationBuckets {
	return common_metrics.NewDurationBuckets(
		10*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond,
		100*time.Millisecond, 200*time.Millisecond, 500*time.Millisecond,
		1*time.Second, 2*time.Second, 5*time.Second,
	)
}

func newOperationStats(registry common_metrics.Registry) *operationStats {
	return &operationStats{
		count:     registry.Counter("count"),
		histogram: registry.DurationHistogram("time", operationDurationBuckets()),
		errors:    registry.Counter("errors"),
	}
}

////////////////////////////////////////////////////////////////////////////////

type storageMetricsImpl struct {
	registry              common_metrics.Registry
	operationMetrics      map[string]*operationStats
	operationMetricsMutex sync.Mutex
	storageType           string
}

func (m *storageMetricsImpl) getOrNewOperationStats(
	name string,
) *operationStats {

	m.operationMetricsMutex.Lock()
	defer m.operationMetricsMutex.Unlock()

	stats, ok := m.operationMetrics[name]
	if !ok {
		stats = newOperationStats(m.registry.WithTags(map[string]string{
			"operation": name,
			"storage":   m.storageType,
		}))
		m.operationMetrics[name] = stats
	}

	return stats
}

func (m *storageMetricsImpl) StatCall(name string) func(err *error) {
	start := time.Now()
	stats := m.getOrNewOperationStats(name)

	return func(err *error) {
		if *err != nil {
			stats.onError()
		} else {
			stats.onCount()
			stats.recordDuration(time.Since(start))
		}
	}
}
