package nbs

import (
	"sync"
	"time"

	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

func requestDurationBuckets() metrics.DurationBuckets {
	return metrics.NewDurationBuckets(
		1*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond,
		100*time.Millisecond, 200*time.Millisecond, 500*time.Millisecond,
		1*time.Second, 2*time.Second, 5*time.Second, 10*time.Second,
		20*time.Second, 30*time.Second,
	)
}

////////////////////////////////////////////////////////////////////////////////

type requestStats struct {
	errors        metrics.Counter
	count         metrics.Counter
	timeHistogram metrics.Timer
}

func (s *requestStats) onCount() {
	s.count.Inc()
}

func (s *requestStats) onError() {
	s.errors.Inc()
}

func (s *requestStats) recordDuration(duration time.Duration) {
	s.timeHistogram.RecordDuration(duration)
}

////////////////////////////////////////////////////////////////////////////////

type clientMetrics struct {
	registry         metrics.Registry
	underlyingErrors metrics.Counter
	requestStats     map[string]*requestStats
	requestMutex     sync.Mutex
}

func (m *clientMetrics) getOrNewRequestStats(
	name string,
) *requestStats {

	m.requestMutex.Lock()
	defer m.requestMutex.Unlock()

	stats, ok := m.requestStats[name]
	if !ok {
		subRegistry := m.registry.WithTags(map[string]string{
			"request": name,
		})

		stats = &requestStats{
			errors:        subRegistry.Counter("errors"),
			count:         subRegistry.Counter("count"),
			timeHistogram: subRegistry.DurationHistogram("time", requestDurationBuckets()),
		}

		m.requestStats[name] = stats
	}

	return stats
}

func (m *clientMetrics) OnError(err nbs_client.ClientError) {
	// TODO: split metrics into types (retriable, fatal, etc.)
	m.underlyingErrors.Inc()
}

func (m *clientMetrics) StatRequest(name string) func(err *error) {
	start := time.Now()
	stats := m.getOrNewRequestStats(name)

	return func(err *error) {
		if *err != nil {
			stats.onError()
		} else {
			stats.onCount()
			stats.recordDuration(time.Since(start))
		}
	}
}

func newClientMetrics(registry metrics.Registry) *clientMetrics {
	return &clientMetrics{
		registry:         registry,
		underlyingErrors: registry.Counter("underlying_errors"),
		requestStats:     make(map[string]*requestStats),
	}
}

////////////////////////////////////////////////////////////////////////////////

type sessionMetrics struct {
	registry         metrics.Registry
	underlyingErrors metrics.Counter
	requestStats     map[string]*requestStats
	requestMutex     sync.Mutex
}

func (m *sessionMetrics) getOrNewRequestStats(
	name string,
) *requestStats {

	m.requestMutex.Lock()
	defer m.requestMutex.Unlock()

	stats, ok := m.requestStats[name]
	if !ok {
		subRegistry := m.registry.WithTags(map[string]string{
			"request": name,
		})

		stats = &requestStats{
			errors:        subRegistry.Counter("errors"),
			count:         subRegistry.Counter("count"),
			timeHistogram: subRegistry.DurationHistogram("time", requestDurationBuckets()),
		}

		m.requestStats[name] = stats
	}

	return stats
}

func (m *sessionMetrics) OnError(err nbs_client.ClientError) {
	// TODO: split metrics into types (retriable, fatal, etc.)
	m.underlyingErrors.Inc()
}

func (m *sessionMetrics) StatRequest(name string) func(err *error) {
	start := time.Now()
	stats := m.getOrNewRequestStats(name)

	return func(err *error) {
		if *err != nil {
			stats.onError()
		} else {
			stats.onCount()
			stats.recordDuration(time.Since(start))
		}
	}
}

func newSessionMetrics(
	registry metrics.Registry,
	host string,
) *sessionMetrics {

	return &sessionMetrics{
		registry: registry.WithTags(map[string]string{
			"request_host": host,
		}),
		underlyingErrors: registry.Counter("underlying_errors"),
		requestStats:     make(map[string]*requestStats),
	}
}
