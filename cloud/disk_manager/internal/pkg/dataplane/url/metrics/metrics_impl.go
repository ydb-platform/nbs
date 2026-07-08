package metrics

import (
	"fmt"
	"sync"
	"time"

	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

func requestDurationBuckets() common_metrics.DurationBuckets {
	return common_metrics.NewDurationBuckets(
		10*time.Millisecond, 25*time.Millisecond, 50*time.Millisecond,
		75*time.Millisecond, 100*time.Millisecond, 200*time.Millisecond,
		300*time.Millisecond, 500*time.Millisecond, 1*time.Second,
		2*time.Second, 5*time.Second, 10*time.Second, 30*time.Second,
	)
}

func requestSizeBuckets() common_metrics.Buckets {
	// Fine-grained from 128 B to 4 KiB, then sparse up to 4 MiB.
	return common_metrics.NewBuckets(
		128, 256, 512, 1024, 2048, 4096,
		65536,   // 64 KiB
		1048576, // 1 MiB
		4194304, // 4 MiB
	)
}

////////////////////////////////////////////////////////////////////////////////

type requestMetrics struct {
	registry    common_metrics.Registry
	errors      common_metrics.Counter
	requestTime common_metrics.Timer
	responses   map[int]common_metrics.Counter
	responsesMu sync.Mutex
}

func newRequestMetrics(registry common_metrics.Registry) *requestMetrics {
	return &requestMetrics{
		registry:    registry,
		errors:      registry.Counter("errors"),
		requestTime: registry.DurationHistogram("time", requestDurationBuckets()),
		responses:   make(map[int]common_metrics.Counter),
	}
}

func (m *requestMetrics) getOrNewResponseCounter(statusCode int) common_metrics.Counter {
	m.responsesMu.Lock()
	defer m.responsesMu.Unlock()

	if c, ok := m.responses[statusCode]; ok {
		return c
	}

	c := m.registry.WithTags(map[string]string{
		"status": fmt.Sprintf("%d", statusCode),
	}).Counter("responses")
	m.responses[statusCode] = c
	return c
}

func (m *requestMetrics) onHttpStatus(statusCode int) {
	m.getOrNewResponseCounter(statusCode).Inc()
}

func (m *requestMetrics) stat() func(*error) {
	start := time.Now()
	return func(err *error) {
		if *err != nil {
			m.errors.Inc()
		} else {
			m.requestTime.RecordDuration(time.Since(start))
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

type urlMetricsImpl struct {
	registry    common_metrics.Registry
	requests    map[string]*requestMetrics
	requestsMu  sync.Mutex
	requestSize common_metrics.Histogram
	cacheHits   common_metrics.Counter
}

func newMetricsImpl(registry common_metrics.Registry) *urlMetricsImpl {
	subRegistry := registry.WithTags(map[string]string{
		"component": "url_source",
	})
	return &urlMetricsImpl{
		registry:    subRegistry,
		requests:    make(map[string]*requestMetrics),
		requestSize: subRegistry.Histogram("requestSize", requestSizeBuckets()),
		cacheHits:   subRegistry.Counter("cacheHits"),
	}
}

func (m *urlMetricsImpl) requestMetrics(request string) *requestMetrics {
	m.requestsMu.Lock()
	defer m.requestsMu.Unlock()

	requestMetrics, ok := m.requests[request]
	if !ok {
		requestMetrics = newRequestMetrics(m.registry.WithTags(map[string]string{
			"method": request,
		}))
		m.requests[request] = requestMetrics
	}

	return requestMetrics
}

func (m *urlMetricsImpl) StatRequest(request string) func(*error) {
	return m.requestMetrics(request).stat()
}

func (m *urlMetricsImpl) OnHttpStatus(request string, status int) {
	m.requestMetrics(request).onHttpStatus(status)
}

func (m *urlMetricsImpl) OnRequestSize(size uint64) {
	m.requestSize.RecordValue(float64(size))
}

func (m *urlMetricsImpl) OnCacheHit() {
	m.cacheHits.Inc()
}
