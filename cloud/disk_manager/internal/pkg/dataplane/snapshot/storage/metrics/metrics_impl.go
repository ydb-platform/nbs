package metrics

import (
	"sync"
	"time"

	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type chunkCompressionMetrics struct {
	ratio                 common_metrics.Histogram
	compressionDuration   common_metrics.Timer
	decompressionDuration common_metrics.Timer
}

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

func chunkCompressionDurationBuckets() common_metrics.DurationBuckets {
	return common_metrics.NewDurationBuckets(
		1*time.Millisecond, 2*time.Millisecond, 5*time.Millisecond,
		10*time.Millisecond, 15*time.Millisecond, 20*time.Millisecond,
		30*time.Millisecond, 50*time.Millisecond, 100*time.Millisecond,
	)
}

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
	registry                     common_metrics.Registry
	operationMetrics             map[string]*operationStats
	operationMetricsMutex        sync.Mutex
	chunkCompressionMetrics      map[string]chunkCompressionMetrics
	chunkCompressionMetricsMutex sync.Mutex
	storageType                  string
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

func (m *storageMetricsImpl) getOrNewCompressionMetrics(
	compression string,
) chunkCompressionMetrics {

	m.chunkCompressionMetricsMutex.Lock()
	defer m.chunkCompressionMetricsMutex.Unlock()

	if metrics, ok := m.chunkCompressionMetrics[compression]; ok {
		return metrics
	}

	subRegistry := m.registry.WithTags(map[string]string{
		"compression": compression,
	})
	metrics := chunkCompressionMetrics{
		ratio: subRegistry.Histogram(
			"chunkCompressionRatio",
			common_metrics.NewLinearBuckets(0, 1, 20),
		),
		compressionDuration: subRegistry.DurationHistogram(
			"chunkCompressionTime",
			chunkCompressionDurationBuckets(),
		),
		decompressionDuration: subRegistry.DurationHistogram(
			"chunkDecompressionTime",
			chunkCompressionDurationBuckets(),
		),
	}
	m.chunkCompressionMetrics[compression] = metrics

	return metrics
}

func (m *storageMetricsImpl) StatOperation(name string) func(err *error) {
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

func (m *storageMetricsImpl) OnChunkCompressed(
	origSize int,
	compressedSize int,
	duration time.Duration,
	compression string,
) {

	metrics := m.getOrNewCompressionMetrics(compression)
	if compressedSize != 0 {
		metrics.ratio.RecordValue(float64(compressedSize) / float64(origSize))
	}
	if duration != 0 {
		metrics.compressionDuration.RecordDuration(duration)
	}
}

func (m *storageMetricsImpl) OnChunkDecompressed(
	duration time.Duration,
	compression string,
) {

	metrics := m.getOrNewCompressionMetrics(compression)
	if duration != 0 {
		metrics.decompressionDuration.RecordDuration(duration)
	}
}

////////////////////////////////////////////////////////////////////////////////

const (
	OperationWriteChunkBlob = "writeChunkBlob"
	OperationReadChunkBlob  = "readChunkBlob"
	OperationRefChunkBlob   = "refChunkBlob"
	OperationUnrefChunkBlob = "unrefChunkBlob"
)
