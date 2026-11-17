package exporter

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

const (
	s3StageRead       = "read"
	s3StageAssemble   = "assemble"
	s3StageCompress   = "compress"
	s3StageUpload     = "upload"
	s3StageBufferWait = "buffer_wait"
	s3StageReadWait   = "read_slot_wait"
	s3StageUploadWait = "upload_slot_wait"
	s3StageCheckpoint = "checkpoint"
)

type s3StageMetrics struct {
	active metrics.IntGauge
	time   metrics.Timer
	errors metrics.Counter
}

type s3ExportMetrics struct {
	readBytes            metrics.Counter
	uploadedBytes        metrics.Counter
	uploadedLogicalBytes metrics.Counter
	uploadedParts        metrics.Counter
	reusedBytes          metrics.Counter
	reusedParts          metrics.Counter
	completed            metrics.Counter
	operationRetries     metrics.Counter
	stages               map[string]s3StageMetrics
}

func newS3ExportMetrics(registry metrics.Registry) *s3ExportMetrics {
	if registry == nil {
		registry = metrics.NewEmptyRegistry()
	}

	m := &s3ExportMetrics{
		readBytes:            registry.Counter("export/readBytes"),
		uploadedBytes:        registry.Counter("export/uploadedBytes"),
		uploadedLogicalBytes: registry.Counter("export/uploadedLogicalBytes"),
		uploadedParts:        registry.Counter("export/uploadedParts"),
		reusedBytes:          registry.Counter("export/reusedBytes"),
		reusedParts:          registry.Counter("export/reusedParts"),
		completed:            registry.Counter("export/completed"),
		operationRetries:     registry.Counter("export/operationRetries"),
		stages:               make(map[string]s3StageMetrics),
	}
	buckets := metrics.NewDurationBuckets(
		time.Millisecond, 10*time.Millisecond, 100*time.Millisecond,
		time.Second, 5*time.Second, 30*time.Second,
		time.Minute, 5*time.Minute, 15*time.Minute, time.Hour,
	)
	for _, stage := range []string{
		s3StageRead, s3StageAssemble, s3StageCompress, s3StageUpload,
		s3StageBufferWait, s3StageReadWait, s3StageUploadWait, s3StageCheckpoint,
	} {
		r := registry.WithTags(map[string]string{"stage": stage})
		m.stages[stage] = s3StageMetrics{
			active: r.IntGauge("export/stageActive"),
			time:   r.DurationHistogram("export/stageTime", buckets),
			errors: r.Counter("export/stageErrors"),
		}
	}

	return m
}

type s3ExportMetricsContextKey struct{}

func recordS3OperationRetry(ctx context.Context) {
	if m, ok := ctx.Value(s3ExportMetricsContextKey{}).(*s3ExportMetrics); ok {
		m.operationRetries.Inc()
	}
}

// Additive gauges aggregate simultaneous exports without overwriting others.
func (m *s3ExportMetrics) addActive(stage string, delta int64) {
	if m != nil {
		m.stages[stage].active.Add(delta)
	}
}

func (m *s3ExportMetrics) recordStage(
	ctx context.Context,
	stage string,
	started time.Time,
	err error,
) {

	if m != nil {
		stats := m.stages[stage]
		stats.time.RecordDuration(time.Since(started))
		// Sibling failures cancel other workers; these are not extra failures.
		if err != nil && ctx.Err() == nil {
			stats.errors.Inc()
		}
	}
}

func (m *s3ExportMetrics) addRead(size uint64) {
	if m != nil {
		m.readBytes.Add(int64(size))
	}
}

func (m *s3ExportMetrics) addUploaded(logical, stored uint64) {
	if m != nil {
		m.uploadedLogicalBytes.Add(int64(logical))
		m.uploadedBytes.Add(int64(stored))
		m.uploadedParts.Inc()
	}
}

func (m *s3ExportMetrics) addReused(size uint64) {
	if m != nil {
		m.reusedBytes.Add(int64(size))
		m.reusedParts.Inc()
	}
}

func (m *s3ExportMetrics) saveCheckpoint(
	ctx context.Context,
	save func(context.Context) error,
) error {

	started := time.Now()
	m.addActive(s3StageCheckpoint, 1)
	err := save(ctx)
	m.addActive(s3StageCheckpoint, -1)
	m.recordStage(ctx, s3StageCheckpoint, started, err)
	return err
}
