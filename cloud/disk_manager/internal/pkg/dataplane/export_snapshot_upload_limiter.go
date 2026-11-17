package dataplane

import (
	"context"
	"sync"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

const snapshotExportUploadRecoverySuccesses = 5

// Existing PUTs finish normally when the limit decreases. New calls wait until
// active requests fall below the current limit, including retry attempts.
type snapshotExportUploadLimiter struct {
	mu          sync.Mutex
	maximum     int
	limit       int
	active      int
	successes   int
	generation  uint64
	changed     chan struct{}
	limitGauge  metrics.IntGauge
	activeGauge metrics.IntGauge
}

func newSnapshotExportUploadLimiter(
	maximum int,
	state *protos.ExportSnapshotTaskState,
	registry metrics.Registry,
) *snapshotExportUploadLimiter {

	limit := maximum
	if state.UploadWorkerLimit > 0 && int(state.UploadWorkerLimit) < limit {
		limit = int(state.UploadWorkerLimit)
	}

	limiter := &snapshotExportUploadLimiter{
		maximum:     maximum,
		limit:       limit,
		changed:     make(chan struct{}),
		limitGauge:  registry.IntGauge("export/uploadWorkerLimit"),
		activeGauge: registry.IntGauge("export/activeDestinationPuts"),
	}
	limiter.limitGauge.Add(int64(limit))
	return limiter
}

func (l *snapshotExportUploadLimiter) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limitGauge.Add(-int64(l.limit))
}

func (l *snapshotExportUploadLimiter) checkpoint(
	state *protos.ExportSnapshotTaskState,
) {

	l.mu.Lock()
	defer l.mu.Unlock()
	state.UploadWorkerLimit = uint32(l.limit)
}

func (l *snapshotExportUploadLimiter) acquire(
	ctx context.Context,
) (uint64, error) {

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		l.mu.Lock()
		if l.active < l.limit {
			l.active++
			l.activeGauge.Add(1)
			generation := l.generation
			l.mu.Unlock()
			return generation, nil
		}

		changed := l.changed
		l.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-changed:
		}
	}
}

func (l *snapshotExportUploadLimiter) finish(
	ctx context.Context,
	generation uint64,
	err error,
) {

	l.mu.Lock()
	defer l.mu.Unlock()
	l.active--
	l.activeGauge.Add(-1)
	previous := l.limit
	if err != nil && ctx.Err() == nil &&
		isSnapshotExportTransientError(err) {

		l.limit = max(1, l.limit/2)
		l.generation++
		l.successes = 0
	} else if err == nil && generation == l.generation {
		l.successes++
		if l.successes >= snapshotExportUploadRecoverySuccesses {
			l.limit = min(l.maximum, l.limit+1)
			l.successes = 0
		}
	}

	if l.limit != previous {
		l.limitGauge.Add(int64(l.limit - previous))
		logging.Info(
			ctx, "target S3 upload concurrency changed: %v -> %v",
			previous, l.limit,
		)
	}

	close(l.changed)
	l.changed = make(chan struct{})
}
