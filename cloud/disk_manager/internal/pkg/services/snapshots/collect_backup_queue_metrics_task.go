package snapshots

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

// Reports the backup queue of snapshots. One instance runs at a time and
// zeroes its gauges when it stops, so the sum over hosts is the queue.
type collectBackupQueueMetricsTask struct {
	storage            resources.Storage
	registry           metrics.Registry
	collectionInterval time.Duration
}

func (t *collectBackupQueueMetricsTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *collectBackupQueueMetricsTask) Load(_, _ []byte) error {
	return nil
}

func (t *collectBackupQueueMetricsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	defer t.clearMetrics()

	ticker := time.NewTicker(t.collectionInterval)
	defer ticker.Stop()

	for {
		err := t.collect(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (t *collectBackupQueueMetricsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *collectBackupQueueMetricsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *collectBackupQueueMetricsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *collectBackupQueueMetricsTask) collect(ctx context.Context) error {
	stats, err := t.storage.GetSnapshotBackupQueueStats(ctx)
	if err != nil {
		return err
	}

	t.registry.Gauge("backup/snapshotsQueued").Set(float64(stats.Queued))
	t.registry.Gauge("backup/snapshotsInflight").Set(float64(stats.Scheduled))
	return nil
}

func (t *collectBackupQueueMetricsTask) clearMetrics() {
	t.registry.Gauge("backup/snapshotsQueued").Set(0)
	t.registry.Gauge("backup/snapshotsInflight").Set(0)
}
