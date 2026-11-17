package snapshots

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	metrics_mocks "github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCollectBackupQueueMetrics(t *testing.T) {
	ctx := context.Background()

	storage := resources_mocks.NewStorageMock()
	storage.On("GetSnapshotBackupQueueStats", mock.Anything).Return(
		resources.SnapshotBackupQueueStats{Queued: 3, Scheduled: 2},
		nil,
	)

	registry := metrics_mocks.NewRegistryMock()
	queued := registry.GetGauge("backup/snapshotsQueued", nil)
	inflight := registry.GetGauge("backup/snapshotsInflight", nil)
	queued.On("Set", float64(3)).Once()
	inflight.On("Set", float64(2)).Once()

	task := &collectBackupQueueMetricsTask{
		storage:            storage,
		registry:           registry,
		collectionInterval: time.Minute,
	}

	err := task.collect(ctx)
	require.NoError(t, err)
	registry.AssertAllExpectations(t)

	// A stopped collector zeroes its gauges: another host reports now.
	queued.On("Set", float64(0)).Once()
	inflight.On("Set", float64(0)).Once()
	task.clearMetrics()
	registry.AssertAllExpectations(t)
}
