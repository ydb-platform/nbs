package dataplane

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type collectSnapshotMetricsTask struct {
	registry                  metrics.Registry
	storage                   storage.Storage
	metricsCollectionInterval time.Duration
}

func (c collectSnapshotMetricsTask) Save() ([]byte, error) {
	return nil, nil
}

func (c collectSnapshotMetricsTask) Load(request []byte, state []byte) error {
	return nil
}

func (c collectSnapshotMetricsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	defer c.clearMetrics()

	ticker := time.NewTicker(c.metricsCollectionInterval)
	defer ticker.Stop()

	for range ticker.C {
		deletingSnapshotCount, err := c.storage.GetDeletingSnapshotCount(ctx)
		if err != nil {
			return err
		}
		c.registry.Gauge("snapshots/deletingCount").Set(float64(deletingSnapshotCount))

		snapshotCount, err := c.storage.GetSnapshotCount(ctx)
		if err != nil {
			return err
		}
		c.registry.Gauge("snapshots/count").Set(float64(snapshotCount))

		totalSnapshotSize, err := c.storage.GetTotalSnapshotSize(ctx)
		if err != nil {
			return err
		}
		c.registry.Gauge("snapshots/totalSize").Set(float64(totalSnapshotSize))

		totalSnapshotStorageSize, err := c.storage.GetTotalSnapshotStorageSize(ctx)
		if err != nil {
			return err
		}
		c.registry.Gauge("snapshots/totalStorageSize").Set(float64(totalSnapshotStorageSize))
	}
	return nil
}

func (c collectSnapshotMetricsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (c collectSnapshotMetricsTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (c collectSnapshotMetricsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (c collectSnapshotMetricsTask) clearMetrics() {
	// We'd like to delete it from registry completely, but there's no such option.
	c.registry.Gauge("snapshots/deletingCount").Set(0)
}
