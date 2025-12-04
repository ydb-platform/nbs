package cells

import (
	"context"
	"time"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
)

////////////////////////////////////////////////////////////////////////////////

type collectCellsMetricsTask struct {
	config                    *cells_config.CellsConfig
	registry                  metrics.Registry
	storage                   storage.Storage
	metricsCollectionInterval time.Duration
}

func (t *collectCellsMetricsTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *collectCellsMetricsTask) Load(request []byte, state []byte) error {
	return nil
}

func (t *collectCellsMetricsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	collectedDiskKinds := []types.DiskKind{
		types.DiskKind_DISK_KIND_SSD,
		types.DiskKind_DISK_KIND_HDD,
		types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		types.DiskKind_DISK_KIND_SSD_MIRROR2,
		types.DiskKind_DISK_KIND_SSD_MIRROR3,
		types.DiskKind_DISK_KIND_HDD_NONREPLICATED,
	}

	ticker := time.NewTicker(t.metricsCollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for zoneID, _ := range t.config.Cells {
				err := t.collectZoneMetrics(ctx, zoneID, collectedDiskKinds)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (t *collectCellsMetricsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *collectCellsMetricsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *collectCellsMetricsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *collectCellsMetricsTask) collectZoneMetrics(
	ctx context.Context,
	zoneID string,
	collectedDiskKinds []types.DiskKind,
) error {

	for _, kind := range collectedDiskKinds {
		capacities, err := t.storage.GetRecentClusterCapacities(
			ctx,
			zoneID,
			kind,
		)
		if err != nil {
			return err
		}

		for _, capacity := range capacities {
			subRegistry := t.registry.WithTags(map[string]string{
				"kind": common.DiskKindToString(kind),
				"zone": zoneID,
				"cell": capacity.CellID,
			})

			subRegistry.Gauge("free_bytes").Set(float64(capacity.FreeBytes))
			subRegistry.Gauge("total_bytes").Set(float64(capacity.TotalBytes))

			timeSinceLastUpdate := time.Since(capacity.CreatedAt)
			subRegistry.Gauge("time_since_last_update").Set(
				timeSinceLastUpdate.Seconds(),
			)
		}
	}

	return nil
}
