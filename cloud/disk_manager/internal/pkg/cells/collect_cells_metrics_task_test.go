package cells

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	cells_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	metrics_mocks "github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

func TestCollectCellsMetricsTaskCollectZoneMetrics(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	registry := metrics_mocks.NewRegistryMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a-cell1", "zone-a"}},
		},
	}

	task := collectCellsMetricsTask{
		storage:  storage,
		registry: registry,
		config:   config,
	}

	collectedDiskKinds := []types.DiskKind{
		types.DiskKind_DISK_KIND_SSD,
	}

	freeBytes := uint64(1024)
	totalBytes := uint64(2048)

	createdAt := time.Now().Add(-time.Hour)

	storage.On(
		"GetRecentClusterCapacities",
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_SSD,
	).Return(
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     "zone-a",
				CellID:     "zone-a",
				FreeBytes:  freeBytes,
				TotalBytes: totalBytes,
				CreatedAt:  createdAt,
			},
			{
				ZoneID:     "zone-a",
				CellID:     "zone-a-cell1",
				FreeBytes:  freeBytes * 2,
				TotalBytes: totalBytes * 2,
				CreatedAt:  createdAt,
			},
		},
		nil,
	)

	firstCellTags := map[string]string{
		"kind": common.DiskKindToString(types.DiskKind_DISK_KIND_SSD),
		"zone": "zone-a",
		"cell": "zone-a",
	}

	secondCellTags := map[string]string{
		"kind": common.DiskKindToString(types.DiskKind_DISK_KIND_SSD),
		"zone": "zone-a",
		"cell": "zone-a-cell1",
	}

	moreThanAnHour := mock.MatchedBy(func(value float64) bool {
		return value >= time.Hour.Seconds()
	})

	registry.GetGauge(
		"free_bytes",
		firstCellTags,
	).On("Set", float64(freeBytes))
	registry.GetGauge(
		"total_bytes",
		firstCellTags,
	).On("Set", float64(totalBytes))
	registry.GetGauge(
		"time_since_last_update",
		firstCellTags,
	).On("Set", moreThanAnHour)

	registry.GetGauge(
		"free_bytes",
		secondCellTags,
	).On("Set", float64(freeBytes*2))
	registry.GetGauge(
		"total_bytes",
		secondCellTags,
	).On("Set", float64(totalBytes*2))
	registry.GetGauge(
		"time_since_last_update",
		secondCellTags,
	).On("Set", moreThanAnHour)

	err := task.collectZoneMetrics(ctx, "zone-a", collectedDiskKinds)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(
		t,
		storage,
	)

	registry.AssertAllExpectations(t)
}
