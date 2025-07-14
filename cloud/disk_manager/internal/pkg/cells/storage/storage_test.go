package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

func makeDefaultConfig() *cells_config.CellsConfig {
	return &cells_config.CellsConfig{}
}

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
) Storage {

	folder := fmt.Sprintf("storage_ydb_test/%v", t.Name())
	config := makeDefaultConfig()

	config.StorageFolder = &folder

	err := CreateYDBTables(ctx, config, db, false /* dropUnusedColumns */)
	require.NoError(t, err)

	storage := NewStorage(config, db)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

func TestGetRecentClusterCapacitiesReturnsOnlyRecentValue(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	zeroTime := time.Time{}

	capacity1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	cellCapacity1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	cellCapacitySsd1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_SSD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	capacity2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	cellCapacity2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	cellCapacitySsd2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_SSD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{capacity1, cellCapacity1, cellCapacitySsd1},
		zeroTime, // deleteBefore
	)
	require.NoError(t, err)

	capacities, err := storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{
		capacity1,
		cellCapacity1,
	}, capacities)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{cellCapacitySsd1}, capacities)

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{capacity2},
		zeroTime, // deleteBefore
	)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{
		capacity2,
		cellCapacity1,
	}, capacities)

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{cellCapacity2, cellCapacitySsd2},
		zeroTime, // deleteBefore
	)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{
		capacity2,
		cellCapacity2,
	}, capacities)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{cellCapacitySsd2}, capacities)
}

func TestUpdateClusterCapacitiesDeletesRecordsBeforeTimestamp(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	deleteBefore := time.Now()
	capacity1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}
	capacity2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity1}, deleteBefore)
	require.NoError(t, err)

	capacities, err := storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, capacities, []ClusterCapacity{capacity1})

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity2}, deleteBefore)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, capacities, []ClusterCapacity{capacity1, capacity2})

	deleteBefore = time.Now()
	capacity3 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity3}, deleteBefore)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, capacities, []ClusterCapacity{capacity3})
}
