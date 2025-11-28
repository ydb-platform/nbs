package storage

import (
	"context"
	"fmt"
	"os"
	"sort"
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

// Uses WithinDuration to compare CreatedAt field correctly, since == may fail
// due to different internal representations.
func requireClusterCapacitiesAreEqual(
	t *testing.T,
	expected ClusterCapacity,
	actual ClusterCapacity,
) {

	require.Equal(t, expected.CellID, actual.CellID)
	require.Equal(t, expected.ZoneID, actual.ZoneID)
	require.Equal(t, expected.Kind, actual.Kind)
	require.Equal(t, expected.FreeBytes, actual.FreeBytes)
	require.Equal(t, expected.TotalBytes, actual.TotalBytes)
	require.WithinDuration(t, expected.CreatedAt, actual.CreatedAt, time.Microsecond)
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
	firstTime := time.Now()
	secondTime := firstTime.Add(time.Hour)

	capacity1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  firstTime,
	}

	cellCapacity1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  firstTime,
	}

	cellCapacitySsd1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_SSD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  firstTime,
	}

	capacity2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  secondTime,
	}

	cellCapacity2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  secondTime,
	}

	cellCapacitySsd2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_SSD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  secondTime,
	}

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{capacity1, cellCapacity1, cellCapacitySsd1},
		zeroTime, // deleteOlderThan
	)
	require.NoError(t, err)

	capacities, err := storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	sort.Slice(capacities, func(i, j int) bool {
		return capacities[i].CellID < capacities[j].CellID
	})
	require.Len(t, capacities, 2)
	requireClusterCapacitiesAreEqual(t, capacities[0], capacity1)
	requireClusterCapacitiesAreEqual(t, capacities[1], cellCapacity1)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Len(t, capacities, 1)
	requireClusterCapacitiesAreEqual(t, cellCapacitySsd1, capacities[0])

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{capacity2},
		zeroTime, // deleteOlderThan
	)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	sort.Slice(capacities, func(i, j int) bool {
		return capacities[i].CellID < capacities[j].CellID
	})
	require.Len(t, capacities, 2)
	requireClusterCapacitiesAreEqual(t, capacities[0], capacity2)
	requireClusterCapacitiesAreEqual(t, capacities[1], cellCapacity1)

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{cellCapacity2, cellCapacitySsd2},
		zeroTime, // deleteOlderThan
	)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	sort.Slice(capacities, func(i, j int) bool {
		return capacities[i].CellID < capacities[j].CellID
	})
	require.Len(t, capacities, 2)
	requireClusterCapacitiesAreEqual(t, capacities[0], capacity2)
	requireClusterCapacitiesAreEqual(t, capacities[1], cellCapacity2)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Len(t, capacities, 1)
	requireClusterCapacitiesAreEqual(t, cellCapacitySsd2, capacities[0])
}

func TestUpdateClusterCapacitiesDeletesRecordsBeforeTimestamp(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	deleteOlderThan := time.Now()
	createdAt := deleteOlderThan.Add(time.Hour)
	capacity1 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  createdAt,
	}
	capacity2 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  createdAt,
	}

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity1}, deleteOlderThan)
	require.NoError(t, err)

	capacities, err := storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.Len(t, capacities, 1)
	requireClusterCapacitiesAreEqual(t, capacity1, capacities[0])

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity2}, deleteOlderThan)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	sort.Slice(capacities, func(i, j int) bool {
		return capacities[i].CellID < capacities[j].CellID
	})
	require.Len(t, capacities, 2)
	requireClusterCapacitiesAreEqual(t, capacities[0], capacity1)
	requireClusterCapacitiesAreEqual(t, capacities[1], capacity2)

	deleteOlderThan = createdAt.Add(time.Hour)
	createdAt = deleteOlderThan.Add(time.Hour)
	capacity3 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  createdAt,
	}

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity3}, deleteOlderThan)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.Len(t, capacities, 1)
	requireClusterCapacitiesAreEqual(t, capacity3, capacities[0])
}
