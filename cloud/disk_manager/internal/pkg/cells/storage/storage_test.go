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

const (
	shardedZoneID = "zone-a"
	cellID1       = "zone-a"
	cellID2       = "zone-a-cell1"
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

	require.WithinDuration(t, expected.CreatedAt, actual.CreatedAt, time.Microsecond)
	actual.CreatedAt = expected.CreatedAt

	require.Equal(t, expected, actual)
}

func requireClusterCapacitiesSlicesAreEqual(
	t *testing.T,
	expected []ClusterCapacity,
	actual []ClusterCapacity,
) {

	require.Len(t, actual, len(expected))

	// GetRecentClusterCapacities returns results in non-deterministic order.
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].CellID < expected[j].CellID
	})
	sort.Slice(actual, func(i, j int) bool {
		return actual[i].CellID < actual[j].CellID
	})

	for i := 0; i < len(expected); i++ {
		requireClusterCapacitiesAreEqual(t, expected[i], actual[i])
	}
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

	capacityOfFirstCell1 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID1,
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  firstTime,
	}

	capacityOfSecondCell1 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID2,
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  firstTime,
	}

	capacityOfSecondCellSsd1 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID2,
		Kind:       types.DiskKind_DISK_KIND_SSD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  firstTime,
	}

	capacityOfFirstCell2 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID1,
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  secondTime,
	}

	capacityOfSecondCell2 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID2,
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  secondTime,
	}

	capacityOfSecondCellSsd2 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID2,
		Kind:       types.DiskKind_DISK_KIND_SSD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  secondTime,
	}

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{capacityOfFirstCell1, capacityOfSecondCell1, capacityOfSecondCellSsd1},
		zeroTime, // deleteOlderThan
	)
	require.NoError(t, err)

	capacities, err := storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacityOfFirstCell1, capacityOfSecondCell1},
		capacities,
	)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacityOfSecondCellSsd1},
		capacities,
	)

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{capacityOfFirstCell2},
		zeroTime, // deleteOlderThan
	)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacityOfFirstCell2, capacityOfSecondCell1},
		capacities,
	)

	err = storage.UpdateClusterCapacities(
		ctx,
		[]ClusterCapacity{capacityOfSecondCell2, capacityOfSecondCellSsd2},
		zeroTime, // deleteOlderThan
	)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacityOfFirstCell2, capacityOfSecondCell2},
		capacities,
	)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacityOfSecondCellSsd2},
		capacities,
	)
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
		ZoneID:     shardedZoneID,
		CellID:     cellID1,
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  createdAt,
	}
	capacity2 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID2,
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
		CreatedAt:  createdAt,
	}

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity1}, deleteOlderThan)
	require.NoError(t, err)

	capacities, err := storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacity1},
		capacities,
	)

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity2}, deleteOlderThan)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacity1, capacity2},
		capacities,
	)

	deleteOlderThan = createdAt.Add(time.Hour)
	createdAt = deleteOlderThan.Add(time.Hour)
	capacity3 := ClusterCapacity{
		ZoneID:     shardedZoneID,
		CellID:     cellID1,
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
		CreatedAt:  createdAt,
	}

	err = storage.UpdateClusterCapacities(ctx, []ClusterCapacity{capacity3}, deleteOlderThan)
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		shardedZoneID,
		types.DiskKind_DISK_KIND_HDD,
	)
	requireClusterCapacitiesSlicesAreEqual(
		t,
		[]ClusterCapacity{capacity3},
		capacities,
	)
}
