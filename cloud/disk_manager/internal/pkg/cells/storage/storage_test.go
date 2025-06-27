package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

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

	capacity3 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	capacity4 := ClusterCapacity{
		ZoneID:     "zone-a",
		CellID:     "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	err = storage.AddClusterCapacities(ctx, []ClusterCapacity{
		capacity1,
		capacity2,
	})
	require.NoError(t, err)

	capacities, err := storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity1, capacity2}, capacities)

	err = storage.AddClusterCapacities(ctx, []ClusterCapacity{capacity3})
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity2, capacity3}, capacities)

	err = storage.AddClusterCapacities(ctx, []ClusterCapacity{capacity4})
	require.NoError(t, err)

	capacities, err = storage.GetRecentClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity3, capacity4}, capacities)
}
