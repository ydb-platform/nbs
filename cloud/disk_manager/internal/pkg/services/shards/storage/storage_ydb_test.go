package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	shards_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/shards/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

func makeDefaultConfig() *shards_config.ShardsConfig {

	return &shards_config.ShardsConfig{}
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

func newStorageWithConfig(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *shards_config.ShardsConfig,
	registry metrics.Registry,
) Storage {

	folder := fmt.Sprintf("storage_ydb_test/%v", t.Name())
	config.StorageFolder = &folder

	err := CreateYDBTables(ctx, config, db, false /* dropUnusedColumns */)
	require.NoError(t, err)

	storage := NewStorage(config, db)

	return storage
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
) Storage {

	return newStorageWithConfig(t, ctx, db, makeDefaultConfig(), metrics.NewEmptyRegistry())
}

////////////////////////////////////////////////////////////////////////////////

func TestGetClusterCapacitiesReturnsCorrectValue(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	capacity1 := ClusterCapacity{
		ZoneId:     "zone-a",
		ShardId:    "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	capacity2 := ClusterCapacity{
		ZoneId:     "zone-b",
		ShardId:    "zone-b",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	capacity3 := ClusterCapacity{
		ZoneId:     "zone-a",
		ShardId:    "zone-a",
		Kind:       types.DiskKind_DISK_KIND_SSD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	err = storage.AddClusterCapacities(ctx, []ClusterCapacity{
		capacity1,
		capacity2,
		capacity3,
	})
	require.NoError(t, err)

	capacities, err := storage.GetClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity1}, capacities)

	capacities, err = storage.GetClusterCapacities(
		ctx,
		"zone-b",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity2}, capacities)

	capacities, err = storage.GetClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity3}, capacities)

	capacities, err = storage.GetClusterCapacities(
		ctx,
		"incorrect-zone",
		types.DiskKind_DISK_KIND_SSD,
	)
	require.NoError(t, err)
	require.Empty(t, capacities)
}

func TestGetClusterCapacitiesReturnsOnlyRecentValue(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)

	capacity1 := ClusterCapacity{
		ZoneId:     "zone-a",
		ShardId:    "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	capacity2 := ClusterCapacity{
		ZoneId:     "zone-a",
		ShardId:    "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 1024,
		FreeBytes:  1024,
	}

	capacity3 := ClusterCapacity{
		ZoneId:     "zone-a",
		ShardId:    "zone-a",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	err = storage.AddClusterCapacities(ctx, []ClusterCapacity{
		capacity1,
		capacity2,
	})
	require.NoError(t, err)

	err = storage.AddClusterCapacities(ctx, []ClusterCapacity{capacity3})
	require.NoError(t, err)

	capacities, err := storage.GetClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity2, capacity3}, capacities)

	capacity4 := ClusterCapacity{
		ZoneId:     "zone-a",
		ShardId:    "zone-a-shard1",
		Kind:       types.DiskKind_DISK_KIND_HDD,
		TotalBytes: 2048,
		FreeBytes:  2048,
	}

	err = storage.AddClusterCapacities(ctx, []ClusterCapacity{capacity4})
	require.NoError(t, err)

	capacities, err = storage.GetClusterCapacities(
		ctx,
		"zone-a",
		types.DiskKind_DISK_KIND_HDD,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClusterCapacity{capacity3, capacity4}, capacities)
}
