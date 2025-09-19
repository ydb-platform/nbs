package schema

import (
	"context"

	cells_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	pools_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func initControlplane(
	ctx context.Context,
	config *server_config.ServerConfig,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	err := tasks_storage.CreateYDBTables(
		ctx,
		config.GetTasksConfig(),
		db,
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	err = pools_storage.CreateYDBTables(
		ctx,
		config.GetPoolsConfig(),
		db,
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	if config.GetCellsConfig() != nil {
		cells_storage.CreateYDBTables(
			ctx,
			config.GetCellsConfig(),
			db,
			dropUnusedColumns,
		)
	}

	filesystemStorageFolder := ""
	if config.GetFilesystemConfig() != nil {
		filesystemStorageFolder = config.GetFilesystemConfig().GetStorageFolder()
	}

	return resources.CreateYDBTables(
		ctx,
		config.GetDisksConfig().GetStorageFolder(),
		config.GetImagesConfig().GetStorageFolder(),
		config.GetSnapshotsConfig().GetStorageFolder(),
		filesystemStorageFolder,
		config.GetPlacementGroupConfig().GetStorageFolder(),
		db,
		dropUnusedColumns,
	)
}
