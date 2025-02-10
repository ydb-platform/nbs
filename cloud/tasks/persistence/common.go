package persistence

import (
	"context"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
)

////////////////////////////////////////////////////////////////////////////////

func CreateYDBTables(
	ctx context.Context,
	config *tasks_config.TasksConfig,
	db *YDBClient,
	dropUnusedColumns bool,
) error {

	err := db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"availability_monitoring",
		availabilityMonitoringTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	return nil
}
