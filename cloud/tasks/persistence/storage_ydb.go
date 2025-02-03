package persistence

import (
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db         *YDBClient
	tablesPath string
}

func NewStorage(
	config *tasks_config.TasksConfig,
	db *YDBClient,
) *storageYDB {

	return &storageYDB{
		db:         db,
		tablesPath: db.AbsolutePath(config.GetStorageFolder()),
	}
}
