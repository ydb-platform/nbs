package filesystem_backups

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	filesystem_backups_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_backups/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *filesystem_backups_config.FilesystemBackupsConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
) error {

	deletedFilesystemBackupExpirationTimeout, err := time.ParseDuration(
		config.GetDeletedFilesystemBackupExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearDeletedFilesystemBackupsTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedFilesystemBackupsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("filesystembackups.CreateFilesystemBackup", func() tasks.Task {
		return &createFilesystemBackupFromDiskTask{
			scheduler:  taskScheduler,
			storage:    storage,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("filesystembackups.DeleteFilesystemBackup", func() tasks.Task {
		return &deleteFilesystemBackupTask{
			scheduler:  taskScheduler,
			storage:    storage,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("filesystembackups.ClearDeletedFilesystemBackups", func() tasks.Task {
		return &clearDeletedFilesystemBackupsTask{
			storage:           storage,
			expirationTimeout: deletedFilesystemBackupExpirationTimeout,
			limit:             int(config.GetClearDeletedFilesystemBackupsLimit()),
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"filesystembackups.ClearDeletedFilesystemBackups",
		tasks.TaskSchedule{
			ScheduleInterval: clearDeletedFilesystemBackupsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
