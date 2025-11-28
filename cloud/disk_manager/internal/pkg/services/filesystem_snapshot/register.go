package filesystem_snapshot

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	filesystem_snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *filesystem_snapshot_config.FilesystemSnapshotsConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	cellSelector cells.CellSelector,
	storage resources.Storage,
) error {

	deletedExpirationTimeout, err := time.ParseDuration(
		config.GetDeletedExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearDeletedTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"filesystem_snapshot.CreateFilesystemSnapshot",
		func() tasks.Task {
			return &createFilesystemSnapshotTask{
				scheduler:    taskScheduler,
				cellSelector: cellSelector,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"filesystem_snapshot.DeleteFilesystemSnapshot",
		func() tasks.Task {
			return &deleteFilesystemSnapshotTask{
				scheduler: taskScheduler,
			}
		})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"filesystem_snapshot.ClearDeletedFilesystemSnapshots", func() tasks.Task {
			return &clearDeletedFilesystemSnapshotsTask{
				storage:           storage,
				expirationTimeout: deletedExpirationTimeout,
				limit:             int(config.GetClearDeletedLimit()),
			}
		})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"snapfilesystem_snapshotshots.ClearDeletedFilesystemSnapshots",
		tasks.TaskSchedule{
			ScheduleInterval: clearDeletedTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
