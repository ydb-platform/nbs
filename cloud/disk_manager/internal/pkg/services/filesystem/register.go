package filesystem

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *config.FilesystemConfig,
	taskScheduler tasks.Scheduler,
	registry *tasks.Registry,
	storage resources.Storage,
	factory nfs.Factory,
) error {

	deletedFilesystemExpirationTimeout, err := time.ParseDuration(config.GetDeletedFilesystemExpirationTimeout())
	if err != nil {
		return err
	}

	clearDeletedFilesystemsTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedFilesystemsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("filesystem.CreateFilesystem", func() tasks.Task {
		return &createFilesystemTask{
			storage:   storage,
			factory:   factory,
			scheduler: taskScheduler,
		}
	})
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("filesystem.CreateExternalFilesystem", func() tasks.Task {
		return &createExternalFilesystemTask{
			storage: storage,
			factory: factory,
		}
	})
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("filesystem.DeleteFilesystem", func() tasks.Task {
		return &deleteFilesystemTask{
			storage:   storage,
			factory:   factory,
			scheduler: taskScheduler,
		}
	})
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("filesystem.DeleteExternalFilesystem", func() tasks.Task {
		return &deleteExternalFilesystemTask{
			storage: storage,
			factory: factory,
		}
	})
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("filesystem.ResizeFilesystem", func() tasks.Task {
		return &resizeFilesystemTask{
			factory: factory,
		}
	})
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("filesystems.ClearDeletedFilesystems", func() tasks.Task {
		return &clearDeletedFilesystemsTask{
			storage:           storage,
			expirationTimeout: deletedFilesystemExpirationTimeout,
			limit:             int(config.GetClearDeletedFilesystemsLimit()),
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"filesystems.ClearDeletedFilesystems",
		tasks.TaskSchedule{
			ScheduleInterval: clearDeletedFilesystemsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
