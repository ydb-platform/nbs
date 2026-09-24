package images

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	images_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *images_config.ImagesConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
	poolService pools.Service,
	cellSelector cells.CellSelector,
	backupS3 *backup.S3,
) error {

	deletedImageExpirationTimeout, err := time.ParseDuration(
		config.GetDeletedImageExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearDeletedImagesTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedImagesTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	scheduleBackupImageTasksScheduleInterval, err := time.ParseDuration(
		config.GetScheduleBackupImageTasksScheduleInterval(),
	)
	if err != nil {
		return err
	}

	deleteBackupMetaTaskScheduleInterval, err := time.ParseDuration(
		config.GetDeleteBackupMetaTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromURL", func() tasks.Task {
		return &createImageFromURLTask{
			config:      config,
			scheduler:   taskScheduler,
			storage:     storage,
			poolService: poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromImage", func() tasks.Task {
		return &createImageFromImageTask{
			config:      config,
			scheduler:   taskScheduler,
			storage:     storage,
			poolService: poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromSnapshot", func() tasks.Task {
		return &createImageFromSnapshotTask{
			config:      config,
			scheduler:   taskScheduler,
			storage:     storage,
			poolService: poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromDisk", func() tasks.Task {
		return &createImageFromDiskTask{
			config:       config,
			scheduler:    taskScheduler,
			storage:      storage,
			nbsFactory:   nbsFactory,
			poolService:  poolService,
			cellSelector: cellSelector,
		}
	})
	if err != nil {
		return err
	}

	if backupS3 != nil {
		err = taskRegistry.RegisterForExecution("images.BackupImage", func() tasks.Task {
			return &backupImageTask{
				scheduler: taskScheduler,
				storage:   storage,
				backupS3:  backupS3,
			}
		})
		if err != nil {
			return err
		}

		err = taskRegistry.RegisterForExecution("images.ScheduleBackupImageTasks", func() tasks.Task {
			return &scheduleBackupImageTasks{
				scheduler: taskScheduler,
				storage:   storage,
				limit:     int(config.GetScheduleBackupImageTasksLimit()),
			}
		})
		if err != nil {
			return err
		}

		taskScheduler.ScheduleRegularTasks(
			ctx,
			"images.ScheduleBackupImageTasks",
			tasks.TaskSchedule{
				ScheduleInterval: scheduleBackupImageTasksScheduleInterval,
				MaxTasksInflight: 1,
			},
		)

		err = taskRegistry.RegisterForExecution(
			"images.DeleteBackupMeta",
			func() tasks.Task {
				return &deleteBackupMetaTask{
					scheduler: taskScheduler,
					storage:   storage,
					backupS3:  backupS3,
					batchSize: int(
						config.GetDeleteBackupMetaTaskBatchSize(),
					),
				}
			},
		)
		if err != nil {
			return err
		}

		taskScheduler.ScheduleRegularTasks(
			ctx,
			"images.DeleteBackupMeta",
			tasks.TaskSchedule{
				ScheduleInterval: deleteBackupMetaTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)
	}

	err = taskRegistry.RegisterForExecution("images.DeleteImage", func() tasks.Task {
		return &deleteImageTask{
			config:      config,
			scheduler:   taskScheduler,
			storage:     storage,
			poolService: poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.ClearDeletedImages", func() tasks.Task {
		return &clearDeletedImagesTask{
			storage:           storage,
			expirationTimeout: deletedImageExpirationTimeout,
			limit:             int(config.GetClearDeletedImagesLimit()),
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"images.ClearDeletedImages",
		tasks.TaskSchedule{
			ScheduleInterval: clearDeletedImagesTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
