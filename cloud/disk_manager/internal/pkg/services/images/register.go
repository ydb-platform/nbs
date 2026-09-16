package images

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	images_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *images_config.ImagesConfig,
	backupConfig *backup_config.SnapshotStorageBackupConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
	poolService pools.Service,
	cellSelector cells.CellSelector,
	slaveS3 *persistence.S3Client,
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

	backupEnabled := slaveS3 != nil

	err = taskRegistry.RegisterForExecution("images.CreateImageFromURL", func() tasks.Task {
		return &createImageFromURLTask{
			config:        config,
			scheduler:     taskScheduler,
			storage:       storage,
			poolService:   poolService,
			backupEnabled: backupEnabled,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromImage", func() tasks.Task {
		return &createImageFromImageTask{
			config:        config,
			scheduler:     taskScheduler,
			storage:       storage,
			poolService:   poolService,
			backupEnabled: backupEnabled,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromSnapshot", func() tasks.Task {
		return &createImageFromSnapshotTask{
			config:        config,
			scheduler:     taskScheduler,
			storage:       storage,
			poolService:   poolService,
			backupEnabled: backupEnabled,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromDisk", func() tasks.Task {
		return &createImageFromDiskTask{
			config:        config,
			scheduler:     taskScheduler,
			storage:       storage,
			nbsFactory:    nbsFactory,
			poolService:   poolService,
			cellSelector:  cellSelector,
			backupEnabled: backupEnabled,
		}
	})
	if err != nil {
		return err
	}

	if backupEnabled {
		err = taskRegistry.RegisterForExecution("images.BackupImage", func() tasks.Task {
			return &backupImageTask{
				scheduler: taskScheduler,
				storage:   storage,
				s3:        slaveS3,
				bucket:    backupConfig.GetS3Bucket(),
				keyPrefix: backupConfig.GetS3KeyPrefix(),
			}
		})
		if err != nil {
			return err
		}
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
