package images

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	images_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *images_config.ImagesConfig,
	disabledZones *common.DisabledZones,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
	poolService pools.Service,
	cellSelector cells.CellSelector,
	backupS3 *backup.S3,
	backupMetricsRegistry metrics.Registry,
	exportConfigs ...*server_config.ExportConfig,
) error {

	var exportConfig *server_config.ExportConfig
	if len(exportConfigs) != 0 {
		exportConfig = exportConfigs[0]
	}

	if exportConfig != nil {
		err := taskRegistry.RegisterForExecution(
			"images.ExportImage",
			func() tasks.Task {
				return &exportImageTask{
					scheduler: taskScheduler,
					storage:   storage,
					config:    exportConfig,
				}
			},
		)
		if err != nil {
			return err
		}
	}

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

		collectBackupQueueMetricsTaskScheduleInterval, err := time.ParseDuration(
			config.GetCollectBackupQueueMetricsTaskScheduleInterval(),
		)
		if err != nil {
			return err
		}

		backupQueueMetricsCollectionInterval, err := time.ParseDuration(
			config.GetBackupQueueMetricsCollectionInterval(),
		)
		if err != nil {
			return err
		}

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
				registry:  backupMetricsRegistry,
				limit:     int(config.GetScheduleBackupImageTasksLimit()),
				inflightLimit: int(
					config.GetBackupImagesInflightLimit(),
				),
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
			"images.CollectBackupQueueMetrics",
			func() tasks.Task {
				return &collectBackupQueueMetricsTask{
					storage:            storage,
					registry:           backupMetricsRegistry,
					collectionInterval: backupQueueMetricsCollectionInterval,
				}
			},
		)
		if err != nil {
			return err
		}

		taskScheduler.ScheduleRegularTasks(
			ctx,
			"images.CollectBackupQueueMetrics",
			tasks.TaskSchedule{
				ScheduleInterval: collectBackupQueueMetricsTaskScheduleInterval,
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
			config:        config,
			disabledZones: disabledZones,
			scheduler:     taskScheduler,
			storage:       storage,
			poolService:   poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"images.SaveImageChecksum",
		func() tasks.Task {
			return &saveImageChecksumTask{
				scheduler: taskScheduler,
				storage:   storage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"images.ValidateImageChecksum",
		func() tasks.Task {
			return &validateImageChecksumTask{
				scheduler: taskScheduler,
				storage:   storage,
			}
		},
	)
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

////////////////////////////////////////////////////////////////////////////////

// Schedules the task that computes the checksum of the image and saves it to
// the database. Returns id of the task.
//
// The task is scheduled with the incoming idempotency key of |ctx|, so the
// same key gives the same task: it should not be used for another image or
// for a task of another type. If |ctx| has no such key, a new one is
// generated, so each call schedules a new task.
func ScheduleSaveImageChecksum(
	ctx context.Context,
	scheduler tasks.Scheduler,
	imageID string,
) (string, error) {

	return scheduleChecksumTask(
		ctx,
		scheduler,
		"images.SaveImageChecksum",
		imageID,
		&protos.SaveImageChecksumRequest{
			ImageId: imageID,
		},
	)
}

// Schedules the task that computes the checksum of the image and compares it
// with the saved one. The task fails if they differ or if there is no saved
// checksum. Returns id of the task.
//
// See ScheduleSaveImageChecksum for the idempotency key.
func ScheduleValidateImageChecksum(
	ctx context.Context,
	scheduler tasks.Scheduler,
	imageID string,
) (string, error) {

	return scheduleChecksumTask(
		ctx,
		scheduler,
		"images.ValidateImageChecksum",
		imageID,
		&protos.ValidateImageChecksumRequest{
			ImageId: imageID,
		},
	)
}

func scheduleChecksumTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	taskType string,
	imageID string,
	request proto.Message,
) (string, error) {

	if len(headers.GetIdempotencyKey(ctx)) == 0 {
		// The scheduler does not accept a task without a key.
		ctx = headers.SetIncomingIdempotencyKey(
			ctx,
			fmt.Sprintf(
				"%v_%v_%v",
				taskType,
				imageID,
				uuid.Must(uuid.NewV4()).String(),
			),
		)
	}

	return scheduler.ScheduleTask(ctx, taskType, "", request)
}
