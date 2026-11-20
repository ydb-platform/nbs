package snapshots

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
	snapshots_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *snapshots_config.SnapshotsConfig,
	disabledZones *common.DisabledZones,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
	cellSelector cells.CellSelector,
	backupS3 *backup.S3,
	backupMetricsRegistry metrics.Registry,
	exportConfigs ...*server_config.ExportConfig,
) error {

	var exportConfig *server_config.ExportConfig
	if len(exportConfigs) != 0 {
		exportConfig = exportConfigs[0]
	}

	registerExport := taskRegistry.Register
	if exportConfig != nil {
		registerExport = taskRegistry.RegisterForExecution
	}

	err := registerExport(
		"snapshots.ExportSnapshot",
		func() tasks.Task {
			return &exportSnapshotTask{
				scheduler: taskScheduler,
				storage:   storage,
				config:    exportConfig,
			}
		},
	)
	if err != nil {
		return err
	}

	deletedSnapshotExpirationTimeout, err := time.ParseDuration(
		config.GetDeletedSnapshotExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearDeletedSnapshotsTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedSnapshotsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	scheduleBackupSnapshotTasksScheduleInterval, err := time.ParseDuration(
		config.GetScheduleBackupSnapshotTasksScheduleInterval(),
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

	err = taskRegistry.RegisterForExecution("snapshots.CreateSnapshotFromDisk", func() tasks.Task {
		return &createSnapshotFromDiskTask{
			scheduler:     taskScheduler,
			storage:       storage,
			nbsFactory:    nbsFactory,
			cellSelector:  cellSelector,
			disabledZones: disabledZones,
		}
	})
	if err != nil {
		return err
	}

	if backupS3 != nil {
		err = taskRegistry.RegisterForExecution("snapshots.BackupSnapshot", func() tasks.Task {
			return &backupSnapshotTask{
				scheduler: taskScheduler,
				storage:   storage,
				backupS3:  backupS3,
			}
		})
		if err != nil {
			return err
		}

		err = taskRegistry.RegisterForExecution("snapshots.ScheduleBackupSnapshotTasks", func() tasks.Task {
			return &scheduleBackupSnapshotTasks{
				scheduler: taskScheduler,
				storage:   storage,
				registry:  backupMetricsRegistry,
				limit:     int(config.GetScheduleBackupSnapshotTasksLimit()),
				inflightLimit: int(
					config.GetBackupSnapshotsInflightLimit(),
				),
			}
		})
		if err != nil {
			return err
		}

		taskScheduler.ScheduleRegularTasks(
			ctx,
			"snapshots.ScheduleBackupSnapshotTasks",
			tasks.TaskSchedule{
				ScheduleInterval: scheduleBackupSnapshotTasksScheduleInterval,
				MaxTasksInflight: 1,
			},
		)

		err = taskRegistry.RegisterForExecution(
			"snapshots.CollectBackupQueueMetrics",
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
			"snapshots.CollectBackupQueueMetrics",
			tasks.TaskSchedule{
				ScheduleInterval: collectBackupQueueMetricsTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)

		err = taskRegistry.RegisterForExecution(
			"snapshots.DeleteBackupMeta",
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
			"snapshots.DeleteBackupMeta",
			tasks.TaskSchedule{
				ScheduleInterval: deleteBackupMetaTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)
	}

	err = taskRegistry.RegisterForExecution("snapshots.DeleteSnapshot", func() tasks.Task {
		return &deleteSnapshotTask{
			scheduler:  taskScheduler,
			storage:    storage,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"snapshots.SaveSnapshotChecksum",
		func() tasks.Task {
			return &saveSnapshotChecksumTask{
				scheduler: taskScheduler,
				storage:   storage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"snapshots.ValidateSnapshotChecksum",
		func() tasks.Task {
			return &validateSnapshotChecksumTask{
				scheduler: taskScheduler,
				storage:   storage,
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("snapshots.ClearDeletedSnapshots", func() tasks.Task {
		return &clearDeletedSnapshotsTask{
			storage:           storage,
			expirationTimeout: deletedSnapshotExpirationTimeout,
			limit:             int(config.GetClearDeletedSnapshotsLimit()),
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"snapshots.ClearDeletedSnapshots",
		tasks.TaskSchedule{
			ScheduleInterval: clearDeletedSnapshotsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// Schedules the task that computes the checksum of the snapshot and saves it
// to the database. Returns id of the task.
//
// The task is scheduled with the incoming idempotency key of |ctx|, so the
// same key gives the same task: it should not be used for another snapshot or
// for a task of another type. If |ctx| has no such key, a new one is
// generated, so each call schedules a new task.
func ScheduleSaveSnapshotChecksum(
	ctx context.Context,
	scheduler tasks.Scheduler,
	snapshotID string,
) (string, error) {

	return scheduleChecksumTask(
		ctx,
		scheduler,
		"snapshots.SaveSnapshotChecksum",
		snapshotID,
		&protos.SaveSnapshotChecksumRequest{
			SnapshotId: snapshotID,
		},
	)
}

// Schedules the task that computes the checksum of the snapshot and compares
// it with the saved one. The task fails if they differ or if there is no
// saved checksum. Returns id of the task.
//
// See ScheduleSaveSnapshotChecksum for the idempotency key.
func ScheduleValidateSnapshotChecksum(
	ctx context.Context,
	scheduler tasks.Scheduler,
	snapshotID string,
) (string, error) {

	return scheduleChecksumTask(
		ctx,
		scheduler,
		"snapshots.ValidateSnapshotChecksum",
		snapshotID,
		&protos.ValidateSnapshotChecksumRequest{
			SnapshotId: snapshotID,
		},
	)
}

func scheduleChecksumTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	taskType string,
	snapshotID string,
	request proto.Message,
) (string, error) {

	if len(headers.GetIdempotencyKey(ctx)) == 0 {
		// The scheduler does not accept a task without a key.
		ctx = headers.SetIncomingIdempotencyKey(
			ctx,
			fmt.Sprintf(
				"%v_%v_%v",
				taskType,
				snapshotID,
				uuid.Must(uuid.NewV4()).String(),
			),
		)
	}

	return scheduler.ScheduleTask(ctx, taskType, "", request)
}
