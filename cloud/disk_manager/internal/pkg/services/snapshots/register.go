package snapshots

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	snapshots_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *snapshots_config.SnapshotsConfig,
	backupConfig *backup_config.SnapshotStorageBackupConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
	cellSelector cells.CellSelector,
	slaveS3 *persistence.S3Client,
) error {

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

	err = taskRegistry.RegisterForExecution("snapshots.CreateSnapshotFromDisk", func() tasks.Task {
		return &createSnapshotFromDiskTask{
			scheduler:     taskScheduler,
			storage:       storage,
			nbsFactory:    nbsFactory,
			cellSelector:  cellSelector,
			backupEnabled: slaveS3 != nil,
		}
	})
	if err != nil {
		return err
	}

	if slaveS3 != nil {
		err = taskRegistry.RegisterForExecution("snapshots.BackupSnapshot", func() tasks.Task {
			return &backupSnapshotTask{
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
