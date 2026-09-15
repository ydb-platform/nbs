package backup

import (
	"context"
	"time"

	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func Register(taskRegistry *tasks.Registry) error {
	err := taskRegistry.Register(
		"dataplane.BackupSnapshot",
		func() tasks.Task {
			return &backupSnapshotTask{}
		},
	)
	if err != nil {
		return err
	}

	return taskRegistry.Register(
		"dataplane.BackupChunks",
		func() tasks.Task {
			return &backupChunksTask{}
		},
	)
}

func RegisterForExecution(
	ctx context.Context,
	config *backup_config.SnapshotStorageBackupConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage storage.Storage,
	chunkCompression string,
	chunkSize uint32,
	backupS3 *persistence.S3Client,
	metricsRegistry metrics.Registry,
) error {

	backupChunksTaskScheduleInterval, err := time.ParseDuration(
		config.GetBackupChunksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.BackupSnapshot",
		func() tasks.Task {
			return &backupSnapshotTask{
				storage:          storage,
				s3:               backupS3,
				bucket:           config.GetS3Bucket(),
				keyPrefix:        config.GetS3KeyPrefix(),
				chunkSize:        chunkSize,
				chunkCompression: chunkCompression,
				enqueueBatchSize: int(config.GetEnqueueBatchSize()),
			}
		},
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"dataplane.BackupChunks",
		func() tasks.Task {
			return &backupChunksTask{
				storage:      storage,
				dstS3:        backupS3,
				dstBucket:    config.GetS3Bucket(),
				dstKeyPrefix: config.GetS3KeyPrefix(),
				batchSize:    int(config.GetBackupChunksBatchSize()),
				workerCount:  int(config.GetBackupChunksWorkerCount()),
				registry:     metricsRegistry,
			}
		},
	)
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"dataplane.BackupChunks",
		tasks.TaskSchedule{
			ScheduleInterval: backupChunksTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
