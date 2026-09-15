package backup

import (
	"context"
	"time"

	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
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

// Chunks are copied from the s3 chunk storage of snapshotConfig to the bucket
// of config.
func RegisterForExecution(
	ctx context.Context,
	config *backup_config.SnapshotStorageBackupConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage storage.Storage,
	snapshotConfig *snapshot_config.SnapshotConfig,
	s3 *persistence.S3Client,
	chunkSize uint32,
	backupS3 *persistence.S3Client,
	metricsRegistry metrics.Registry,
) error {

	if s3 == nil {
		return errors.NewNonRetriableErrorf(
			"snapshot backup requires s3 chunk storage",
		)
	}

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
				chunkCompression: snapshotConfig.GetChunkCompression(),
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
				srcS3:        s3,
				srcBucket:    snapshotConfig.GetS3Bucket(),
				srcKeyPrefix: snapshotConfig.GetChunkBlobsS3KeyPrefix(),
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
