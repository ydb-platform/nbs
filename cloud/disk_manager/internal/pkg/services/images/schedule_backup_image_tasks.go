package images

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

// A data query returns at most this many rows.
const maxScheduledImageBackupsRead = 1000

////////////////////////////////////////////////////////////////////////////////

// Starts queued backup attempts while fewer than inflightLimit copies run, so
// that a few images are copied at a time instead of all of them together.
type scheduleBackupImageTasks struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	registry  metrics.Registry
	// Attempts started per pass at most.
	limit int
	// Copies running at once; 0 = no limit.
	inflightLimit int
}

func (t *scheduleBackupImageTasks) Save() ([]byte, error) {
	return nil, nil
}

func (t *scheduleBackupImageTasks) Load(_, _ []byte) error {
	return nil
}

func (t *scheduleBackupImageTasks) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	inflight, err := t.countRunningCopies(ctx)
	if err != nil {
		return err
	}

	limit := t.limit
	if t.inflightLimit > 0 {
		limit = min(limit, t.inflightLimit-inflight)
	}
	if limit <= 0 {
		return nil
	}

	backups, err := t.storage.ListImagesToBackup(ctx, limit)
	if err != nil {
		return err
	}

	for _, item := range backups {
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				fmt.Sprintf(
					"backup_image_%v_%v",
					item.ImageID,
					item.BackupID,
				),
			),
			"images.BackupImage",
			"",
			&protos.BackupImageRequest{
				ImageId:  item.ImageID,
				BackupId: item.BackupID,
			},
		)
		if err != nil {
			return err
		}

		// If this fails, the next pass schedules the same task again by the
		// same key and records it.
		enqueuedAt, err := t.storage.ImageBackupScheduled(
			ctx,
			item.ImageID,
			item.BackupID,
			taskID,
		)
		if err != nil {
			return err
		}

		if !enqueuedAt.IsZero() {
			t.registry.Timer("backup/imageQueueWaitTime").RecordDuration(
				time.Since(enqueuedAt),
			)
		}
	}

	return nil
}

func (t *scheduleBackupImageTasks) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBackupImageTasks) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scheduleBackupImageTasks) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

// An images.BackupImage task removes its queue row when it ends. A row whose
// task ended anyway, for example force-finished, would hold a slot forever, so
// it is removed here. A task that is being cancelled counts as ended: its slot
// frees before its copy has cleared its chunks.
func (t *scheduleBackupImageTasks) countRunningCopies(
	ctx context.Context,
) (int, error) {

	scheduled, err := t.storage.ListScheduledImageBackups(
		ctx,
		maxScheduledImageBackupsRead,
	)
	if err != nil {
		return 0, err
	}

	if len(scheduled) == 0 {
		return 0, nil
	}

	var taskIDs []string
	for _, backup := range scheduled {
		taskIDs = append(taskIDs, backup.TaskID)
	}

	ended, err := t.scheduler.GetEndedTasks(ctx, taskIDs)
	if err != nil {
		return 0, err
	}

	endedTaskIDs := make(map[string]struct{}, len(ended))
	for _, taskID := range ended {
		endedTaskIDs[taskID] = struct{}{}
	}

	for _, backup := range scheduled {
		if _, ok := endedTaskIDs[backup.TaskID]; !ok {
			continue
		}

		err = t.storage.RemoveImageFromBackupQueue(
			ctx,
			backup.ImageID,
			backup.BackupID,
		)
		if err != nil {
			return 0, err
		}
	}

	return len(scheduled) - len(ended), nil
}
