package snapshots

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

// A data query returns at most this many rows.
const maxScheduledSnapshotBackupsRead = 1000

////////////////////////////////////////////////////////////////////////////////

// Starts queued backup attempts while fewer than inflightLimit copies run, so
// that a few snapshots are copied at a time instead of all of them together.
type scheduleBackupSnapshotTasks struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	registry  metrics.Registry
	// Attempts started per pass at most.
	limit int
	// Copies running at once; 0 = no limit.
	inflightLimit int
}

func (t *scheduleBackupSnapshotTasks) Save() ([]byte, error) {
	return nil, nil
}

func (t *scheduleBackupSnapshotTasks) Load(_, _ []byte) error {
	return nil
}

func (t *scheduleBackupSnapshotTasks) Run(
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

	backups, err := t.storage.ListSnapshotsToBackup(ctx, limit)
	if err != nil {
		return err
	}

	for _, item := range backups {
		idempotencyKey := fmt.Sprintf(
			"backup_snapshot_%v_%v",
			item.SnapshotID,
			item.BackupID,
		)

		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
			"snapshots.BackupSnapshot",
			"",
			&protos.BackupSnapshotRequest{
				SnapshotId: item.SnapshotID,
				BackupId:   item.BackupID,
			},
		)
		if err != nil {
			return err
		}

		// If this fails, the next pass schedules the same task again by the
		// same key and records it.
		enqueuedAt, err := t.storage.SnapshotBackupScheduled(
			ctx,
			item.SnapshotID,
			item.BackupID,
			taskID,
		)
		if err != nil {
			return err
		}

		if !enqueuedAt.IsZero() {
			t.registry.Timer("backup/snapshotQueueWaitTime").RecordDuration(
				time.Since(enqueuedAt),
			)
		}
	}

	return nil
}

func (t *scheduleBackupSnapshotTasks) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBackupSnapshotTasks) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scheduleBackupSnapshotTasks) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

// A snapshots.BackupSnapshot task removes its queue row when it ends. A row
// whose task ended anyway, for example force-finished, would hold a slot
// forever, so it is removed here. A task that is being cancelled counts as
// ended: its slot frees before its copy has cleared its chunks.
func (t *scheduleBackupSnapshotTasks) countRunningCopies(
	ctx context.Context,
) (int, error) {

	scheduled, err := t.storage.ListScheduledSnapshotBackups(
		ctx,
		maxScheduledSnapshotBackupsRead,
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

		err = t.storage.RemoveSnapshotFromBackupQueue(
			ctx,
			backup.SnapshotID,
			backup.BackupID,
		)
		if err != nil {
			return 0, err
		}
	}

	return len(scheduled) - len(ended), nil
}
