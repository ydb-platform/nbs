package snapshots

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type deleteBackupMetaTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	backupS3  *backup.S3
	batchSize int
}

func (t *deleteBackupMetaTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *deleteBackupMetaTask) Load(_, _ []byte) error {
	return nil
}

func (t *deleteBackupMetaTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {
		snapshotBackupIDsForDeletion, err :=
			t.storage.GetSnapshotBackupDeleteQueue(
				ctx,
				t.batchSize,
			)
		if err != nil {
			return err
		}

		if len(snapshotBackupIDsForDeletion) == 0 {
			return errors.NewInterruptExecutionError()
		}

		// All backups of the batch are cancelled first, so that they stop
		// at the same time.
		backupTaskIDs := make([]string, len(snapshotBackupIDsForDeletion))
		for i, snapshotBackupID := range snapshotBackupIDsForDeletion {
			backupTaskIDs[i], err = t.cancelBackup(ctx, snapshotBackupID)
			if err != nil {
				return err
			}
		}

		var snapshotIDs []string
		for i, snapshotBackupID := range snapshotBackupIDsForDeletion {
			// A running backup could still write meta.json.
			if len(backupTaskIDs[i]) != 0 {
				err = t.scheduler.WaitTaskEnded(ctx, backupTaskIDs[i])
				if err != nil {
					return err
				}
			}

			err = t.deleteSnapshotBackup(ctx, execCtx, snapshotBackupID)
			if err != nil {
				return err
			}

			snapshotIDs = append(snapshotIDs, snapshotBackupID.SnapshotID)
		}

		err = t.storage.SnapshotBackupDeletionsCompleted(ctx, snapshotIDs)
		if err != nil {
			return err
		}
	}
}

// Cancels the backup attempt queued when the snapshot was deleted and returns
// its task, or an empty string if no attempt was queued. The task is found by
// the idempotency key it was scheduled with. If it is not found, a new one is
// created; it is cancelled at once and, if it runs, finds the snapshot
// deleting and copies nothing.
func (t *deleteBackupMetaTask) cancelBackup(
	ctx context.Context,
	snapshotBackupID resources.SnapshotBackupID,
) (string, error) {

	if len(snapshotBackupID.BackupID) == 0 {
		return "", nil
	}

	taskID, err := scheduleBackupSnapshotTask(
		ctx,
		t.scheduler,
		snapshotBackupID.SnapshotID,
		snapshotBackupID.BackupID,
	)
	if err != nil {
		return "", err
	}

	_, err = t.scheduler.CancelTask(ctx, taskID)
	if err != nil {
		return "", err
	}

	return taskID, nil
}

func (t *deleteBackupMetaTask) deleteSnapshotBackup(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshotBackupID resources.SnapshotBackupID,
) error {

	idempotencyKey := fmt.Sprintf(
		"%v_%v_delete_backup",
		execCtx.GetTaskID(),
		snapshotBackupID.SnapshotID,
	)
	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
		"dataplane.DeleteBackupSnapshotData",
		"",
		&dataplane_protos.DeleteBackupSnapshotDataRequest{
			SnapshotId: snapshotBackupID.SnapshotID,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.backupS3.DeleteSnapshotMeta(
		ctx,
		snapshotBackupID.DiskID,
		snapshotBackupID.SnapshotID,
	)
}

func (t *deleteBackupMetaTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteBackupMetaTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteBackupMetaTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
