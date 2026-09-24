package images

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
		imageIDs, err := t.storage.GetImageBackupDeleteQueue(
			ctx,
			t.batchSize,
		)
		if err != nil {
			return err
		}

		if len(imageIDs) == 0 {
			return errors.NewInterruptExecutionError()
		}

		for _, imageID := range imageIDs {
			err = t.deleteImageBackup(ctx, execCtx, imageID)
			if err != nil {
				return err
			}
		}

		err = t.storage.ImageBackupDeletionsCompleted(ctx, imageIDs)
		if err != nil {
			return err
		}
	}
}

func (t *deleteBackupMetaTask) deleteImageBackup(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	imageID string,
) error {

	idempotencyKey := fmt.Sprintf(
		"%v_%v_delete_backup",
		execCtx.GetTaskID(),
		imageID,
	)
	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
		"dataplane.DeleteBackupSnapshotData",
		"",
		&dataplane_protos.DeleteBackupSnapshotDataRequest{
			SnapshotId: imageID,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.backupS3.DeleteImageMeta(ctx, imageID)
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
