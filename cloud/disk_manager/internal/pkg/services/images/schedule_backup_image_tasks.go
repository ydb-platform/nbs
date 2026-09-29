package images

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type scheduleBackupImageTasks struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	limit     int
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

	imageIDs, err := t.storage.ListImagesToBackup(ctx, t.limit)
	if err != nil {
		return err
	}

	for _, imageID := range imageIDs {
		idempotencyKey := fmt.Sprintf("backup_image_%v", imageID)

		_, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
			"images.BackupImage",
			"",
			&protos.BackupImageRequest{
				ImageId: imageID,
			},
		)
		if err != nil {
			return err
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
