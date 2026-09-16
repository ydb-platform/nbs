package images

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupImageTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
	request   *protos.BackupImageRequest
	state     *protos.BackupImageTaskState
}

func (t *backupImageTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupImageTask) Load(request, state []byte) error {
	t.request = &protos.BackupImageRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupImageTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupImageTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	imageID := t.request.ImageId

	meta, err := t.storage.GetImageMeta(ctx, imageID)
	if err != nil {
		return err
	}

	if meta == nil {
		return errors.NewNonRetriableErrorf(
			"image %v is not found",
			imageID,
		)
	}

	imageMeta, err := backup.NewImageMeta(*meta)
	if err != nil {
		return err
	}

	err = backup.WriteMeta(
		ctx,
		t.s3,
		t.bucket,
		backup.ImageMetaKey(t.keyPrefix, imageID),
		imageMeta,
	)
	if err != nil {
		return err
	}

	taskID, err := backup.ScheduleBackupSnapshot(ctx, execCtx, t.scheduler, imageID)
	if err != nil {
		return err
	}

	t.state.DataplaneTaskID = taskID

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	return err
}

func (t *backupImageTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *backupImageTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupImageTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
