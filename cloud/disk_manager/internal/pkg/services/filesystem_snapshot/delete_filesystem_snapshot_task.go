package filesystem_snapshot

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystemSnapshotTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	request   *protos.DeleteFilesystemSnapshotRequest
	state     *protos.DeleteFilesystemSnapshotTaskState
}

func (t *deleteFilesystemSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteFilesystemSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.DeleteFilesystemSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteFilesystemSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteFilesystemSnapshotTask) deleteFilesystemSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.DeleteFilesystemSnapshot(
		ctx,
		t.request.FilesystemSnapshotId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if snapshotMeta == nil {
		// Should be idempotent.
		return nil
	}

	if snapshotMeta.DeleteTaskID != selfTaskID {
		return t.scheduler.WaitTaskEnded(ctx, snapshotMeta.DeleteTaskID)
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID),
		"dataplane.DeleteFilesystemSnapshot",
		"",
		&dataplane_protos.DeleteSnapshotRequest{
			SnapshotId: t.request.FilesystemSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.storage.FilesystemSnapshotDeleted(ctx, t.request.FilesystemSnapshotId, time.Now())
}

func (t *deleteFilesystemSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteFilesystemSnapshot(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteFilesystemSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteFilesystemSnapshot(ctx, execCtx)
}

func (t *deleteFilesystemSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeleteFilesystemSnapshotMetadata{
		FilesystemSnapshotId: t.request.FilesystemSnapshotId,
	}, nil
}

func (t *deleteFilesystemSnapshotTask) GetResponse() proto.Message {
	return &disk_manager.DeleteFilesystemSnapshotResponse{}
}
