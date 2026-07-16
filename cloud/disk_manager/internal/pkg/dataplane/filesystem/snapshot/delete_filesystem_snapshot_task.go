package snapshot

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystemSnapshotTask struct {
	storage snapshot_storage.Storage
	request *snapshot_protos.DeleteFilesystemSnapshotRequest
	state   *snapshot_protos.DeleteFilesystemSnapshotTaskState
}

func (t *deleteFilesystemSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteFilesystemSnapshotTask) Load(request, state []byte) error {
	t.request = &snapshot_protos.DeleteFilesystemSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &snapshot_protos.DeleteFilesystemSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteFilesystemSnapshotTask) deleteFilesystemSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	_, err := t.storage.DeletingFilesystemSnapshot(
		ctx,
		t.request.SnapshotId,
		execCtx.GetTaskID(),
	)
	return err
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

	return &empty.Empty{}, nil
}

func (t *deleteFilesystemSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
