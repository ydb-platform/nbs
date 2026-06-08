package snapshot

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystemSnapshotDataTask struct {
	nodesStorage nodes_storage.Storage
	request      *snapshot_protos.DeleteFilesystemSnapshotDataRequest
	state        *snapshot_protos.DeleteFilesystemSnapshotDataTaskState
}

func (t *deleteFilesystemSnapshotDataTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteFilesystemSnapshotDataTask) Load(request, state []byte) error {
	t.request = &snapshot_protos.DeleteFilesystemSnapshotDataRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &snapshot_protos.DeleteFilesystemSnapshotDataTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteFilesystemSnapshotDataTask) deleteFilesystemSnapshotData(
	ctx context.Context,
) error {

	return t.nodesStorage.DeleteSnapshotData(ctx, t.request.SnapshotId)
}

func (t *deleteFilesystemSnapshotDataTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteFilesystemSnapshotData(ctx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteFilesystemSnapshotDataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteFilesystemSnapshotData(ctx)
}

func (t *deleteFilesystemSnapshotDataTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteFilesystemSnapshotDataTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
