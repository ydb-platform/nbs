package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type deleteSnapshotTask struct {
	storage storage.Storage
	request *protos.DeleteSnapshotRequest
	state   *protos.DeleteSnapshotTaskState
}

func (t *deleteSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.DeleteSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteSnapshotTask) deletingSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotMeta, err := t.storage.DeletingSnapshot(ctx, t.request.SnapshotId, execCtx.GetTaskID())
	if err != nil {
		return err
	}

	if len(snapshotMeta.BaseSnapshotID) != 0 {
		err := t.storage.UnlockSnapshot(
			ctx,
			snapshotMeta.BaseSnapshotID,
			snapshotMeta.CreateTaskID,
		)
		if err != nil {
			return err
		}
		logging.Info(
			ctx,
			"Successfully unlocked snapshot with id %v",
			snapshotMeta.BaseSnapshotID,
		)
	}

	return nil
}

func (t *deleteSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deletingSnapshot(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deletingSnapshot(ctx, execCtx)
}

func (t *deleteSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
