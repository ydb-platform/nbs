package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type relocateSnapshotDataFromYDBToS3Task struct {
	storage storage.Storage
	request *protos.RelocateSnapshotDataFromYDBToS3Request
	state   *protos.RelocateSnapshotDataFromYDBToS3TaskState
}

func (t *relocateSnapshotDataFromYDBToS3Task) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *relocateSnapshotDataFromYDBToS3Task) Load(request, state []byte) error {
	t.request = &protos.RelocateSnapshotDataFromYDBToS3Request{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.RelocateSnapshotDataFromYDBToS3TaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *relocateSnapshotDataFromYDBToS3Task) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (err error) {

	meta, err := t.storage.CheckSnapshotReady(ctx, t.request.SnapshotId)
	if err != nil {
		return err
	}

	t.state.ChunkCount = meta.ChunkCount

	locked, err := t.storage.LockSnapshot(
		ctx,
		t.request.SnapshotId,
		execCtx.GetTaskID(),
	)
	if err != nil {
		return err
	}
	if !locked {
		return task_errors.NewNonRetriableErrorf(
			"failed to lock snapshot %v for relocate to s3",
			t.request.SnapshotId,
		)
	}

	defer func() {
		unlockErr := t.storage.UnlockSnapshot(
			ctx,
			t.request.SnapshotId,
			execCtx.GetTaskID(),
		)
		if unlockErr != nil {
			logging.Error(
				ctx,
				"failed to unlock snapshot %v after relocate: %v",
				t.request.SnapshotId,
				unlockErr,
			)
			if err == nil {
				err = unlockErr
			}
		}
	}()

	err = t.storage.RelocateSnapshotChunksToS3(
		ctx,
		t.request.SnapshotId,
		t.state.MilestoneChunkIndex,
		func(ctx context.Context, milestoneChunkIndex uint32) error {
			_, checkErr := t.storage.CheckSnapshotReady(
				ctx,
				t.request.SnapshotId,
			)
			if checkErr != nil {
				return checkErr
			}

			t.state.MilestoneChunkIndex = milestoneChunkIndex
			return t.saveProgress(ctx, execCtx)
		},
	)
	if err != nil {
		return err
	}

	t.state.MilestoneChunkIndex = t.state.ChunkCount
	t.state.Progress = 1
	return execCtx.SaveState(ctx)
}

func (t *relocateSnapshotDataFromYDBToS3Task) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.UnlockSnapshot(
		ctx,
		t.request.SnapshotId,
		execCtx.GetTaskID(),
	)
}

func (t *relocateSnapshotDataFromYDBToS3Task) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.RelocateSnapshotDataFromYDBToS3Metadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *relocateSnapshotDataFromYDBToS3Task) GetResponse() proto.Message {
	return &protos.RelocateSnapshotDataFromYDBToS3Response{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *relocateSnapshotDataFromYDBToS3Task) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.ChunkCount != 0 {
		t.state.Progress =
			float64(t.state.MilestoneChunkIndex) / float64(t.state.ChunkCount)
	}

	logging.Debug(ctx, "saving relocate to s3 state %+v", t.state)
	return execCtx.SaveState(ctx)
}
