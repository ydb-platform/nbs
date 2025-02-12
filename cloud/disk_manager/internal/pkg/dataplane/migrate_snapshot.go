package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type migrateSnapshotTask struct {
	srcStorage storage.Storage
	dstStorage storage.Storage
	config     *config.DataplaneConfig
	useS3      bool
	request    *protos.MigrateSnapshotRequest
	state      *protos.MigrateSnapshotTaskState
}

func (t *migrateSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *migrateSnapshotTask) Load(
	request, state []byte,
) error {

	t.request = &protos.MigrateSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.MigrateSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *migrateSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	srcMeta, err := t.srcStorage.CheckSnapshotReady(
		ctx,
		t.request.SrcSnapshotId,
	)
	if err != nil {
		return err
	}

	t.state.ChunkCount = srcMeta.ChunkCount

	_, err = t.dstStorage.CreateSnapshot(
		ctx,
		storage.SnapshotMeta{
			ID:           t.request.SrcSnapshotId,
			Disk:         srcMeta.Disk,
			CheckpointID: srcMeta.CheckpointID,
			CreateTaskID: execCtx.GetTaskID(),
		},
	)
	if err != nil {
		return err
	}

	source := snapshot.NewSnapshotSource(
		t.request.SrcSnapshotId,
		t.srcStorage,
	)
	defer source.Close(ctx)

	target := snapshot.NewSnapshotTarget(
		execCtx.GetTaskID(),
		t.request.SrcSnapshotId,
		t.dstStorage,
		true, // ignoreZeroChunks
		t.useS3,
	)
	defer target.Close(ctx)

	transferer := common.Transferer{
		ReaderCount:         t.config.GetReaderCount(),
		WriterCount:         t.config.GetWriterCount(),
		ChunksInflightLimit: t.config.GetChunksInflightLimit(),
		ChunkSize:           chunkSize,
	}

	transferredChunkCount, err := transferer.Transfer(
		ctx,
		source,
		target,
		common.Milestone{
			ChunkIndex:            t.state.MilestoneChunkIndex,
			TransferredChunkCount: t.state.TransferredChunkCount,
		},
		func(ctx context.Context, milestone common.Milestone) error {
			_, err := t.srcStorage.CheckSnapshotReady(
				ctx,
				t.request.SrcSnapshotId,
			)
			if err != nil {
				return err
			}

			err = t.dstStorage.CheckSnapshotAlive(
				ctx,
				t.request.SrcSnapshotId,
			)
			if err != nil {
				return err
			}

			t.state.MilestoneChunkIndex = milestone.ChunkIndex
			t.state.TransferredChunkCount = milestone.TransferredChunkCount
			return t.saveProgress(ctx, execCtx)
		},
	)
	if err != nil {
		return err
	}

	t.state.MilestoneChunkIndex = t.state.ChunkCount
	t.state.TransferredChunkCount = transferredChunkCount
	t.state.Progress = 1

	dataChunkCount, err := t.dstStorage.GetDataChunkCount(
		ctx,
		t.request.SrcSnapshotId,
	)
	if err != nil {
		return err
	}

	size := srcMeta.Size
	storageSize := dataChunkCount * chunkSize

	t.state.SnapshotSize = size
	t.state.SnapshotStorageSize = storageSize
	t.state.TransferredDataSize =
		uint64(t.state.TransferredChunkCount) * chunkSize

	return t.dstStorage.SnapshotCreated(
		ctx,
		t.request.SrcSnapshotId,
		size,
		storageSize,
		t.state.ChunkCount,
		srcMeta.Encryption,
	)
}

func (t *migrateSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	_, err := t.dstStorage.DeletingSnapshot(ctx, t.request.SrcSnapshotId, execCtx.GetTaskID())
	return err
}

func (t *migrateSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.MigrateSnapshotMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *migrateSnapshotTask) GetResponse() proto.Message {
	return &protos.MigrateSnapshotResponse{
		SnapshotSize:        t.state.SnapshotSize,
		SnapshotStorageSize: t.state.SnapshotStorageSize,
		TransferredDataSize: t.state.TransferredDataSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *migrateSnapshotTask) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {
	if t.state.ChunkCount != 0 {
		t.state.Progress =
			float64(t.state.MilestoneChunkIndex) / float64(t.state.ChunkCount)
	}

	logging.Debug(ctx, "saving state %+v", t.state)
	return execCtx.SaveState(ctx)
}
