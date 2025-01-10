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

type migrateSnapshotToAnotherDatabaseTask struct {
	sourceStorage      storage.Storage
	destinationStorage storage.Storage
	config             *config.DataplaneConfig
	request            *protos.MigrateSnapshotToAnotherDatabaseRequest
	state              *protos.MigrateSnapshotToAnotherDatabaseTaskState
}

func (t *migrateSnapshotToAnotherDatabaseTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *migrateSnapshotToAnotherDatabaseTask) Load(
	request, state []byte,
) error {

	t.request = &protos.MigrateSnapshotToAnotherDatabaseRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.MigrateSnapshotToAnotherDatabaseTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *migrateSnapshotToAnotherDatabaseTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	srcMeta, err := t.sourceStorage.CheckSnapshotReady(
		ctx,
		t.request.SrcSnapshotId,
	)
	if err != nil {
		return err
	}

	t.state.ChunkCount = srcMeta.ChunkCount

	source := snapshot.NewSnapshotSource(
		t.request.SrcSnapshotId,
		t.sourceStorage,
	)
	defer source.Close(ctx)

	target := snapshot.NewSnapshotTarget(
		execCtx.GetTaskID(),
		t.request.SrcSnapshotId,
		t.destinationStorage,
		true,
		t.request.UseS3,
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
			_, err := t.sourceStorage.CheckSnapshotReady(
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

	dataChunkCount, err := t.destinationStorage.GetDataChunkCount(
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

	return nil
}

func (t *migrateSnapshotToAnotherDatabaseTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {
	return nil
}

func (t *migrateSnapshotToAnotherDatabaseTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {
	return &protos.CreateSnapshotFromLegacySnapshotMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *migrateSnapshotToAnotherDatabaseTask) GetResponse() proto.Message {
	return &protos.MigrateSnapshotToAnotherDatabaseResponse{
		SnapshotSize:        t.state.SnapshotSize,
		SnapshotStorageSize: t.state.SnapshotStorageSize,
		TransferredDataSize: t.state.TransferredDataSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *migrateSnapshotToAnotherDatabaseTask) saveProgress(
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
