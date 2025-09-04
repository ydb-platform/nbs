package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type checksumSaverTarget struct {
	checksums []uint32
}

func (t *checksumSaverTarget) Write(ctx context.Context, chunk common.Chunk) error {
	t.checksums[chunk.Index] = chunk.Checksum()
	return nil
}

func (t *checksumSaverTarget) Close(ctx context.Context) {}

////////////////////////////////////////////////////////////////////////////////

func makeNonRetriableErrorsNonSilent(err error) error {
	nonRetriableErr := errors.NewEmptyNonRetriableError()
	if errors.As(err, &nonRetriableErr) {
		nonRetriableErr.Silent = false
		return nonRetriableErr
	}

	return err
}

////////////////////////////////////////////////////////////////////////////////

type verifyMigratedLegacySnapshotTask struct {
	config        *config.DataplaneConfig
	storage       storage.Storage
	legacyStorage storage.Storage
	request       *protos.VerifyMigratedLegacySnapshotRequest
	state         *protos.VerifyMigratedLegacySnapshotState
}

func (t *verifyMigratedLegacySnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *verifyMigratedLegacySnapshotTask) Load(request, state []byte) error {
	t.request = &protos.VerifyMigratedLegacySnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.VerifyMigratedLegacySnapshotState{}
	return proto.Unmarshal(state, t.state)
}

func (t *verifyMigratedLegacySnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	meta, err := t.storage.CheckSnapshotReady(
		ctx,
		t.request.SnapshotId,
	)
	if err != nil {
		// Error returned by CheckSnapshotReady is silent
		return makeNonRetriableErrorsNonSilent(err)
	}

	legacyMeta, err := t.legacyStorage.CheckSnapshotReady(
		ctx,
		t.request.SnapshotId,
	)
	if err != nil {
		return makeNonRetriableErrorsNonSilent(err)
	}

	if meta.Size != legacyMeta.Size {
		return errors.NewNonRetriableErrorf(
			"size mismatch: %d != %d", meta.Size, legacyMeta.Size,
		)
	}

	if meta.ChunkCount != legacyMeta.ChunkCount {
		return errors.NewNonRetriableErrorf(
			"chunk count mismatch: %d != %d",
			meta.ChunkCount, legacyMeta.ChunkCount,
		)
	}

	t.state.ChunkCount = meta.ChunkCount

	target, err := t.runTransferer(
		ctx, execCtx, t.storage, &t.state.MilestoneChunkIndex,
	)
	if err != nil {
		return err
	}
	t.state.MilestoneChunkIndex = t.state.ChunkCount

	legacyTarget, err := t.runTransferer(
		ctx, execCtx, t.legacyStorage, &t.state.LegacyMilestoneChunkIndex,
	)
	if err != nil {
		return err
	}
	t.state.LegacyMilestoneChunkIndex = t.state.ChunkCount

	t.state.Progress = 1.0

	for i := range meta.ChunkCount {
		storageChecksum := target.checksums[i]
		legacyStorageChecksum := legacyTarget.checksums[i]
		if storageChecksum != legacyStorageChecksum {
			return errors.NewNonRetriableErrorf(
				"checksum mismatch at index %d: %d != %d",
				i, storageChecksum, legacyStorageChecksum,
			)
		}
	}

	return nil
}

func (t *verifyMigratedLegacySnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *verifyMigratedLegacySnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.VerifyMigratedLegacySnapshotMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *verifyMigratedLegacySnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *verifyMigratedLegacySnapshotTask) runTransferer(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	storage storage.Storage,
	chunkIndex *uint32,
) (*checksumSaverTarget, error) {

	transferer := common.Transferer{
		ReaderCount:         t.config.GetReaderCount(),
		WriterCount:         t.config.GetWriterCount(),
		ChunksInflightLimit: t.config.GetChunksInflightLimit(),
		ChunkSize:           chunkSize,
	}

	source := snapshot.NewSnapshotSource(
		t.request.SnapshotId,
		storage,
	)
	defer source.Close(ctx)

	target := &checksumSaverTarget{checksums: make([]uint32, t.state.ChunkCount)}

	_, err := transferer.Transfer(
		ctx,
		source,
		target,
		common.Milestone{},
		func(ctx context.Context, milestone common.Milestone) error {
			_, err := storage.CheckSnapshotReady(
				ctx,
				t.request.SnapshotId,
			)
			if err != nil {
				return makeNonRetriableErrorsNonSilent(err)
			}

			*chunkIndex = milestone.ChunkIndex
			return t.saveProgress(ctx, execCtx)
		},
	)
	return target, err
}

func (t *verifyMigratedLegacySnapshotTask) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.ChunkCount != 0 {
		milestone := t.state.MilestoneChunkIndex + t.state.LegacyMilestoneChunkIndex
		total := 2 * t.state.ChunkCount
		t.state.Progress = float64(milestone) / float64(total)
	}

	logging.Debug(ctx, "saving state %+v", t.state)
	return execCtx.SaveState(ctx)
}
