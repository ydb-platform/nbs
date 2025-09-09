package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotFromLegacySnapshotTask struct {
	storage           storage.Storage
	legacyStorage     storage.Storage
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	request           *protos.CreateSnapshotFromLegacySnapshotRequest
	state             *protos.CreateSnapshotFromLegacySnapshotTaskState
}

func (t *createSnapshotFromLegacySnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createSnapshotFromLegacySnapshotTask) Load(
	request, state []byte,
) error {

	t.request = &protos.CreateSnapshotFromLegacySnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateSnapshotFromLegacySnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createSnapshotFromLegacySnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	srcMeta, err := t.legacyStorage.CheckSnapshotReady(
		ctx,
		t.request.SrcSnapshotId,
	)
	if err != nil {
		return err
	}

	t.state.ChunkCount = srcMeta.ChunkCount

	execCtx.SetEstimatedInflightDuration(performance.Estimate(
		srcMeta.StorageSize,
		t.performanceConfig.GetCreateSnapshotFromLegacySnapshotBandwidthMiBs(),
	))

	_, err = t.storage.CreateSnapshot(
		ctx,
		storage.SnapshotMeta{
			ID: t.request.DstSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	source := snapshot.NewSnapshotSource(
		t.request.SrcSnapshotId,
		t.legacyStorage,
	)
	defer source.Close(ctx)

	ignoreZeroChunks := true

	target := snapshot.NewSnapshotTarget(
		execCtx.GetTaskID(),
		t.request.DstSnapshotId,
		t.storage,
		ignoreZeroChunks,
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
			_, err := t.legacyStorage.CheckSnapshotReady(
				ctx,
				t.request.SrcSnapshotId,
			)
			if err != nil {
				return err
			}

			err = t.storage.CheckSnapshotAlive(ctx, t.request.DstSnapshotId)
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

	dataChunkCount, err := t.storage.GetDataChunkCount(
		ctx,
		t.request.DstSnapshotId,
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

	return t.storage.SnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		size,
		storageSize,
		t.state.ChunkCount,
		srcMeta.Encryption,
	)
}

func (t *createSnapshotFromLegacySnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	_, err := t.storage.DeletingSnapshot(ctx, t.request.DstSnapshotId, execCtx.GetTaskID())
	return err
}

func (t *createSnapshotFromLegacySnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.CreateSnapshotFromLegacySnapshotMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *createSnapshotFromLegacySnapshotTask) GetResponse() proto.Message {
	return &protos.CreateSnapshotFromLegacySnapshotResponse{
		SnapshotSize:        t.state.SnapshotSize,
		SnapshotStorageSize: t.state.SnapshotStorageSize,
		TransferredDataSize: t.state.TransferredDataSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createSnapshotFromLegacySnapshotTask) saveProgress(
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
