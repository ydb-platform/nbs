package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type transferFromSnapshotToDiskTask struct {
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	nbsFactory        nbs_client.Factory
	storage           storage.Storage
	request           *protos.TransferFromSnapshotToDiskRequest
	state             *protos.TransferFromSnapshotToDiskTaskState
}

func (t *transferFromSnapshotToDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *transferFromSnapshotToDiskTask) Load(request, state []byte) error {
	t.request = &protos.TransferFromSnapshotToDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.TransferFromSnapshotToDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *transferFromSnapshotToDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	srcMeta, err := t.storage.CheckSnapshotReady(ctx, t.request.SrcSnapshotId)
	if err != nil {
		return err
	}

	t.state.ChunkCount = srcMeta.ChunkCount

	source := snapshot.NewSnapshotSource(t.request.SrcSnapshotId, t.storage)
	defer source.Close(ctx)

	bytesToTransfer, err := source.EstimatedBytesToRead(ctx)
	if err != nil {
		return err
	}

	execCtx.SetEstimatedInflightDuration(performance.Estimate(
		bytesToTransfer,
		t.performanceConfig.GetTransferBetweenDiskAndSnapshotBandwidthMiBs(),
	))

	target, err := nbs.NewDiskTarget(
		ctx,
		t.nbsFactory,
		t.request.DstDisk,
		t.request.DstEncryption,
		chunkSize,
		false, // ignoreZeroChunks
		0,     // fillGeneration
		0,     // fillSeqNumber
	)
	if err != nil {
		return err
	}
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
		common.Milestone{ChunkIndex: t.state.MilestoneChunkIndex},
		func(ctx context.Context, milestone common.Milestone) error {
			_, err := t.storage.CheckSnapshotReady(ctx, t.request.SrcSnapshotId)
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
	return nil
}

func (t *transferFromSnapshotToDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *transferFromSnapshotToDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.TransferFromSnapshotToDiskMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *transferFromSnapshotToDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *transferFromSnapshotToDiskTask) saveProgress(
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
