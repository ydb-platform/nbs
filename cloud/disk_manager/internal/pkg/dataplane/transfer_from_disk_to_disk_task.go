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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type transferFromDiskToDiskTask struct {
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	nbsFactory        nbs_client.Factory
	request           *protos.TransferFromDiskToDiskRequest
	state             *protos.TransferFromDiskToDiskTaskState
}

func (t *transferFromDiskToDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *transferFromDiskToDiskTask) Load(request, state []byte) error {
	t.request = &protos.TransferFromDiskToDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.TransferFromDiskToDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *transferFromDiskToDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.setEstimate(ctx, execCtx)
	if err != nil {
		return err
	}

	client, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return err
	}

	diskParams, err := client.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return err
	}

	source, err := nbs.NewDiskSource(
		ctx,
		client,
		t.request.SrcDisk.DiskId,
		"", // proxyDiskID
		t.request.SrcDiskBaseCheckpointId,
		t.request.SrcDiskCheckpointId,
		diskParams.EncryptionDesc,
		chunkSize,
		false, // duplicateChunkIndices
		false, // ignoreBaseDisk
		false, // dontReadFromCheckpoint
	)
	if err != nil {
		return err
	}
	defer source.Close(ctx)

	chunkCount, err := source.ChunkCount(ctx)
	if err != nil {
		return err
	}

	t.state.ChunkCount = chunkCount

	ignoreZeroChunks := true

	target, err := nbs.NewDiskTarget(
		ctx,
		t.nbsFactory,
		t.request.DstDisk,
		diskParams.EncryptionDesc,
		chunkSize,
		ignoreZeroChunks,
		t.request.FillGeneration,
		t.request.FillSeqNumber,
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

func (t *transferFromDiskToDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *transferFromDiskToDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.TransferFromDiskToDiskMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *transferFromDiskToDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *transferFromDiskToDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	nbsClient, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return err
	}

	bytesToTransfer, err := nbsClient.GetChangedBytes(
		ctx,
		t.request.SrcDisk.DiskId,
		t.request.SrcDiskBaseCheckpointId,
		t.request.SrcDiskCheckpointId,
		false, // ignoreBaseDisk
	)
	if err != nil {
		if !nbs_client.IsGetChangedBlocksNotSupportedError(err) {
			return err
		}

		diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
		if err != nil {
			return err
		}

		bytesToTransfer = diskParams.BlocksCount * uint64(diskParams.BlockSize)
	}

	logging.Info(ctx, "bytes to transfer is %v", bytesToTransfer)

	execCtx.SetEstimatedInflightDuration(performance.Estimate(
		bytesToTransfer,
		t.performanceConfig.GetTransferFromDiskToDiskBandwidthMiBs(),
	))

	return nil
}

func (t *transferFromDiskToDiskTask) saveProgress(
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
