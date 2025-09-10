package dataplane

import (
	"context"
	"fmt"
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"google.golang.org/protobuf/types/known/timestamppb"
)

////////////////////////////////////////////////////////////////////////////////

type replicateDiskTask struct {
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	nbsFactory        nbs_client.Factory
	request           *protos.ReplicateDiskTaskRequest
	state             *protos.ReplicateDiskTaskState
}

func (t *replicateDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *replicateDiskTask) Load(request, state []byte) error {
	t.request = &protos.ReplicateDiskTaskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ReplicateDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *replicateDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.setEstimate(ctx, execCtx)
	if err != nil {
		return err
	}

	useLightCheckpoint, err := t.useLightCheckpoint(ctx)
	if err != nil {
		return err
	}

	for {
		if t.state.FinalIteration == 0 {
			if execCtx.HasEvent(
				ctx,
				int64(protos.ReplicateDiskTaskEvents_FINISH_REPLICATION),
			) {
				t.state.FinalIteration = t.state.Iteration + 1

				err := execCtx.SaveState(ctx)
				if err != nil {
					return err
				}
			}
		} else if t.state.Iteration > t.state.FinalIteration {
			return nil
		}

		err = t.updateCheckpoints(ctx, execCtx, useLightCheckpoint)
		if err != nil {
			return err
		}

		t.state.FillSeqNumber++
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}

		err = t.replicate(ctx, execCtx)
		if err != nil {
			return err
		}
	}
}

func (t *replicateDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *replicateDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &protos.ReplicateDiskTaskMetadata{
		Progress:         0,
		SecondsRemaining: math.MaxInt64,
	}

	if t.state.UpdatedAt != metadata.UpdatedAt {
		metadata.Progress = t.state.Progress
		metadata.SecondsRemaining = t.state.SecondsRemaining
		metadata.UpdatedAt = t.state.UpdatedAt
	}

	return metadata, nil
}

func (t *replicateDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *replicateDiskTask) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	bytesToReplicate, err := t.getBytesToReplicate(ctx, execCtx)
	if err != nil {
		return err
	}

	bytesPerSecond := performance.ConvertMiBsToBytes(
		t.performanceConfig.GetTransferFromDiskToDiskBandwidthMiBs(),
	)

	if t.state.ChunkCount != 0 && t.state.Progress != 1 {
		t.state.Progress = float64(t.state.MilestoneChunkIndex) / float64(t.state.ChunkCount)
	}
	t.state.SecondsRemaining = int64(bytesToReplicate / bytesPerSecond)
	t.state.UpdatedAt = timestamppb.Now()

	logging.Debug(ctx, "saving state %+v", t.state)
	return execCtx.SaveState(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (t *replicateDiskTask) checkReplicationProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.Iteration == 0 || t.state.MeasuredSecondsRemaining == 0 {
		t.state.MeasuredSecondsRemaining = math.MaxInt64
		return nil
	}

	var err error
	if t.state.MeasuredSecondsRemaining < t.state.SecondsRemaining {
		err = errors.NewDetailedError(
			errors.New("replication failed"),
			&errors.ErrorDetails{
				Code:     codes.Aborted,
				Message:  "Aborted, because relocation is not expected to finish.",
				Internal: false,
			},
		)
		t.logInfo(
			ctx,
			execCtx,
			"MeasuredSecondsRemaining=%d < SecondsRemaining=%d (it=%d)",
			t.state.MeasuredSecondsRemaining,
			t.state.SecondsRemaining,
			t.state.Iteration,
		)
	}

	if t.config.GetUselessReplicationIterationsBeforeAbort() != 0 &&
		t.state.Iteration%
			t.config.GetUselessReplicationIterationsBeforeAbort() == 0 {

		t.state.MeasuredSecondsRemaining = t.state.SecondsRemaining
		return err
	}

	return nil
}

func (t *replicateDiskTask) useLightCheckpoint(
	ctx context.Context,
) (bool, error) {

	client, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return false, err
	}

	diskParams, err := client.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return false, err
	}

	diskKind := diskParams.Kind

	return diskKind == types.DiskKind_DISK_KIND_SSD_NONREPLICATED ||
		diskKind == types.DiskKind_DISK_KIND_SSD_MIRROR2 ||
		diskKind == types.DiskKind_DISK_KIND_SSD_MIRROR3 ||
		diskKind == types.DiskKind_DISK_KIND_HDD_NONREPLICATED, nil
}

func (t *replicateDiskTask) replicate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t.state.MilestoneChunkIndex = 0
	t.state.Progress = 0

	_, currentCheckpointID, nextCheckpointID := t.getCheckpointIDs(execCtx)

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
		currentCheckpointID,
		nextCheckpointID,
		diskParams.EncryptionDesc,
		chunkSize,
		false, // duplicateChunkIndices
		t.request.IgnoreBaseDisk,
		true, // dontReadFromCheckpoint
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

	target, err := nbs.NewDiskTarget(
		ctx,
		t.nbsFactory,
		t.request.DstDisk,
		diskParams.EncryptionDesc,
		chunkSize,
		false, // ignoreZeroChunks
		t.request.FillGeneration,
		t.state.FillSeqNumber,
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

	_, err = transferer.Transfer(
		ctx,
		source,
		target,
		common.Milestone{ChunkIndex: t.state.MilestoneChunkIndex},
		func(ctx context.Context, milestone common.Milestone) error {
			t.state.MilestoneChunkIndex = milestone.ChunkIndex
			return t.saveProgress(ctx, execCtx)
		},
	)
	if err != nil {
		return err
	}

	err = t.checkReplicationProgress(ctx, execCtx)
	if err != nil {
		return err
	}

	t.state.MilestoneChunkIndex = t.state.ChunkCount
	t.state.Progress = 1
	t.state.Iteration++
	return execCtx.SaveState(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (t *replicateDiskTask) updateCheckpoints(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	isLightCheckpoint bool,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return err
	}

	previousCheckpointID, _, nextCheckpointID := t.getCheckpointIDs(execCtx)

	if len(previousCheckpointID) != 0 {
		err = client.DeleteCheckpoint(
			ctx,
			t.request.SrcDisk.DiskId,
			previousCheckpointID,
		)
		if err != nil {
			return err
		}
	}

	checkpointType := nbs_client.CheckpointTypeWithoutData
	if isLightCheckpoint {
		checkpointType = nbs_client.CheckpointTypeLight
	}

	err = client.CreateCheckpoint(
		ctx,
		nbs_client.CheckpointParams{
			DiskID:         t.request.SrcDisk.DiskId,
			CheckpointID:   nextCheckpointID,
			CheckpointType: checkpointType,
		},
	)
	return err
}

func (t *replicateDiskTask) getCheckpointIDs(
	execCtx tasks.ExecutionContext,
) (previousCheckpointID string, currentCheckpointID string, nextCheckpointID string) {

	if t.state.Iteration >= 2 {
		previousCheckpointID = makeCheckpointID(execCtx, t.state.Iteration-1)
	}
	if t.state.Iteration >= 1 {
		currentCheckpointID = makeCheckpointID(execCtx, t.state.Iteration)
	}
	nextCheckpointID = makeCheckpointID(execCtx, t.state.Iteration+1)

	return previousCheckpointID, currentCheckpointID, nextCheckpointID
}

func makeCheckpointID(
	execCtx tasks.ExecutionContext,
	checkpointIndex uint32,
) string {

	return fmt.Sprintf("replicate_%v_%v", execCtx.GetTaskID(), checkpointIndex)
}

////////////////////////////////////////////////////////////////////////////////

func (t *replicateDiskTask) getBytesToReplicate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (uint64, error) {

	client, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return 0, err
	}

	_, currentCheckpointID, _ := t.getCheckpointIDs(execCtx)
	bytesToReplicate, err := client.GetChangedBytes(
		ctx,
		t.request.SrcDisk.DiskId,
		currentCheckpointID,
		"",
		false, // ignoreBaseDisk
	)
	if err != nil {
		return 0, err
	}

	t.logInfo(
		ctx,
		execCtx,
		"bytes to replicate is %v",
		bytesToReplicate,
	)

	return bytesToReplicate, nil
}

func (t *replicateDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return err
	}

	stats, err := client.Stat(
		ctx,
		t.request.SrcDisk.DiskId,
	)
	if err != nil {
		return err
	}

	execCtx.SetEstimatedInflightDuration(performance.Estimate(
		stats.StorageSize,
		t.performanceConfig.GetTransferFromDiskToDiskBandwidthMiBs(),
	))

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (t *replicateDiskTask) logInfo(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	format string,
	args ...interface{},
) {

	format += t.getAdditionalLogInfo(execCtx)
	logging.Info(ctx, format, args...)
}

func (t *replicateDiskTask) getAdditionalLogInfo(
	execCtx tasks.ExecutionContext,
) string {

	return fmt.Sprintf(". TaskID: %v", execCtx.GetTaskID())
}
