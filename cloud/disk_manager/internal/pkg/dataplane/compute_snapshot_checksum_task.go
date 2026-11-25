package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/checksum"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

// Reads all the chunks of the snapshot and computes its checksum, see
// checksum.State for the format. Does not change the snapshot.
type computeSnapshotChecksumTask struct {
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	storage           storage.Storage
	request           *protos.ComputeSnapshotChecksumRequest
	state             *protos.ComputeSnapshotChecksumTaskState
}

func (t *computeSnapshotChecksumTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *computeSnapshotChecksumTask) Load(request, state []byte) error {
	t.request = &protos.ComputeSnapshotChecksumRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ComputeSnapshotChecksumTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *computeSnapshotChecksumTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.run(ctx, execCtx)

	// The task reads the whole snapshot, so it may work for hours, and
	// transient errors of the storage add up past the retry limit: the task
	// would fail while the snapshot is fine. The progress is saved, so a retry
	// costs little, and retriable errors are retried without a limit.
	var retriableError *errors.RetriableError
	if errors.As(err, &retriableError) && !retriableError.IgnoreRetryLimit {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return err
}

func (t *computeSnapshotChecksumTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotMeta, err := t.checkSnapshotReady(ctx)
	if err != nil {
		return err
	}

	// It is not saved: a ready snapshot does not change.
	chunkCount := snapshotMeta.ChunkCount

	source := snapshot.NewSnapshotSource(t.request.SnapshotId, t.storage)
	defer source.Close(ctx)

	err = t.setEstimate(ctx, execCtx, source)
	if err != nil {
		return err
	}

	// The target does not outlive the attempt, so it is restored from the
	// saved state.
	// The storage returns all the chunks of the chunk map, the ones after the
	// chunk count of the snapshot as well. A restore and a migration would
	// read them too, but a snapshot should not have them: its chunk map is
	// damaged, so the target fails on such a chunk and the task with it.
	target := checksum.NewTarget(
		checksum.State{
			NextChunkIndex: t.state.NextChunkIndex,
			Checksum:       t.state.Checksum,
		},
		chunkCount,
	)
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
		// Chunks before NextChunkIndex are already covered by Checksum.
		common.Milestone{ChunkIndex: t.state.NextChunkIndex},
		func(ctx context.Context, milestone common.Milestone) error {
			_, err := t.checkSnapshotReady(ctx)
			if err != nil {
				return err
			}

			// All the chunks before the milestone are written, so the target
			// takes them into account for good.
			// NextChunkIndex and Checksum should come from the same state of
			// the target, otherwise chunks might be lost or counted twice
			// after a restart.
			targetState := target.GetState(milestone.ChunkIndex)
			t.state.NextChunkIndex = targetState.NextChunkIndex
			t.state.Checksum = targetState.Checksum
			return t.saveProgress(ctx, execCtx, chunkCount)
		},
	)
	if err != nil {
		return err
	}

	// All the chunks are written, so the rest of them is taken into account
	// regardless of the milestone: it might be behind the last chunk.
	// It rarely is, so the tests of this task hardly notice if the rest is
	// not folded: that is checked by the tests of the target, see "finish"
	// cases of TestChecksumTarget.
	targetState := target.Finish()
	t.state.NextChunkIndex = targetState.NextChunkIndex
	t.state.Checksum = targetState.Checksum
	t.state.Progress = 1
	return nil
}

func (t *computeSnapshotChecksumTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *computeSnapshotChecksumTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.ComputeSnapshotChecksumMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *computeSnapshotChecksumTask) GetResponse() proto.Message {
	return &protos.ComputeSnapshotChecksumResponse{
		Checksum: t.state.Checksum,
	}
}

////////////////////////////////////////////////////////////////////////////////

// The storage reports a snapshot that is not ready with a silent error. It is
// not silent here: the task that needs the checksum has seen the snapshot
// ready, so its data should be ready as well.
func (t *computeSnapshotChecksumTask) checkSnapshotReady(
	ctx context.Context,
) (storage.SnapshotMeta, error) {

	snapshotMeta, err := t.storage.CheckSnapshotReady(
		ctx,
		t.request.SnapshotId,
	)

	nonRetriableError := errors.NewEmptyNonRetriableError()
	if errors.As(err, &nonRetriableError) && nonRetriableError.Silent {
		return snapshotMeta, errors.NewNonRetriableError(nonRetriableError.Err)
	}

	return snapshotMeta, err
}

func (t *computeSnapshotChecksumTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	source common.Source,
) error {

	bytesToRead, err := source.EstimatedBytesToRead(ctx)
	if err != nil {
		return err
	}

	estimatedDuration := performance.Estimate(
		bytesToRead,
		t.performanceConfig.GetTransferBetweenDiskAndSnapshotBandwidthMiBs(),
	)
	execCtx.SetEstimatedInflightDuration(estimatedDuration)

	logging.Info(
		ctx,
		"bytes to read is %v, estimated duration is %v",
		bytesToRead,
		estimatedDuration,
	)

	return nil
}

func (t *computeSnapshotChecksumTask) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	chunkCount uint32,
) error {

	if chunkCount != 0 {
		// The milestone might be after the chunk count for a moment if the
		// snapshot has a chunk after it: the task is about to fail on this
		// chunk.
		t.state.Progress = min(
			float64(t.state.NextChunkIndex)/float64(chunkCount),
			1,
		)
	}

	logging.Debug(ctx, "saving state %+v", t.state)
	return execCtx.SaveState(ctx)
}
