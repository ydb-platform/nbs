package dataplane

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url"
	url_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotFromURLTask struct {
	config                    *config.DataplaneConfig
	performanceConfig         *performance_config.PerformanceConfig
	nbsFactory                nbs_client.Factory
	storage                   storage.Storage
	httpClientTimeout         time.Duration
	httpClientMinRetryTimeout time.Duration
	httpClientMaxRetryTimeout time.Duration

	request *protos.CreateSnapshotFromURLRequest
	state   *protos.CreateSnapshotFromURLTaskState
}

func (t *createSnapshotFromURLTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createSnapshotFromURLTask) Load(request, state []byte) error {
	t.request = &protos.CreateSnapshotFromURLRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateSnapshotFromURLTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createSnapshotFromURLTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	_, err := t.storage.CreateSnapshot(
		ctx,
		storage.SnapshotMeta{
			ID: t.request.DstSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	source, err := url.NewURLSource(
		ctx,
		t.httpClientTimeout,
		t.httpClientMinRetryTimeout,
		t.httpClientMaxRetryTimeout,
		t.config.GetHTTPClientMaxRetries(),
		t.request.SrcURL,
		chunkSize,
	)
	if err != nil {
		return err
	}
	defer source.Close(ctx)

	if len(t.state.ETag) == 0 {
		t.state.ETag = source.ETag()

		chunkCount, err := source.ChunkCount(ctx)
		if err != nil {
			return err
		}

		t.state.ChunkCount = chunkCount

		execCtx.SetEstimatedInflightDuration(performance.Estimate(
			uint64(chunkCount)*chunkSize,
			t.performanceConfig.GetCreateSnapshotFromURLBandwidthMiBs(),
		))

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	if t.state.ETag != source.ETag() {
		return url_common.NewSourceOverwrittenError(
			"task with id %v has wrong ETag, expected %v, actual %v",
			selfTaskID,
			t.state.ETag,
			source.ETag(),
		)
	}

	target := snapshot.NewSnapshotTarget(
		selfTaskID,
		t.request.DstSnapshotId,
		t.storage,
		true, // ignoreZeroChunks
		t.request.UseS3,
	)
	defer target.Close(ctx)

	transferer := common.Transferer{
		ReaderCount:         t.config.GetCreateSnapshotFromURLReaderCount(),
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
			err := t.storage.CheckSnapshotAlive(ctx, t.request.DstSnapshotId)
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

	size := uint64(t.state.ChunkCount) * chunkSize
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
		nil,
	)
}

func (t *createSnapshotFromURLTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	_, err := t.storage.DeletingSnapshot(ctx, t.request.DstSnapshotId, execCtx.GetTaskID())
	return err
}

func (t *createSnapshotFromURLTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.CreateSnapshotFromURLMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *createSnapshotFromURLTask) GetResponse() proto.Message {
	return &protos.CreateSnapshotFromURLResponse{
		SnapshotSize:        t.state.SnapshotSize,
		SnapshotStorageSize: t.state.SnapshotStorageSize,
		TransferredDataSize: t.state.TransferredDataSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createSnapshotFromURLTask) saveProgress(
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
