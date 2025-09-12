package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotFromSnapshotTask struct {
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	storage           storage.Storage
	request           *protos.CreateSnapshotFromSnapshotRequest
	state             *protos.CreateSnapshotFromSnapshotTaskState
}

func (t *createSnapshotFromSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createSnapshotFromSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.CreateSnapshotFromSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateSnapshotFromSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createSnapshotFromSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	srcMeta, err := t.storage.CheckSnapshotReady(ctx, t.request.SrcSnapshotId)
	if err != nil {
		return err
	}

	t.state.ChunkCount = srcMeta.ChunkCount

	execCtx.SetEstimatedInflightDuration(performance.Estimate(
		srcMeta.StorageSize,
		t.performanceConfig.GetSnapshotShallowCopyBandwidthMiBs(),
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

	err = t.storage.ShallowCopySnapshot(
		ctx,
		t.request.SrcSnapshotId,
		t.request.DstSnapshotId,
		t.state.MilestoneChunkIndex,
		func(ctx context.Context, milestoneChunkIndex uint32) error {
			_, err := t.storage.CheckSnapshotReady(ctx, t.request.SrcSnapshotId)
			if err != nil {
				return err
			}

			err = t.storage.CheckSnapshotAlive(ctx, t.request.DstSnapshotId)
			if err != nil {
				return err
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
	t.state.SnapshotSize = srcMeta.Size
	t.state.SnapshotStorageSize = srcMeta.StorageSize

	return t.storage.SnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		srcMeta.Size,
		srcMeta.StorageSize,
		srcMeta.ChunkCount,
		srcMeta.Encryption,
	)
}

func (t *createSnapshotFromSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	_, err := t.storage.DeletingSnapshot(ctx, t.request.DstSnapshotId, execCtx.GetTaskID())
	return err
}

func (t *createSnapshotFromSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.CreateSnapshotFromSnapshotMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *createSnapshotFromSnapshotTask) GetResponse() proto.Message {
	return &protos.CreateSnapshotFromSnapshotResponse{
		SnapshotSize:        t.state.SnapshotSize,
		SnapshotStorageSize: t.state.SnapshotStorageSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createSnapshotFromSnapshotTask) saveProgress(
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
