package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deleteSnapshotDataTask struct {
	performanceConfig *performance_config.PerformanceConfig
	storage           storage.Storage
	request           *protos.DeleteSnapshotDataRequest
	state             *protos.DeleteSnapshotDataTaskState
}

func (t *deleteSnapshotDataTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteSnapshotDataTask) Load(request, state []byte) error {
	t.request = &protos.DeleteSnapshotDataRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteSnapshotDataTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteSnapshotDataTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteSnapshotData(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteSnapshotDataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteSnapshotData(ctx, execCtx)
}

func (t *deleteSnapshotDataTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteSnapshotDataTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *deleteSnapshotDataTask) deleteSnapshotData(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotMeta, err := t.storage.CheckSnapshotReady(ctx, t.request.SnapshotId)
	if err != nil {
		return err
	}

	// Shallow copy means creating references for chunks.
	// DeleteSnapshotData deletes references, so the bandwidth is the same.
	execCtx.SetEstimatedInflightDuration(performance.Estimate(
		snapshotMeta.StorageSize,
		t.performanceConfig.GetSnapshotShallowCopyBandwidthMiBs(),
	))

	return t.storage.DeleteSnapshotData(ctx, t.request.SnapshotId)
}
