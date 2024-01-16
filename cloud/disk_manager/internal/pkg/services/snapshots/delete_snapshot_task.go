package snapshots

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type deleteSnapshotTask struct {
	scheduler  tasks.Scheduler
	storage    resources.Storage
	nbsFactory nbs.Factory
	request    *protos.DeleteSnapshotRequest
	state      *protos.DeleteSnapshotTaskState
}

func (t *deleteSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.DeleteSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteSnapshotTask) deleteSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.DeleteSnapshot(
		ctx,
		t.request.SnapshotId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if snapshotMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.SnapshotId,
		)
	}

	// NBS-3535.
	if snapshotMeta.UseDataplaneTasks && len(snapshotMeta.BaseSnapshotID) != 0 {
		err := t.storage.UnlockSnapshot(
			ctx,
			snapshotMeta.BaseSnapshotID,
			snapshotMeta.CreateTaskID,
		)
		if err != nil {
			return err
		}

		logging.Debug(
			ctx,
			"Successfully unlocked snapshot with id %v",
			snapshotMeta.BaseSnapshotID,
		)
	}

	if len(snapshotMeta.CheckpointID) != 0 {
		nbsClient, err := t.nbsFactory.GetClient(ctx, snapshotMeta.Disk.ZoneId)
		if err != nil {
			return err
		}

		err = nbsClient.DeleteCheckpoint(
			ctx,
			snapshotMeta.Disk.DiskId,
			snapshotMeta.CheckpointID,
		)
		if err != nil {
			return err
		}
	}

	// Hack for NBS-2225.
	if snapshotMeta.DeleteTaskID != selfTaskID {
		return t.scheduler.WaitTaskEnded(ctx, snapshotMeta.DeleteTaskID)
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID),
		"dataplane.DeleteSnapshot",
		"",
		&dataplane_protos.DeleteSnapshotRequest{
			SnapshotId: t.request.SnapshotId,
		},
		t.request.OperationCloudId,
		t.request.OperationFolderId,
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.storage.SnapshotDeleted(ctx, t.request.SnapshotId, time.Now())
}

func (t *deleteSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteSnapshot(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteSnapshot(ctx, execCtx)
}

func (t *deleteSnapshotTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &disk_manager.DeleteSnapshotMetadata{
		SnapshotId: t.request.SnapshotId,
	}, nil
}

func (t *deleteSnapshotTask) GetResponse() proto.Message {

	// TODO: Fill response with data.
	return &disk_manager.DeleteSnapshotResponse{}
}
