package filesystem_snapshot

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createFilesystemSnapshotTask struct {
	scheduler    tasks.Scheduler
	cellSelector cells.CellSelector
	storage      resources.Storage
	request      *protos.CreateFilesystemSnapshotRequest
	state        *protos.CreateFilesystemSnapshotTaskState
}

func (t *createFilesystemSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createFilesystemSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.CreateFilesystemSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateFilesystemSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createFilesystemSnapshotTask) getSourceFilesystem(
	ctx context.Context,
) (*types.Filesystem, error) {

	filesystem := t.request.SrcFilesystem
	filesystemMeta, err := t.storage.GetFilesystemMeta(
		ctx,
		filesystem.FilesystemId,
	)
	if err != nil {
		return nil, err
	}

	if filesystemMeta == nil {
		return nil, errors.NewNonCancellableErrorf(
			"no such filesystem: %v",
			filesystem.FilesystemId,
		)
	}

	if filesystemMeta.ZoneID != filesystem.ZoneId &&
		(t.cellSelector == nil ||
			!t.cellSelector.ZoneContainsCell(filesystem.ZoneId, filesystemMeta.ZoneID)) {

		return nil, errors.NewNonCancellableErrorf(
			"filesystem %s is not in zone %s",
			filesystem.FilesystemId,
			filesystem.ZoneId,
		)
	}

	return &types.Filesystem{
		ZoneId:       filesystemMeta.ZoneID,
		FilesystemId: filesystem.FilesystemId,
	}, nil
}

func (t *createFilesystemSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem, err := t.getSourceFilesystem(ctx)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.CreateFilesystemSnapshot(
		ctx,
		resources.FilesystemSnapshotMeta{
			ID:            t.request.DstSnapshotId,
			FolderID:      t.request.FolderId,
			Filesystem:    filesystem,
			CreateRequest: t.request,
			CreateTaskID:  selfTaskID,
			CreatingAt:    time.Now(),
		},
	)
	if err != nil {
		return err
	}

	if snapshotMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.DstSnapshotId,
		)
	}

	if snapshotMeta.Ready {
		return nil
	}

	// TODO: (jkuradobery) Create fs checkpoint once checkpoints
	// are implemented on the  filestore side.
	taskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateSnapshotFromFilesystem",
		"",
		filesystem.ZoneId,
		&dataplane_protos.CreateFilesystemSnapshotRequest{
			Filesystem:   filesystem,
			CheckpointId: "",
			SnapshotId:   t.request.DstSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	t.state.DataplaneTaskID = taskID

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	// TODO: (jkuradobery) pass actual storage size once data backup is implemented.
	// See: https://github.com/ydb-platform/nbs/issues/1559
	return t.storage.FilesystemSnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		time.Now(),
		0,
		0,
	)
}

func (t *createFilesystemSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.DeleteFilesystemSnapshot(
		ctx,
		t.request.DstSnapshotId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if snapshotMeta == nil {
		// Nothing to do.
		return nil
	}

	if snapshotMeta.DeleteTaskID != selfTaskID {
		return t.scheduler.WaitTaskEnded(ctx, snapshotMeta.DeleteTaskID)
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_cancel"),
		"dataplane.DeleteFilesystemSnapshot",
		"",
		&dataplane_protos.DeleteFilesystemSnapshotRequest{
			SnapshotId: t.request.DstSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.storage.FilesystemSnapshotDeleted(
		ctx,
		t.request.DstSnapshotId,
		time.Now(),
	)
}

func (t *createFilesystemSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.CreateFilesystemSnapshotMetadata{}

	return metadata, nil
}

func (t *createFilesystemSnapshotTask) GetResponse() proto.Message {
	return &disk_manager.CreateFilesystemSnapshotResponse{}
}
