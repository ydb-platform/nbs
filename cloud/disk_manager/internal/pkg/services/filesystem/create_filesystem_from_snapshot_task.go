package filesystem

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	filesystem_snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createFilesystemFromSnapshotTask struct {
	storage      resources.Storage
	factory      nfs.Factory
	scheduler    tasks.Scheduler
	cellSelector cells.CellSelector
	request      *protos.CreateFilesystemFromSnapshotRequest
	state        *protos.CreateFilesystemFromSnapshotTaskState
}

func (t *createFilesystemFromSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createFilesystemFromSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.CreateFilesystemFromSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateFilesystemFromSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createFilesystemFromSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	params := t.request.Params

	client, err := SelectCellForFilesystem(
		ctx,
		execCtx,
		t.state,
		params,
		t.cellSelector,
		t.factory,
	)
	if err != nil {
		return err
	}
	defer client.Close()

	selfTaskID := execCtx.GetTaskID()

	filesystemMeta, err := t.storage.CreateFilesystem(ctx, resources.FilesystemMeta{
		ID:            params.Filesystem.FilesystemId,
		ZoneID:        client.ZoneID(),
		SrcSnapshotID: t.request.SrcSnapshotId,
		BlocksCount:   params.BlocksCount,
		BlockSize:     params.BlockSize,
		Kind:          fsKindToString(params.Kind),
		CloudID:       params.CloudId,
		FolderID:      params.FolderId,

		CreateRequest: t.request,
		CreateTaskID:  selfTaskID,
		CreatingAt:    time.Now(),
		CreatedBy:     "", // TODO: Extract CreatedBy from execCtx
		IsExternal:    false,
	})
	if err != nil {
		return err
	}

	if filesystemMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			params.Filesystem.FilesystemId,
		)
	}

	err = client.Create(
		ctx,
		params.Filesystem.FilesystemId,
		nfs.CreateFilesystemParams{
			CloudID:     params.CloudId,
			FolderID:    params.FolderId,
			BlocksCount: params.BlocksCount,
			BlockSize:   params.BlockSize,
			Kind:        params.Kind,
		},
	)
	if err != nil {
		return err
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_transfer_from_snapshot"),
		"dataplane.TransferFromSnapshotToFilesystem",
		"",
		&filesystem_snapshot_protos.TransferFromSnapshotToFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       filesystemMeta.ZoneID,
				FilesystemId: filesystemMeta.ID,
			},
			SnapshotId: t.request.SrcSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	t.state.TransferFromSnapshotTaskId = taskID

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	filesystemMeta.CreatedAt = time.Now()
	return t.storage.FilesystemCreated(ctx, *filesystemMeta)
}

func (t *createFilesystemFromSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	params := t.request.Params

	client, err := SelectCellForFilesystem(
		ctx,
		execCtx,
		t.state,
		params,
		t.cellSelector,
		t.factory,
	)
	if err != nil {
		return err
	}
	defer client.Close()

	selfTaskID := execCtx.GetTaskID()

	fs, err := t.storage.DeleteFilesystem(
		ctx,
		params.Filesystem.FilesystemId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if fs == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			params.Filesystem.FilesystemId,
		)
	}

	err = client.Delete(ctx, params.Filesystem.FilesystemId, false)
	if err != nil {
		return err
	}

	return t.storage.FilesystemDeleted(
		ctx,
		params.Filesystem.FilesystemId,
		time.Now(),
	)
}

func (t *createFilesystemFromSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *createFilesystemFromSnapshotTask) GetResponse() proto.Message {
	return &disk_manager.CreateFilesystemResponse{}
}
