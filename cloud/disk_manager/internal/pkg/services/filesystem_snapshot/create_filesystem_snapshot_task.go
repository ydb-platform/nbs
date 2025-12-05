package filesystem_snapshot

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/headers"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type createFilesystemSnapshotTask struct {
	scheduler    tasks.Scheduler
	cellSelector cells.CellSelector
	storage    resources.Storage
	nfsFactory nfs.Factory
	request    *protos.CreateFilesystemSnapshotRequest
	state      *protos.CreateFilesystemSnapshotTaskState
}

func (t *createFilesystemSnapshotTask) Save() ([]byte, error) {
	return []byte{}, nil
}

func (t *createFilesystemSnapshotTask) Load(request, state []byte) error {
	return nil
}

func (t *createFilesystemSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.CreateFilesystemSnapshot(
		ctx,
		&disk_manager.FilesystemSnapshotMeta{
			ID:           t.request.DstSnapshotId,
			FolderID:     t.request.FolderId,
			Filesystem:   t.request.SrcFilesystem,
			CreateTaskID: selfTaskID,
			CreatingAt:   time.Now(),
		},
	)
	if err != nil {
		return err
	}

	if snapshotMeta.Ready {
		return nil
	}

	filesystemID := snapshotMeta.Filesystem.FilesystemId
	zoneID := snapshotMeta.Filesystem.ZoneId
	client, err := t.nfsFactory.NewClient(ctx, zoneID)
	if err != nil {
		return err
	}

	createCheckpointSession, err := client.CreateSession(
		ctx,
		filesystemID,
		"",
		false,
	)
	if err != nil {
		return err
	}
	defer createCheckpointSession.Close(ctx)

	err = client.CreateCheckpoint(
		ctx,
		createCheckpointSession,
		filesystemID,
		t.request.DstSnapshotId,
		t.request.NodeId,
	)
	if err != nil {
		return err
	}
	taskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateFilesystemSnapshotMetadata",
		"",
		zoneID,
		&empty.Empty{},
	)
	if err != nil {
		return err
	}

	t.state.MetadataBackupTaskId = taskID

	t.scheduler.WaitTask(ctx, execCtx, taskID)

	// todo here process the response
	// todo add data migration task
	err = t.storage.FilesystemSnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		time.Now(),
		uint64(t.state.SnapshotSize),
		uint64(t.state.SnapshotStorageSize),
	)
}

func (t *createFilesystemSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
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
