package filesystembackups

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystembackups/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createFilesystemBackupFromFilesystemTask struct {
	scheduler  tasks.Scheduler
	storage    resources.Storage
	nfsFactory nfs.Factory
	request    *protos.CreateFilesystemBackupFromFilesystemRequest
	state      *protos.CreateFilesystemBackupFromFilesystemTaskState
}

func (t *createFilesystemBackupFromFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createFilesystemBackupFromFilesystemTask) Load(request, state []byte) error {
	t.request = &protos.CreateFilesystemBackupFromFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateFilesystemBackupFromFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createFilesystemBackupFromFilesystemTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nfsClient nfs.Client,
) (string, error) {

	filesystem := t.request.SrcFilesystem

	selfTaskID := execCtx.GetTaskID()

	filesystemParams, err := nfsClient.DescribeFilesystem(ctx, filesystem.FilesystemId)
	if err != nil {
		return "", err
	}

	filesystemBackupMeta, err := t.storage.CreateFilesystemBackup(ctx, resources.FilesystemBackupMeta{
		ID:                t.request.DstFilesystemBackupId,
		FolderID:          t.request.FolderId,
		Filesystem:        filesystem,
		CreateRequest:     t.request,
		CreateTaskID:      selfTaskID,
		CreatingAt:        time.Now(),
		CreatedBy:         "",
		UseDataplaneTasks: true,
	})
	if err != nil {
		return "", err
	}

	if filesystemBackupMeta.Ready {
		// Already created.
		return filesystemBackupMeta.CheckpointID, nil
	}

	checkpointID, err := common.CreateCheckpointFilesystem(
		ctx,
		execCtx,
		t.scheduler,
		nfsClient,
		t.request.SrcFilesystem,
		t.request.DstFilesystemBackupId,
		selfTaskID,
		filesystemParams.IsDiskRegistryBasedFilesystem,
		t.request.RetryBrokenDRBasedDiskCheckpoint,
	)
	if err != nil {
		return "", err
	}

	taskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateFilesystemBackupFromFilesystem",
		"",
		filesystem.ZoneId,
		&dataplane_protos.CreateFilesystemBackupFromFilesystemRequest{
			SrcFilesystem:             filesystem,
			SrcFilesystemCheckpointId: checkpointID,
			DstFilesystemBackupId:     t.request.DstFilesystemBackupId,
			UseS3:                     t.request.UseS3,
			UseProxyOverlayDisk:       t.request.UseProxyOverlayDisk,
		},
	)
	if err != nil {
		return "", err
	}

	t.state.DataplaneTaskID = taskID

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return "", err
	}

	typedResponse, ok := response.(*dataplane_protos.CreateFilesystemBackupFromFilesystemResponse)
	if !ok {
		return "", errors.NewNonRetriableErrorf(
			"invalid dataplane.CreateFilesystemBackupFromFilesystem response type %T",
			response,
		)
	}

	t.state.FilesystemBackupSize = int64(typedResponse.FilesystemBackupSize)
	t.state.FilesystemBackupStorageSize = int64(typedResponse.FilesystemBackupStorageSize)

	err = execCtx.SaveState(ctx)
	if err != nil {
		return "", err
	}

	err = t.storage.FilesystemBackupCreated(
		ctx,
		t.request.DstFilesystemBackupId,
		checkpointID,
		time.Now(),
		uint64(t.state.FilesystemBackupSize),
		uint64(t.state.FilesystemBackupStorageSize),
	)
	if err != nil {
		return "", err
	}

	return checkpointID, nil
}

func (t *createFilesystemBackupFromFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem := t.request.SrcFilesystem

	nfsClient, err := t.nfsFactory.NewClient(ctx, filesystem.ZoneId)
	if err != nil {
		return err
	}

	checkpointID, err := t.run(ctx, execCtx, nfsClient)
	if err != nil {
		return err
	}

	filesystemParams, err := nfsClient.DescribeFilesystem(ctx, t.request.SrcFilesystem.FilesystemId)
	if err != nil {
		return err
	}

	if filesystemParams.IsDiskRegistryBasedFilesystem {
		return nfsClient.DeleteCheckpointFilesystem(ctx, filesystem.FilesystemId, checkpointID)
	}

	return nfsClient.DeleteCheckpointDataFilesystem(ctx, filesystem.FilesystemId, checkpointID)
}

func (t *createFilesystemBackupFromFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem := t.request.SrcFilesystem
	nbsClient, err := t.nfsFactory.GetClient(ctx, t.request.SrcFilesystem.ZoneId)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	checkpointID, err := common.CancelCheckpointCreationFilesystem(
		ctx,
		t.scheduler,
		nbsClient,
		filesystem,
		t.request.DstFilesystemBackupId,
		selfTaskID,
		t.request.RetryBrokenDRBasedDiskCheckpoint,
	)
	if err != nil {
		return err
	}

	if checkpointID != "" {
		err = nbsClient.DeleteCheckpointFilesystem(ctx, filesystem.FilesystemId, checkpointID)
		if err != nil {
			return err
		}
	}

	filesystemBackupMeta, err := t.storage.DeleteFilesystemBackup(
		ctx,
		t.request.DstFilesystemBackupId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if filesystemBackupMeta == nil {
		// Nothing to do.
		return nil
	}

	// Hack for NBS-2225.
	if filesystemBackupMeta.DeleteTaskID != selfTaskID {
		return t.scheduler.WaitTaskEnded(ctx, filesystemBackupMeta.DeleteTaskID)
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_cancel"),
		"dataplane.DeleteFilesystemBackup",
		"",
		&dataplane_protos.DeleteFilesystemBackupRequest{
			FilesystemBackupId: t.request.DstFilesystemBackupId,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.storage.FilesystemBackupDeleted(ctx, t.request.DstFilesystemBackupId, time.Now())
}

func (t *createFilesystemBackupFromFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.CreateFilesystemBackupMetadata{}

	if len(t.state.DataplaneTaskID) != 0 {
		message, err := t.scheduler.GetTaskMetadata(
			ctx,
			t.state.DataplaneTaskID,
		)
		if err != nil {
			return nil, err
		}

		createMetadata, ok := message.(*dataplane_protos.CreateFilesystemBackupFromFilesystemMetadata)
		if ok {
			metadata.Progress = createMetadata.Progress
		}
	} else {
		metadata.Progress = t.state.Progress
	}

	return metadata, nil
}

func (t *createFilesystemBackupFromFilesystemTask) GetResponse() proto.Message {
	return &disk_manager.CreateFilesystemBackupResponse{
		Size:        t.state.FilesystemBackupSize,
		StorageSize: t.state.FilesystemBackupStorageSize,
	}
}
