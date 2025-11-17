package filesystembackups

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_backups/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystemBackupTask struct {
	scheduler  tasks.Scheduler
	storage    resources.Storage
	nfsFactory nfs.Factory
	request    *protos.DeleteFilesystemBackupRequest
	state      *protos.DeleteFilesystemBackupTaskState
}

func (t *deleteFilesystemBackupTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteFilesystemBackupTask) Load(request, state []byte) error {
	t.request = &protos.DeleteFilesystemBackupRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteFilesystemBackupTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteFilesystemBackupTask) deleteFilesystemBackup(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	filesystemBackupMeta, err := t.storage.DeleteFilesystemBackup(
		ctx,
		t.request.FilesystemBackupId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if filesystemBackupMeta == nil {
		// Should be idempotent.
		return nil
	}

	// If case of concurrent deletion we should wait for the first deletion task to complete.
	if filesystemBackupMeta.DeleteTaskID != selfTaskID {
		_, err := t.scheduler.WaitTask(ctx, execCtx, filesystemBackupMeta.DeleteTaskID)
		return err
	}

	// TODO: Implement dataplane.DeleteFilesystemBackup task
	// For now, we just mark the backup as deleted in storage.
	// The actual data deletion should be handled by a dataplane task similar to DeleteSnapshot.
	// This will require:
	// 1. Creating dataplane/protos/delete_filesystem_backup_task.proto
	// 2. Implementing dataplane delete_filesystem_backup_task.go
	// 3. Scheduling the task here with:
	//    taskID, err := t.scheduler.ScheduleTask(
	//        headers.SetIncomingIdempotencyKey(ctx, selfTaskID),
	//        "dataplane.DeleteFilesystemBackup",
	//        "",
	//        &dataplane_protos.DeleteFilesystemBackupRequest{
	//            FilesystemBackupId: t.request.FilesystemBackupId,
	//        },
	//    )
	//    if err != nil {
	//        return err
	//    }
	//    _, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	//    if err != nil {
	//        return err
	//    }

	return t.storage.FilesystemBackupDeleted(ctx, t.request.FilesystemBackupId, time.Now())
}

func (t *deleteFilesystemBackupTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteFilesystemBackup(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteFilesystemBackupTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteFilesystemBackup(ctx, execCtx)
}

func (t *deleteFilesystemBackupTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeleteFilesystemBackupMetadata{
		FilesystemBackupId: t.request.FilesystemBackupId,
	}, nil
}

func (t *deleteFilesystemBackupTask) GetResponse() proto.Message {

	// TODO: Fill response with data about changed filesystem backups.
	return &disk_manager.DeleteFilesystemBackupResponse{}
}
