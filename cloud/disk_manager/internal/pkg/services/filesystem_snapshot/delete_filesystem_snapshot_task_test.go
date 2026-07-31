package filesystem_snapshot

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem_snapshot/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteFilesystemSnapshotTask(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := newExecutionContextMock()

	task := &deleteFilesystemSnapshotTask{
		storage:   storage,
		scheduler: scheduler,
		request: &protos.DeleteFilesystemSnapshotRequest{
			SnapshotId: "snapshot",
		},
		state: &protos.DeleteFilesystemSnapshotTaskState{},
	}

	storage.On(
		"DeleteFilesystemSnapshot",
		ctx,
		"snapshot",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.FilesystemSnapshotMeta{
		ID:           "snapshot",
		DeleteTaskID: "toplevel_task_id",
	}, nil)
	scheduler.On(
		"ScheduleTask",
		headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id"),
		"dataplane.DeleteFilesystemSnapshot",
		"",
		&dataplane_protos.DeleteFilesystemSnapshotRequest{
			SnapshotId: "snapshot",
		},
	).Return("delete_dataplane", nil)
	scheduler.On(
		"WaitTask",
		ctx,
		execCtx,
		"delete_dataplane",
	).Return(nil, nil)
	storage.On(
		"FilesystemSnapshotDeleted",
		ctx,
		"snapshot",
		mock.Anything,
	).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
	require.NoError(t, err)
}

func TestDeleteFilesystemSnapshotTaskWaitsForConcurrentDeletion(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := newExecutionContextMock()

	task := &deleteFilesystemSnapshotTask{
		storage:   storage,
		scheduler: scheduler,
		request: &protos.DeleteFilesystemSnapshotRequest{
			SnapshotId: "snapshot",
		},
		state: &protos.DeleteFilesystemSnapshotTaskState{},
	}

	storage.On(
		"DeleteFilesystemSnapshot",
		ctx,
		"snapshot",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.FilesystemSnapshotMeta{
		ID:           "snapshot",
		DeleteTaskID: "other_task_id",
	}, nil)
	scheduler.On(
		"WaitTaskEnded",
		ctx,
		"other_task_id",
	).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
	require.NoError(t, err)
}
