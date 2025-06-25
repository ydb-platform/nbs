package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteFilesystemTaskRun(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	filesystem := &protos.FilesystemId{ZoneId: "zone", FilesystemId: "filesystem"}
	request := &protos.DeleteFilesystemRequest{Filesystem: filesystem}

	task := &deleteFilesystemTask{
		storage: storage,
		factory: nfsFactory,
		request: request,
		state:   &protos.DeleteFilesystemTaskState{},
	}

	storage.On(
		"DeleteFilesystem",
		ctx,
		"filesystem",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.FilesystemMeta{
		DeleteTaskID: "toplevel_task_id",
	}, nil)
	storage.On("FilesystemDeleted", ctx, "filesystem", mock.Anything).Return(nil)

	nfsFactory.On("NewClient", ctx, "zone").Return(nfsClient, nil)
	nfsClient.On("Delete", ctx, "filesystem").Return(nil)
	nfsClient.On("Close").Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, nfsFactory, nfsClient, execCtx)
	require.NoError(t, err)
}

func TestDeleteFilesystemTaskCancel(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	filesystem := &protos.FilesystemId{ZoneId: "zone", FilesystemId: "filesystem"}
	request := &protos.DeleteFilesystemRequest{Filesystem: filesystem}

	task := &deleteFilesystemTask{
		storage: storage,
		factory: nfsFactory,
		request: request,
		state:   &protos.DeleteFilesystemTaskState{},
	}

	storage.On(
		"DeleteFilesystem",
		ctx,
		"filesystem",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.FilesystemMeta{
		DeleteTaskID: "toplevel_task_id",
	}, nil)
	storage.On("FilesystemDeleted", ctx, "filesystem", mock.Anything).Return(nil)

	nfsFactory.On("NewClient", ctx, "zone").Return(nfsClient, nil)
	nfsClient.On("Delete", ctx, "filesystem").Return(nil)
	nfsClient.On("Close").Return(nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, nfsFactory, nfsClient, execCtx)
	require.NoError(t, err)
}

func TestDeleteExternalFilesystemTaskRun(t *testing.T) {
	ctx := context.Background()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	filesystem := &protos.FilesystemId{ZoneId: "zone", FilesystemId: "filesystem"}
	request := &protos.DeleteFilesystemRequest{Filesystem: filesystem}

	task := &deleteFilesystemTask{
		storage:   storage,
		factory:   nfsFactory,
		scheduler: scheduler,
		request:   request,
		state:     &protos.DeleteFilesystemTaskState{},
	}

	externalTask := &deleteExternalFilesystemTask{
		storage: storage,
		factory: nfsFactory,
		request: request,
		state:   &protos.DeleteFilesystemTaskState{},
	}

	scheduler.On("WaitTask", mock.Anything, mock.Anything).Return(mock.Anything, externalTask.Run(ctx, execCtx))
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"filesystem.DeleteExternalFilesystem",
		mock.Anything,
		mock.Anything,
	).Return(mock.Anything, nil)

	execCtx.On("SaveState", mock.Anything).Return(nil)

	storage.On(
		"DeleteFilesystem",
		ctx,
		"filesystem",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.FilesystemMeta{
		DeleteTaskID: "toplevel_task_id",
		IsExternal:   true,
	}, nil)
	storage.On("FilesystemDeleted", ctx, "filesystem", mock.Anything).Return(nil)
	nfsFactory.On("NewClient", ctx, "zone").Return(nfsClient, nil)
	nfsClient.On("Close").Return(nil)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	mock.AssertExpectationsForObjects(t, scheduler, nfsFactory, execCtx)
}
