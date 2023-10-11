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
