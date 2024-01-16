package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newExecutionContextMock() *tasks_mocks.ExecutionContextMock {
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("toplevel_task_id")
	return execCtx
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystemTask(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	request := &protos.CreateFilesystemRequest{
		Filesystem: &protos.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
		CloudId:     "cloud",
		FolderId:    "folder",
		BlockSize:   456,
		BlocksCount: 123,
	}

	task := &createFilesystemTask{
		storage: storage,
		factory: nfsFactory,
		request: request,
		state:   &protos.CreateFilesystemTaskState{},
	}

	storage.On("CreateFilesystem", ctx, mock.Anything).Return(&resources.FilesystemMeta{
		ID: "filesystem",
	}, nil)
	storage.On("FilesystemCreated", ctx, mock.Anything).Return(nil)

	nfsFactory.On("NewClient", ctx, "zone").Return(nfsClient, nil)
	nfsClient.On("Create", ctx, "filesystem", nfs.CreateFilesystemParams{
		CloudID:     "cloud",
		FolderID:    "folder",
		BlockSize:   456,
		BlocksCount: 123,
	}).Return(nil)

	nfsClient.On("Close").Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, nfsFactory, nfsClient, execCtx)
	require.NoError(t, err)
}

func TestCreateFilesystemTaskFailure(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	request := &protos.CreateFilesystemRequest{
		Filesystem: &protos.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
		CloudId:     "cloud",
		FolderId:    "folder",
		BlockSize:   456,
		BlocksCount: 123,
	}

	task := &createFilesystemTask{
		storage: storage,
		factory: nfsFactory,
		request: request,
		state:   &protos.CreateFilesystemTaskState{},
	}

	storage.On("CreateFilesystem", ctx, mock.Anything).Return(&resources.FilesystemMeta{
		ID: "filesystem",
	}, nil)
	storage.On("FilesystemCreated", ctx, mock.Anything).Return(nil)
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
	nfsClient.On("Create", ctx, "filesystem", nfs.CreateFilesystemParams{
		CloudID:     "cloud",
		FolderID:    "folder",
		BlockSize:   456,
		BlocksCount: 123,
	}).Return(assert.AnError)

	nfsClient.On("Close").Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, nfsFactory, nfsClient, execCtx)
	require.Equal(t, err, assert.AnError)
}

func TestCancelCreateFilesystemTask(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	request := &protos.CreateFilesystemRequest{
		Filesystem: &protos.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
		CloudId:     "cloud",
		FolderId:    "folder",
		BlockSize:   456,
		BlocksCount: 123,
	}

	task := &createFilesystemTask{
		storage: storage,
		factory: nfsFactory,
		request: request,
		state:   &protos.CreateFilesystemTaskState{},
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
