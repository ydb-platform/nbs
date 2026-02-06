package filesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
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
	nfsClient.On("ZoneID").Return("zone")
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
	nfsClient.On("ZoneID").Return("zone")
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
	nfsClient.On("Delete", ctx, "filesystem", false).Return(nil)
	nfsClient.On("Close").Return(nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, nfsFactory, nfsClient, execCtx)
	require.NoError(t, err)
}

func TestCreateExternalFilesystemTask(t *testing.T) {
	ctx := context.Background()
	scheduler := tasks_mocks.NewSchedulerMock()
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
		IsExternal:  true,
	}

	task := &createFilesystemTask{
		storage:   storage,
		factory:   nfsFactory,
		scheduler: scheduler,
		request:   request,
		state:     &protos.CreateFilesystemTaskState{},
	}

	externalTask := &createExternalFilesystemTask{
		storage: storage,
		factory: nfsFactory,
		request: request,
		state:   &protos.CreateFilesystemTaskState{},
	}

	scheduler.On("WaitTask", mock.Anything, mock.Anything).Return(mock.Anything, externalTask.Run(ctx, execCtx))
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"filesystem.CreateExternalFilesystem",
		mock.Anything,
		mock.Anything,
	).Return(mock.Anything, nil)

	execCtx.On("SaveState", mock.Anything).Return(nil)

	storage.On("CreateFilesystem", ctx, mock.Anything).Return(&resources.FilesystemMeta{
		ID:         "filesystem",
		IsExternal: true,
	}, nil)
	storage.On("FilesystemCreated", ctx, mock.Anything).Return(nil)
	nfsFactory.On("NewClient", ctx, "zone").Return(nfsClient, nil)
	nfsClient.On("ZoneID").Return("zone")
	nfsClient.On("Close").Return(nil)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	mock.AssertExpectationsForObjects(t, scheduler, nfsFactory, execCtx)
}

func TestCreateFilesystemTaskWithCellSelection(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
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

	state := &protos.CreateFilesystemTaskState{}

	task := &createFilesystemTask{
		storage:      storage,
		factory:      nfsFactory,
		cellSelector: cellSelector,
		request:      request,
		state:        state,
	}

	// Cell selector returns client for cell, not zone.
	cellSelector.On(
		"SelectCellForFilesystem",
		ctx,
		"zone",
		"folder",
	).Return(nfsClient, nil)

	// Client should return cell ID, not zone ID.
	nfsClient.On("ZoneID").Return("cell")
	nfsClient.On("Close").Return(nil)

	// SaveState should be called to persist the selected cell.
	execCtx.On("SaveState", ctx).Return(nil)

	storage.On("CreateFilesystem", ctx, mock.MatchedBy(func(meta resources.FilesystemMeta) bool {
		// Verify that filesystem is created with cell ID, not zone ID.
		return meta.ZoneID == "cell"
	})).Return(&resources.FilesystemMeta{
		ID:     "filesystem",
		ZoneID: "cell",
	}, nil)
	storage.On("FilesystemCreated", ctx, mock.Anything).Return(nil)

	nfsClient.On("Create", ctx, "filesystem", nfs.CreateFilesystemParams{
		CloudID:     "cloud",
		FolderID:    "folder",
		BlockSize:   456,
		BlocksCount: 123,
	}).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	// Verify that state contains the selected cell ID.
	require.Equal(t, "cell", state.SelectedCellId)

	mock.AssertExpectationsForObjects(t, cellSelector, nfsFactory, nfsClient, execCtx)
}

func TestCreateFilesystemTaskWithCellSelectionFromState(t *testing.T) {
	ctx := context.Background()
	storage := resources_mocks.NewStorageMock()
	nfsFactory := nfs_mocks.NewFactoryMock()
	nfsClient := nfs_mocks.NewClientMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
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

	// State already contains selected cell from previous execution.
	state := &protos.CreateFilesystemTaskState{
		SelectedCellId: "cell",
	}

	task := &createFilesystemTask{
		storage:      storage,
		factory:      nfsFactory,
		cellSelector: cellSelector,
		request:      request,
		state:        state,
	}

	// When state has SelectedCellId, factory.NewClient should be called
	// instead of cellSelector.SelectCellForFilesystem.
	nfsFactory.On("NewClient", ctx, "cell").Return(nfsClient, nil)
	nfsClient.On("ZoneID").Return("cell")
	nfsClient.On("Close").Return(nil)

	// SaveState should NOT be called since we're using existing state.
	// cellSelector.SelectCellForFilesystem should NOT be called.

	storage.On("CreateFilesystem", ctx, mock.MatchedBy(func(meta resources.FilesystemMeta) bool {
		return meta.ZoneID == "cell"
	})).Return(&resources.FilesystemMeta{
		ID:     "filesystem",
		ZoneID: "cell",
	}, nil)
	storage.On("FilesystemCreated", ctx, mock.Anything).Return(nil)

	nfsClient.On("Create", ctx, "filesystem", nfs.CreateFilesystemParams{
		CloudID:     "cloud",
		FolderID:    "folder",
		BlockSize:   456,
		BlocksCount: 123,
	}).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	// Cell selector should NOT be called when state already has cell ID.
	cellSelector.AssertNotCalled(t, "SelectCellForFilesystem", mock.Anything, mock.Anything, mock.Anything)

	// SaveState should NOT be called.
	execCtx.AssertNotCalled(t, "SaveState", mock.Anything)

	mock.AssertExpectationsForObjects(t, nfsFactory, nfsClient, execCtx)
}
