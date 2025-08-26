package disks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

func TestCreateEmptyDiskTask(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		BlockSize: 456,
		Kind:      types.DiskKind_DISK_KIND_SSD,
		CloudId:   "cloud",
		FolderId:  "folder",
	}
	task := &createEmptyDiskTask{
		storage:    storage,
		nbsFactory: nbsFactory,
		params:     params,
		state:      &protos.CreateEmptyDiskTaskState{},
	}

	// TODO: Improve this expectations.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{
		ID: "disk",
	}, nil)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 123,
		BlockSize:   456,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, execCtx)
	require.NoError(t, err)
}

func TestCreateEmptyDiskTaskFailure(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		BlockSize: 456,
		Kind:      types.DiskKind_DISK_KIND_SSD,
		CloudId:   "cloud",
		FolderId:  "folder",
	}
	task := &createEmptyDiskTask{
		storage:    storage,
		nbsFactory: nbsFactory,
		params:     params,
		state:      &protos.CreateEmptyDiskTaskState{},
	}

	// TODO: Improve this expectation.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{}, nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 123,
		BlockSize:   456,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(assert.AnError)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, execCtx)
	require.Equal(t, err, assert.AnError)
}

func TestCancelCreateEmptyDiskTask(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		BlockSize: 456,
		Kind:      types.DiskKind_DISK_KIND_SSD,
		CloudId:   "cloud",
		FolderId:  "folder",
	}
	task := &createEmptyDiskTask{
		storage:    storage,
		nbsFactory: nbsFactory,
		params:     params,
		state:      &protos.CreateEmptyDiskTaskState{},
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{
		DeleteTaskID: "toplevel_task_id",
	}, nil)
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, execCtx)
	require.NoError(t, err)
}

func TestCancelCreateEmptyDiskTaskFailure(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		BlockSize: 456,
		Kind:      types.DiskKind_DISK_KIND_SSD,
		CloudId:   "cloud",
		FolderId:  "folder",
	}
	task := &createEmptyDiskTask{
		storage:    storage,
		nbsFactory: nbsFactory,
		params:     params,
		state:      &protos.CreateEmptyDiskTaskState{},
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{
		DeleteTaskID: "toplevel_task_id",
	}, nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(assert.AnError)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, execCtx)
	require.Equal(t, err, assert.AnError)
}

func TestCancelCreateEmptyDiskTaskBeforeDatabaseInsert(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone",
			DiskId: "disk",
		},
		BlockSize: 456,
		Kind:      types.DiskKind_DISK_KIND_SSD,
		CloudId:   "cloud",
		FolderId:  "folder",
	}
	task := &createEmptyDiskTask{
		storage:    storage,
		nbsFactory: nbsFactory,
		params:     params,
		state:      &protos.CreateEmptyDiskTaskState{},
	}

	// There is no such disk in storage.
	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return((*resources.DiskMeta)(nil), nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, execCtx)
	require.NoError(t, err)
}
