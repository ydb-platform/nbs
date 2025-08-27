package disks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

func testCreateEmptyDiskTask(t *testing.T, selectedZoneID string) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

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
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	cellSelector.On(
		"SelectCell",
		mock.Anything,
		mock.Anything,
	).Return(selectedZoneID)

	// TODO: Improve this expectations.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{
		ID:     "disk",
		ZoneID: selectedZoneID,
	}, nil)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	nbsFactory.On("GetClient", ctx, selectedZoneID).Return(nbsClient, nil)
	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 123,
		BlockSize:   456,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.NoError(t, err)
}

func TestCreateEmptyDiskTask(t *testing.T) {
	testCreateEmptyDiskTask(t, "zone")
}

func TestCreateEmptyDiskTaskCellSelectorSelectsDifferentZone(t *testing.T) {
	testCreateEmptyDiskTask(t, "other-zone")
}

func TestCreateEmptyDiskTaskFailure(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

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
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	cellSelector.On(
		"SelectCell",
		mock.Anything,
		mock.Anything,
	).Return("zone")

	// TODO: Improve this expectation.
	storage.On("CreateDisk", ctx, mock.Anything).Return(
		&resources.DiskMeta{ZoneID: "zone"},
		nil,
	)

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
	mock.AssertExpectationsForObjects(
		t,
		storage,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
	require.Equal(t, err, assert.AnError)
}

func TestCancelCreateEmptyDiskTask(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

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
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	storage.On(
		"DeleteDisk",
		ctx,
		"disk",
		"toplevel_task_id",
		mock.Anything,
	).Return(&resources.DiskMeta{
		DeleteTaskID: "toplevel_task_id",
		ZoneID:       "zone",
	}, nil)
	storage.On("DiskDeleted", ctx, "disk", mock.Anything).Return(nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(nil)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
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
		ZoneID:       "zone",
	}, nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(assert.AnError)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		nbsFactory,
		nbsClient,
		execCtx,
	)
	require.Equal(t, err, assert.AnError)
}

func TestCancelCreateEmptyDiskTaskBeforeRunIsCalled(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
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

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, execCtx)
	require.NoError(t, err)
}
