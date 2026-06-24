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

func TestCreateEmptyDiskTask(t *testing.T) {
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

	// TODO: Improve this expectations.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{
		ID: "disk",
	}, nil)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	cellSelector.On(
		"SelectCellForDisk",
		ctx,
		"zone",
		"folder",
		types.DiskKind_DISK_KIND_SSD,
		false, // requireExactCellIDMatch
	).Return(nbsClient, nil)

	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 123,
		BlockSize:   456,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(nil)
	nbsClient.On("ZoneID").Return("zone")

	execCtx.On("SaveState", ctx).Return(nil)

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

	// TODO: Improve this expectation.
	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{}, nil)

	cellSelector.On(
		"SelectCellForDisk",
		ctx,
		"zone",
		"folder",
		types.DiskKind_DISK_KIND_SSD,
		false, // requireExactCellIDMatch
	).Return(nbsClient, nil)

	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 123,
		BlockSize:   456,
		Kind:        types.DiskKind_DISK_KIND_SSD,
		CloudID:     "cloud",
		FolderID:    "folder",
	}).Return(assert.AnError)
	nbsClient.On("ZoneID").Return("zone")

	execCtx.On("SaveState", ctx).Return(nil)

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
		ID:           "disk",
		ZoneID:       "zone",
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
		ID:           "disk",
		ZoneID:       "zone",
		DeleteTaskID: "toplevel_task_id",
	}, nil)

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
	nbsClient.On("Delete", ctx, "disk").Return(assert.AnError)

	err := task.Cancel(ctx, execCtx)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, execCtx)
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

func TestCreateEmptyDiskTaskWithPlacementGroup(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone-a",
			DiskId: "disk",
		},
		BlockSize:        456,
		Kind:             types.DiskKind_DISK_KIND_SSD,
		CloudId:          "cloud",
		FolderId:         "folder",
		PlacementGroupId: "pg-1",
	}
	task := &createEmptyDiskTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	// PG was created on "zone-a-shard1" cell.
	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	// Zone "zone-a" contains "zone-a-shard1".
	cellSelector.On("ResolveCells", "zone-a").Return(
		[]string{"zone-a", "zone-a-shard1"}, nil,
	)

	// Disk should be pinned to the PG's cell, NOT go through CellSelector.
	nbsFactory.On("GetClient", ctx, "zone-a-shard1").Return(nbsClient, nil)

	storage.On("CreateDisk", ctx, mock.Anything).Return(&resources.DiskMeta{
		ID: "disk",
	}, nil)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:               "disk",
		BlocksCount:      123,
		BlockSize:        456,
		Kind:             types.DiskKind_DISK_KIND_SSD,
		CloudID:          "cloud",
		FolderID:         "folder",
		PlacementGroupID: "pg-1",
	}).Return(nil)
	nbsClient.On("ZoneID").Return("zone-a-shard1")

	execCtx.On("SaveState", ctx).Return(nil)

	err := task.Run(ctx, execCtx)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		nbsFactory,
		nbsClient,
		execCtx,
	)
	require.NoError(t, err)

	// CellSelector should NOT have been called — PG pinning bypasses it.
	cellSelector.AssertNotCalled(t, "SelectCellForDisk")
}

func TestCreateEmptyDiskTaskWithPlacementGroupNotFound(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone-a",
			DiskId: "disk",
		},
		BlockSize:        456,
		Kind:             types.DiskKind_DISK_KIND_SSD,
		CloudId:          "cloud",
		FolderId:         "folder",
		PlacementGroupId: "pg-nonexistent",
	}
	task := &createEmptyDiskTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	storage.On("GetPlacementGroupMeta", ctx, "pg-nonexistent").Return(
		(*resources.PlacementGroupMeta)(nil), nil,
	)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.ErrorContains(t, err, "placement group pg-nonexistent not found")

	cellSelector.AssertNotCalled(t, "SelectCellForDisk")
}

func TestCreateEmptyDiskTaskWithPlacementGroupZoneMismatch(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone-b",
			DiskId: "disk",
		},
		BlockSize:        456,
		Kind:             types.DiskKind_DISK_KIND_SSD,
		CloudId:          "cloud",
		FolderId:         "folder",
		PlacementGroupId: "pg-1",
	}
	task := &createEmptyDiskTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	// PG was created in zone-a (on "zone-a-shard1" cell).
	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	// Disk requests zone-b, which does not contain "zone-a-shard1".
	cellSelector.On("ResolveCells", "zone-b").Return(
		[]string{"zone-b"}, nil,
	)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.ErrorContains(t, err, "placement group pg-1 is in zone zone-a-shard1, not in requested zone zone-b")

	cellSelector.AssertNotCalled(t, "SelectCellForDisk")
}

func TestCreateEmptyDiskTaskWithPlacementGroupExactCellMismatch(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone-a",
			DiskId: "disk",
		},
		BlockSize:               456,
		Kind:                    types.DiskKind_DISK_KIND_SSD,
		CloudId:                 "cloud",
		FolderId:                "folder",
		PlacementGroupId:        "pg-1",
		RequireExactCellIdMatch: true,
	}
	task := &createEmptyDiskTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	// PG is on "zone-a-shard1", but caller requires exact cell "zone-a".
	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.ErrorContains(t, err,
		"placement group pg-1 is in zone zone-a-shard1, not in requested zone zone-a")

	cellSelector.AssertNotCalled(t, "ResolveCells")
}

func TestCreateEmptyDiskTaskLocalDiskWithPGCellMismatch(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	localClient := nbs_mocks.NewClientMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone-a",
			DiskId: "disk",
		},
		BlockSize:        456,
		Kind:             types.DiskKind_DISK_KIND_SSD_LOCAL,
		CloudId:          "cloud",
		FolderId:         "folder",
		PlacementGroupId: "pg-1",
		AgentIds:         []string{"agent-1"},
	}
	task := &createEmptyDiskTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-a").Return(
		[]string{"zone-a-shard1", "zone-a-shard2"}, nil,
	)

	// Agent is on zone-a-shard2 but PG is on zone-a-shard1.
	cellSelector.On(
		"SelectCellForLocalDisk", ctx, "zone-a", []string{"agent-1"},
	).Return(localClient, nil)
	localClient.On("ZoneID").Return("zone-a-shard2")

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.ErrorContains(t, err,
		"agent for local disk is in cell zone-a-shard2, "+
			"but placement group pg-1 is in cell zone-a-shard1")
}

func TestCreateEmptyDiskTaskLocalDiskWithPGSameCell(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	execCtx := newExecutionContextMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	localClient := nbs_mocks.NewClientMock()

	params := &protos.CreateDiskParams{
		BlocksCount: 123,
		Disk: &types.Disk{
			ZoneId: "zone-a",
			DiskId: "disk",
		},
		BlockSize:        456,
		Kind:             types.DiskKind_DISK_KIND_SSD_LOCAL,
		CloudId:          "cloud",
		FolderId:         "folder",
		PlacementGroupId: "pg-1",
		AgentIds:         []string{"agent-1"},
	}
	task := &createEmptyDiskTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-a").Return(
		[]string{"zone-a-shard1", "zone-a-shard2"}, nil,
	)

	// Agent is on zone-a-shard1, same as PG — should succeed.
	cellSelector.On(
		"SelectCellForLocalDisk", ctx, "zone-a", []string{"agent-1"},
	).Return(localClient, nil)
	localClient.On("ZoneID").Return("zone-a-shard1")

	storage.On("CreateDisk", ctx, mock.Anything).Return(
		&resources.DiskMeta{ID: "disk"}, nil,
	)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	localClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:               "disk",
		BlocksCount:      123,
		BlockSize:        456,
		Kind:             types.DiskKind_DISK_KIND_SSD_LOCAL,
		CloudID:          "cloud",
		FolderID:         "folder",
		PlacementGroupID: "pg-1",
		AgentIds:         []string{"agent-1"},
	}).Return(nil)

	execCtx.On("SaveState", ctx).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, localClient, execCtx, cellSelector)
}

func TestCreateEmptyLocalDiskTask(t *testing.T) {
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
		Kind:      types.DiskKind_DISK_KIND_SSD_LOCAL,
		CloudId:   "cloud",
		FolderId:  "folder",
		AgentIds:  []string{"agent"},
	}
	task := &createEmptyDiskTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		params:       params,
		state:        &protos.CreateEmptyDiskTaskState{},
		cellSelector: cellSelector,
	}

	storage.On("CreateDisk", ctx, mock.Anything).Return(
		&resources.DiskMeta{
			ID: "disk",
		},
		nil,
	)
	storage.On("DiskCreated", ctx, mock.Anything).Return(nil)

	cellSelector.On(
		"SelectCellForLocalDisk",
		ctx,
		"zone",
		[]string{"agent"},
	).Return(nbsClient, nil)

	nbsClient.On("Create", ctx, nbs.CreateDiskParams{
		ID:          "disk",
		BlocksCount: 123,
		BlockSize:   456,
		Kind:        types.DiskKind_DISK_KIND_SSD_LOCAL,
		CloudID:     "cloud",
		FolderID:    "folder",
		AgentIds:    []string{"agent"},
	}).Return(nil)
	nbsClient.On("ZoneID").Return("zone")

	execCtx.On("SaveState", ctx).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		nbsFactory,
		nbsClient,
		execCtx,
		cellSelector,
	)
}
