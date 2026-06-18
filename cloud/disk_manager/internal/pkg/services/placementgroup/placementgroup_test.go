package placementgroup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newExecutionContextMock() *tasks_mocks.ExecutionContextMock {
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("toplevel_task_id")
	return execCtx
}

////////////////////////////////////////////////////////////////////////////////

func TestAlterPlacementGroupMembershipUsesStoredCell(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &alterPlacementGroupMembershipTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		cellSelector: cellSelector,
		request: &protos.AlterPlacementGroupMembershipRequest{
			ZoneId:     "zone-a",
			GroupId:    "pg-1",
			DisksToAdd: []string{"disk-1"},
		},
		state: &protos.AlterPlacementGroupMembershipTaskState{},
	}

	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-a").Return(
		[]string{"zone-a", "zone-a-shard1"}, nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-a-shard1").Return(nbsClient, nil)

	nbsClient.On(
		"AlterPlacementGroupMembership",
		ctx,
		mock.AnythingOfType("func() error"),
		"pg-1",
		uint32(0),
		[]string{"disk-1"},
		[]string(nil),
	).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient, cellSelector)
}

func TestAlterPlacementGroupMembershipFallsBackToRequestZone(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &alterPlacementGroupMembershipTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		cellSelector: cellSelector,
		request: &protos.AlterPlacementGroupMembershipRequest{
			ZoneId:     "zone-a",
			GroupId:    "pg-1",
			DisksToAdd: []string{"disk-1"},
		},
		state: &protos.AlterPlacementGroupMembershipTaskState{},
	}

	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		(*resources.PlacementGroupMeta)(nil), nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-a").Return(nbsClient, nil)

	nbsClient.On(
		"AlterPlacementGroupMembership",
		ctx,
		mock.AnythingOfType("func() error"),
		"pg-1",
		uint32(0),
		[]string{"disk-1"},
		[]string(nil),
	).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient)
}

func TestAlterPlacementGroupMembershipZoneMismatch(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &alterPlacementGroupMembershipTask{
		storage:      storage,
		nbsFactory:   nbs_mocks.NewFactoryMock(),
		cellSelector: cellSelector,
		request: &protos.AlterPlacementGroupMembershipRequest{
			ZoneId:     "zone-b",
			GroupId:    "pg-1",
			DisksToAdd: []string{"disk-1"},
		},
		state: &protos.AlterPlacementGroupMembershipTaskState{},
	}

	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-b").Return(
		[]string{"zone-b"}, nil,
	)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.ErrorContains(t, err,
		"placement group pg-1 is in zone zone-a-shard1, not in requested zone zone-b")
}

////////////////////////////////////////////////////////////////////////////////

func TestAlterPlacementGroupMembershipEmptyZoneIDFallback(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	task := &alterPlacementGroupMembershipTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		cellSelector: cells_mocks.NewCellSelectorMock(),
		request: &protos.AlterPlacementGroupMembershipRequest{
			ZoneId:     "zone-a",
			GroupId:    "pg-1",
			DisksToAdd: []string{"disk-1"},
		},
		state: &protos.AlterPlacementGroupMembershipTaskState{},
	}

	// Tombstone with empty ZoneID — should fallback to request zone.
	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "",
		}, nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-a").Return(nbsClient, nil)

	nbsClient.On(
		"AlterPlacementGroupMembership",
		ctx,
		mock.AnythingOfType("func() error"),
		"pg-1",
		uint32(0),
		[]string{"disk-1"},
		[]string(nil),
	).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient)
}

////////////////////////////////////////////////////////////////////////////////

func TestDeletePlacementGroupEmptyZoneIDFallback(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	task := &deletePlacementGroupTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		cellSelector: cells_mocks.NewCellSelectorMock(),
		request: &protos.DeletePlacementGroupRequest{
			ZoneId:  "zone-a",
			GroupId: "pg-1",
		},
		state: &protos.DeletePlacementGroupTaskState{},
	}

	storage.On("DeletePlacementGroup", ctx, "pg-1", "toplevel_task_id",
		mock.AnythingOfType("time.Time"),
	).Return(&resources.PlacementGroupMeta{
		ID:     "pg-1",
		ZoneID: "",
	}, nil)

	nbsFactory.On("GetClient", ctx, "zone-a").Return(nbsClient, nil)

	nbsClient.On("DeletePlacementGroup", ctx, "pg-1").Return(nil)

	storage.On("PlacementGroupDeleted", ctx, "pg-1",
		mock.AnythingOfType("time.Time"),
	).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClient)
}

func TestDeletePlacementGroupZoneMismatch(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	execCtx := newExecutionContextMock()

	task := &deletePlacementGroupTask{
		storage:      storage,
		nbsFactory:   nbs_mocks.NewFactoryMock(),
		cellSelector: cellSelector,
		request: &protos.DeletePlacementGroupRequest{
			ZoneId:  "zone-b",
			GroupId: "pg-1",
		},
		state: &protos.DeletePlacementGroupTaskState{},
	}

	storage.On("DeletePlacementGroup", ctx, "pg-1", mock.Anything, mock.Anything).Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-b").Return(
		[]string{"zone-b"}, nil,
	)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.ErrorContains(t, err,
		"placement group pg-1 is in zone zone-a-shard1, not in requested zone zone-b")
}

////////////////////////////////////////////////////////////////////////////////

func TestCreatePlacementGroupRetryUsesStoredCell(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	nbsClientStored := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	task := &createPlacementGroupTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		cellSelector: cellSelector,
		request: &protos.CreatePlacementGroupRequest{
			ZoneId:            "zone-a",
			GroupId:           "pg-1",
			PlacementStrategy: types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
		},
		state: &protos.CreatePlacementGroupTaskState{},
	}

	// On retry, PG meta already exists — cell selection should be skipped.
	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-a-shard1").Return(nbsClientStored, nil)

	storage.On("CreatePlacementGroup", ctx, mock.Anything).Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	nbsClientStored.On("ZoneID").Return("zone-a-shard1")

	nbsClientStored.On(
		"CreatePlacementGroup",
		ctx,
		"pg-1",
		types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
		uint32(0),
	).Return(nil)

	storage.On("PlacementGroupCreated", ctx, mock.Anything).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	cellSelector.AssertNotCalled(t, "SelectCellForPlacementGroup",
		mock.Anything, mock.Anything)
	mock.AssertExpectationsForObjects(t, storage, nbsFactory, nbsClientStored)
}

func TestCreatePlacementGroupFirstRun(t *testing.T) {
	ctx := context.Background()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	task := &createPlacementGroupTask{
		storage:      storage,
		nbsFactory:   nbsFactory,
		cellSelector: cellSelector,
		request: &protos.CreatePlacementGroupRequest{
			ZoneId:            "zone-a",
			GroupId:           "pg-1",
			PlacementStrategy: types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
		},
		state: &protos.CreatePlacementGroupTaskState{},
	}

	// First run — no existing meta, cell selection is used.
	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		(*resources.PlacementGroupMeta)(nil), nil,
	)

	cellSelector.On(
		"SelectCellForPlacementGroup",
		ctx,
		"zone-a",
	).Return(nbsClient, nil)

	nbsClient.On("ZoneID").Return("zone-a-shard1")

	execCtx.On("SaveState", ctx).Return(nil)

	storage.On("CreatePlacementGroup", ctx, mock.Anything).Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	nbsClient.On(
		"CreatePlacementGroup",
		ctx,
		"pg-1",
		types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
		uint32(0),
	).Return(nil)

	storage.On("PlacementGroupCreated", ctx, mock.Anything).Return(nil)

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)

	mock.AssertExpectationsForObjects(t, storage, nbsFactory, cellSelector, nbsClient)
}

////////////////////////////////////////////////////////////////////////////////

func TestDescribePlacementGroupZoneMismatch(t *testing.T) {
	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	resourceStorage := storage_mocks.NewStorageMock()

	svc := &service{
		nbsFactory:      nbsFactory,
		resourceStorage: resourceStorage,
		cellSelector:    cellSelector,
	}

	resourceStorage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-b").Return(
		[]string{"zone-b"}, nil,
	)

	_, err := svc.DescribePlacementGroup(ctx, &disk_manager.DescribePlacementGroupRequest{
		GroupId: &disk_manager.GroupId{
			ZoneId:  "zone-b",
			GroupId: "pg-1",
		},
	})
	require.Error(t, err)
	require.ErrorContains(t, err,
		"placement group pg-1 is in zone zone-a-shard1, not in requested zone zone-b")
}

func TestDescribePlacementGroupUsesStoredCell(t *testing.T) {
	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	nbsClient := nbs_mocks.NewClientMock()
	resourceStorage := storage_mocks.NewStorageMock()

	svc := &service{
		nbsFactory:      nbsFactory,
		resourceStorage: resourceStorage,
		cellSelector:    cellSelector,
	}

	resourceStorage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "zone-a-shard1",
		}, nil,
	)

	cellSelector.On("ResolveCells", "zone-a").Return(
		[]string{"zone-a", "zone-a-shard1"}, nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-a-shard1").Return(nbsClient, nil)

	nbsClient.On("DescribePlacementGroup", ctx, "pg-1").Return(
		nbs.PlacementGroup{
			GroupID:           "pg-1",
			PlacementStrategy: types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD,
			DiskIDs:           []string{"disk-1"},
		}, nil,
	)

	resp, err := svc.DescribePlacementGroup(ctx, &disk_manager.DescribePlacementGroupRequest{
		GroupId: &disk_manager.GroupId{
			ZoneId:  "zone-a",
			GroupId: "pg-1",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "zone-a", resp.GroupId.ZoneId)
	require.Equal(t, "pg-1", resp.GroupId.GroupId)
	require.Equal(t, []string{"disk-1"}, resp.DiskIds)
	mock.AssertExpectationsForObjects(t, nbsFactory, cellSelector, nbsClient, resourceStorage)
}

////////////////////////////////////////////////////////////////////////////////

func TestListPlacementGroupsAggregatesCells(t *testing.T) {
	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	nbsClient1 := nbs_mocks.NewClientMock()
	nbsClient2 := nbs_mocks.NewClientMock()
	resourceStorage := storage_mocks.NewStorageMock()

	svc := &service{
		nbsFactory:      nbsFactory,
		resourceStorage: resourceStorage,
		cellSelector:    cellSelector,
	}

	cellSelector.On("ResolveCells", "zone-a").Return(
		[]string{"zone-a", "zone-a-shard1"}, nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-a").Return(nbsClient1, nil)
	nbsFactory.On("GetClient", ctx, "zone-a-shard1").Return(nbsClient2, nil)

	nbsClient1.On("ListPlacementGroups", ctx).Return([]string{"pg-1", "pg-2"}, nil)
	nbsClient2.On("ListPlacementGroups", ctx).Return([]string{"pg-3"}, nil)

	resp, err := svc.ListPlacementGroups(ctx, &disk_manager.ListPlacementGroupsRequest{
		ZoneId: "zone-a",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"pg-1", "pg-2", "pg-3"}, resp.GroupIds)
	mock.AssertExpectationsForObjects(t, nbsFactory, cellSelector, nbsClient1, nbsClient2)
}

func TestListPlacementGroupsSingleCell(t *testing.T) {
	ctx := context.Background()
	nbsFactory := nbs_mocks.NewFactoryMock()
	cellSelector := cells_mocks.NewCellSelectorMock()
	nbsClient := nbs_mocks.NewClientMock()
	resourceStorage := storage_mocks.NewStorageMock()

	svc := &service{
		nbsFactory:      nbsFactory,
		resourceStorage: resourceStorage,
		cellSelector:    cellSelector,
	}

	cellSelector.On("ResolveCells", "zone-b").Return(
		[]string{"zone-b"}, nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-b").Return(nbsClient, nil)
	nbsClient.On("ListPlacementGroups", ctx).Return([]string{"pg-10"}, nil)

	resp, err := svc.ListPlacementGroups(ctx, &disk_manager.ListPlacementGroupsRequest{
		ZoneId: "zone-b",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"pg-10"}, resp.GroupIds)
	mock.AssertExpectationsForObjects(t, nbsFactory, cellSelector, nbsClient)
}
