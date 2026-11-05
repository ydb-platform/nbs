package placementgroup

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	placement_group_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
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
	).Once()

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
	).Twice()

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
		storage:       storage,
		nbsFactory:    nbs_mocks.NewFactoryMock(),
		cellSelector:  cellSelector,
		disabledZones: common.NewDisabledZones([]string{"zone-c"}),
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

	// Tombstone with empty ZoneID — should fallback to request zone.
	storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(
		&resources.PlacementGroupMeta{
			ID:     "pg-1",
			ZoneID: "",
		}, nil,
	)

	nbsFactory.On("GetClient", ctx, "zone-a").Return(nbsClient, nil)

	storage.On("DeletePlacementGroup", ctx, "pg-1", "toplevel_task_id",
		mock.AnythingOfType("time.Time"),
	).Return(&resources.PlacementGroupMeta{
		ID: "pg-1",
	}, nil)

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

////////////////////////////////////////////////////////////////////////////////

func TestAlterPlacementGroupMembershipRunDisabledZones(t *testing.T) {
	testCases := []struct {
		name              string
		metadata          *resources.PlacementGroupMeta
		disabledZones     []string
		metadataReadError error
		zoneDisabled      bool
		removalOnly       bool
	}{
		// Reject the disabled stored cell before resolving cells or calling NBS.
		{
			name:          "disabled stored cell",
			metadata:      &resources.PlacementGroupMeta{ID: "pg-1", ZoneID: "zone-a-shard1"},
			disabledZones: []string{"zone-a-shard1"},
			zoneDisabled:  true,
		},

		// With no metadata, reject the disabled zone from the request.
		{
			name:          "missing metadata uses disabled request zone",
			disabledZones: []string{"zone-a"},
			zoneDisabled:  true,
		},

		// Use the enabled stored cell even when the original request zone is disabled.
		{
			name:          "enabled stored cell in disabled request zone",
			metadata:      &resources.PlacementGroupMeta{ID: "pg-1", ZoneID: "zone-a-shard1"},
			disabledZones: []string{"zone-a"},
		},

		// Preserve a metadata lookup failure instead of falling back to the request zone.
		{
			name:              "metadata read fails",
			disabledZones:     []string{"zone-a"},
			metadataReadError: assert.AnError,
		},
		{
			name:          "removal skips disabled stored cell",
			metadata:      &resources.PlacementGroupMeta{ID: "pg-1", ZoneID: "zone-a-shard1"},
			disabledZones: []string{"zone-a-shard1"},
			zoneDisabled:  true,
			removalOnly:   true,
		},
		{
			name:          "removal without metadata skips disabled request zone",
			disabledZones: []string{"zone-a"},
			zoneDisabled:  true,
			removalOnly:   true,
		},
		{
			name:          "removal in enabled stored cell calls NBS",
			metadata:      &resources.PlacementGroupMeta{ID: "pg-1", ZoneID: "zone-a-shard1"},
			disabledZones: []string{"zone-a"},
			removalOnly:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			storage := storage_mocks.NewStorageMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsClient := nbs_mocks.NewClientMock()
			cellSelector := cells_mocks.NewCellSelectorMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			scheduler := tasks_mocks.NewSchedulerMock()

			for _, m := range []*mock.Mock{
				&storage.Mock, &nbsFactory.Mock, &nbsClient.Mock,
				&cellSelector.Mock, &execCtx.Mock, &scheduler.Mock,
			} {
				m.Test(t)
				t.Cleanup(func() { m.AssertExpectations(t) })
			}

			registry := tasks.NewRegistry()
			scheduler.On("ScheduleRegularTasks", ctx, "placement_groups.ClearDeletedPlacementGroups", mock.Anything).
				Once()

			require.NoError(t, RegisterForExecution(
				ctx, &placement_group_config.Config{}, common.NewDisabledZones(tc.disabledZones),
				registry, scheduler, storage, nbsFactory, cellSelector,
			))

			registeredTask, err := registry.NewTask("placement_group.AlterPlacementGroupMembership")
			require.NoError(t, err)

			task := registeredTask.(*alterPlacementGroupMembershipTask)
			disksToAdd := []string{"disk-to-add-1", "disk-to-add-2"}
			if tc.removalOnly {
				disksToAdd = nil
			}
			task.request = &protos.AlterPlacementGroupMembershipRequest{
				ZoneId:                  "zone-a",
				GroupId:                 "pg-1",
				PlacementPartitionIndex: 2,
				DisksToAdd:              disksToAdd,
				DisksToRemove:           []string{"disk-to-remove"},
			}
			task.state = &protos.AlterPlacementGroupMembershipTaskState{}

			storage.On("GetPlacementGroupMeta", ctx, "pg-1").Return(tc.metadata, tc.metadataReadError).Once()

			if tc.metadata != nil && !tc.zoneDisabled && tc.metadataReadError == nil {
				cellSelector.On("ResolveCells", "zone-a").
					Return([]string{"zone-a", "zone-a-shard1"}, nil).Once()
			}

			if !tc.zoneDisabled && tc.metadataReadError == nil {
				nbsFactory.On("GetClient", ctx, "zone-a-shard1").Return(nbsClient, nil).Once()
				execCtx.On("SaveState", ctx).Return(nil).Once()

				nbsClient.On(
					"AlterPlacementGroupMembership",
					ctx,
					mock.AnythingOfType("func() error"),
					"pg-1",
					uint32(2),
					disksToAdd,
					[]string{"disk-to-remove"},
				).Run(func(args mock.Arguments) {
					// The NBS operation must retain the task-generation check.
					require.NoError(t, args.Get(1).(func() error)())
				}).Return(nil).Once()
			}

			err = task.Run(ctx, execCtx)

			switch {
			case tc.metadataReadError != nil:
				require.ErrorIs(t, err, tc.metadataReadError)

			case tc.zoneDisabled && !tc.removalOnly:
				require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
				require.NotErrorIs(t, err, errors.NewEmptyNonCancellableError())
				require.NotErrorIs(t, err, errors.NewEmptyAbortedError())

				var detailed *errors.DetailedError
				require.ErrorAs(t, err, &detailed)
				require.NotNil(t, detailed.Details)
				require.Equal(t, int64(codes.Aborted), detailed.Details.Code)
				require.True(t, detailed.Details.Internal)
				require.Contains(t, detailed.Details.Message, tc.disabledZones[0])

			default:
				require.NoError(t, err)
			}

			if tc.zoneDisabled || tc.metadataReadError != nil {
				cellSelector.AssertNumberOfCalls(t, "ResolveCells", 0)
				nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
				nbsClient.AssertNumberOfCalls(t, "AlterPlacementGroupMembership", 0)
				execCtx.AssertNumberOfCalls(t, "SaveState", 0)
			}
		})
	}
}
