package placementgroup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type deletePlacementGroupTaskTest struct {
	ctx          context.Context
	storage      *storage_mocks.StorageMock
	nbsFactory   *nbs_mocks.FactoryMock
	nbsClient    *nbs_mocks.ClientMock
	cellSelector *cells_mocks.CellSelectorMock
	execCtx      tasks.ExecutionContext
	task         *deletePlacementGroupTask
	meta         *resources.PlacementGroupMeta
}

func newDeletePlacementGroupTaskTest(t *testing.T) *deletePlacementGroupTaskTest {
	t.Helper()

	f := &deletePlacementGroupTaskTest{
		ctx:          logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.WarnLevel)),
		storage:      storage_mocks.NewStorageMock(),
		nbsFactory:   nbs_mocks.NewFactoryMock(),
		nbsClient:    nbs_mocks.NewClientMock(),
		cellSelector: cells_mocks.NewCellSelectorMock(),
		execCtx:      newExecutionContextMock(),
		meta:         &resources.PlacementGroupMeta{ID: "pg-1", ZoneID: "zone-d"},
	}
	f.task = &deletePlacementGroupTask{
		storage:       f.storage,
		nbsFactory:    f.nbsFactory,
		cellSelector:  f.cellSelector,
		disabledZones: common.NewDisabledZones([]string{"zone-d"}),
		request: &protos.DeletePlacementGroupRequest{
			ZoneId: "zone-d", GroupId: "pg-1",
		},
		state: &protos.DeletePlacementGroupTaskState{},
	}

	// Each test declares its allowed calls; any other mock call fails the test.
	f.storage.Test(t)
	f.nbsFactory.Test(t)
	f.nbsClient.Test(t)
	f.cellSelector.Test(t)

	t.Cleanup(func() {
		mock.AssertExpectationsForObjects(t, f.storage, f.nbsFactory, f.nbsClient, f.cellSelector)
	})

	return f
}

func requireDeletePlacementGroupRetryError(t *testing.T, err, cause error) {
	t.Helper()

	require.True(t, errors.Is(err, cause))

	retryError, ok := err.(*errors.RetriableError)
	require.True(t, ok)
	require.True(t, retryError.IgnoreRetryLimit)
}

////////////////////////////////////////////////////////////////////////////////

func TestDeletePlacementGroupTaskDisabledZones(t *testing.T) {
	disabledZone := common.NewDisabledZones([]string{"zone-d"})
	disabledCell := common.NewDisabledZones([]string{"zone-d-shard1"})
	zoneMeta := &resources.PlacementGroupMeta{ID: "pg-1", ZoneID: "zone-d"}
	cellMeta := &resources.PlacementGroupMeta{ID: "pg-1", ZoneID: "zone-d-shard1"}

	testCases := []struct {
		name            string
		metadata        *resources.PlacementGroupMeta
		requestZone     string
		disabledZones   *common.DisabledZones
		resolvedCells   []string
		expectedNBSZone string // Empty means no NBS calls are expected.
	}{
		{
			// A disabled stored zone requires only metadata cleanup.
			name:          "disabled zone",
			metadata:      zoneMeta,
			requestZone:   "zone-d",
			disabledZones: disabledZone,
		},
		{
			// A disabled stored cell bypasses resolution of the logical zone.
			name:          "disabled stored cell",
			metadata:      cellMeta,
			requestZone:   "zone-d",
			disabledZones: disabledCell,
		},
		{
			// A disabled group's metadata is authoritative despite the request zone.
			name:          "disabled zone accepts different request zone",
			metadata:      zoneMeta,
			requestZone:   "zone-a",
			disabledZones: disabledZone,
		},
		{
			// Disabling the request zone does not disable its active stored cell.
			name:            "allowed stored cell overrides disabled request zone",
			metadata:        cellMeta,
			requestZone:     "zone-d",
			disabledZones:   disabledZone,
			resolvedCells:   []string{"zone-d-shard1"},
			expectedNBSZone: "zone-d-shard1",
		},
		{
			// Disabling a cell does not disable a different stored zone ID.
			name:            "disabled cell does not disable zone",
			metadata:        zoneMeta,
			requestZone:     "zone-d",
			disabledZones:   disabledCell,
			resolvedCells:   []string{"zone-d"},
			expectedNBSZone: "zone-d",
		},
		{
			// Without metadata, the disabled request zone controls cleanup.
			name:          "missing metadata uses disabled request zone",
			requestZone:   "zone-d",
			disabledZones: disabledZone,
		},
		{
			// An empty stored zone falls back to the disabled request zone.
			name:          "empty metadata zone uses disabled request zone",
			metadata:      &resources.PlacementGroupMeta{ID: "pg-1"},
			requestZone:   "zone-d",
			disabledZones: disabledZone,
		},
		{
			// Without metadata, an active request zone still uses NBS.
			name:            "missing metadata uses allowed request zone",
			requestZone:     "zone-a",
			disabledZones:   disabledZone,
			expectedNBSZone: "zone-a",
		},
		{
			// With no disabled policy, existing resolution and NBS deletion remain.
			name:            "missing policy",
			metadata:        zoneMeta,
			requestZone:     "zone-d",
			resolvedCells:   []string{"zone-d"},
			expectedNBSZone: "zone-d",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeletePlacementGroupTaskTest(t)
			f.task.request.ZoneId = tc.requestZone
			f.task.disabledZones = tc.disabledZones

			f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(tc.metadata, nil).Once()
			if tc.resolvedCells != nil {
				f.cellSelector.On("ResolveCells", tc.requestZone).Return(tc.resolvedCells, nil).Once()
			}
			if tc.expectedNBSZone != "" {
				f.nbsFactory.On("GetClient", f.ctx, tc.expectedNBSZone).Return(f.nbsClient, nil).Once()
				f.nbsClient.On("DeletePlacementGroup", f.ctx, "pg-1").Return(nil).Once()
			}
			f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
				Return(&resources.PlacementGroupMeta{ID: "pg-1"}, nil).Once()
			f.storage.On("PlacementGroupDeleted", f.ctx, "pg-1", mock.Anything).Return(nil).Once()

			err := f.task.Run(f.ctx, f.execCtx)

			require.NoError(t, err)
			if tc.expectedNBSZone == "" {
				f.cellSelector.AssertNumberOfCalls(t, "ResolveCells", 0)
				f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
				f.nbsClient.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
			}
		})
	}
}

func TestDeletePlacementGroupTaskMetadataReadError(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").
		Return((*resources.PlacementGroupMeta)(nil), assert.AnError).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, assert.AnError)
	f.cellSelector.AssertNumberOfCalls(t, "ResolveCells", 0)
	f.storage.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
}

func TestDeletePlacementGroupTaskDisabledZoneRemovedFromCellsConfig(t *testing.T) {
	testCases := []struct {
		name       string
		zoneCells  map[string]*cells_config.ZoneCells
		cancelTask bool
	}{
		{
			// Run succeeds when the request zone is no longer configured.
			name:      "run with removed zone",
			zoneCells: map[string]*cells_config.ZoneCells{},
		},
		{
			// Cancel succeeds when the request zone is no longer configured.
			name:       "cancel with removed zone",
			zoneCells:  map[string]*cells_config.ZoneCells{},
			cancelTask: true,
		},
		{
			// Run succeeds when the disabled cell is removed from a known zone.
			name: "run with removed cell",
			zoneCells: map[string]*cells_config.ZoneCells{
				"zone-d": {Cells: []string{"zone-d-shard2"}},
			},
		},
		{
			// Cancel succeeds when the disabled cell is removed from a known zone.
			name: "cancel with removed cell",
			zoneCells: map[string]*cells_config.ZoneCells{
				"zone-d": {Cells: []string{"zone-d-shard2"}},
			},
			cancelTask: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeletePlacementGroupTaskTest(t)
			f.meta.ZoneID = "zone-d-shard1"
			f.task.disabledZones = common.NewDisabledZones([]string{f.meta.ZoneID})
			f.task.cellSelector = cells.NewCellSelector(
				&cells_config.CellsConfig{Cells: tc.zoneCells},
				nil, // storage
				f.nbsFactory,
				nil, // nfsFactory
			)

			f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
			f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
				Return(f.meta, nil).Once()
			f.storage.On("PlacementGroupDeleted", f.ctx, "pg-1", mock.Anything).Return(nil).Once()

			var err error
			if tc.cancelTask {
				err = f.task.Cancel(f.ctx, f.execCtx)
			} else {
				err = f.task.Run(f.ctx, f.execCtx)
			}

			require.NoError(t, err)
			f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
			f.nbsClient.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
		})
	}
}

func TestDeletePlacementGroupTaskActiveZoneRequiresCellResolution(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)
	f.meta.ZoneID = "zone-a"
	f.task.request.ZoneId = "zone-a"

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.cellSelector.On("ResolveCells", "zone-a").Return([]string(nil), assert.AnError).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, assert.AnError)
	f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
	f.storage.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
	f.storage.AssertNumberOfCalls(t, "PlacementGroupDeleted", 0)
}

func TestDeletePlacementGroupTaskActiveZoneRejectsWrongRequestZone(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)
	f.meta.ZoneID = "zone-a"

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.cellSelector.On("ResolveCells", "zone-d").Return([]string{"zone-d"}, nil).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	require.ErrorContains(t, err, "not in requested zone")
	requireDeletePlacementGroupRetryError(t, err, errors.NewEmptyNonRetriableError())
	f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
	f.storage.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
	f.storage.AssertNumberOfCalls(t, "PlacementGroupDeleted", 0)
}

func TestDeletePlacementGroupTaskClientErrorDoesNotMutateMetadata(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)
	f.meta.ZoneID = "zone-a"
	f.task.request.ZoneId = "zone-a"

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.cellSelector.On("ResolveCells", "zone-a").Return([]string{"zone-a"}, nil).Once()
	f.nbsFactory.On("GetClient", f.ctx, "zone-a").Return(nil, assert.AnError).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, assert.AnError)
	f.storage.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
	f.storage.AssertNumberOfCalls(t, "PlacementGroupDeleted", 0)
}

func TestDeletePlacementGroupTaskMetadataDeletionError(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
		Return(f.meta, assert.AnError).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, assert.AnError)
	f.storage.AssertNumberOfCalls(t, "PlacementGroupDeleted", 0)
}

func TestDeletePlacementGroupTaskMetadataDeletionRejected(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
		Return((*resources.PlacementGroupMeta)(nil), nil).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, errors.NewEmptyNonCancellableError())
	f.storage.AssertNumberOfCalls(t, "PlacementGroupDeleted", 0)
}

func TestDeletePlacementGroupTaskBackendDeletionError(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)
	f.meta.ZoneID = "zone-a"
	f.task.request.ZoneId = "zone-a"

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.cellSelector.On("ResolveCells", "zone-a").Return([]string{"zone-a"}, nil).Once()
	f.nbsFactory.On("GetClient", f.ctx, "zone-a").Return(f.nbsClient, nil).Once()
	f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
		Return(f.meta, nil).Once()
	f.nbsClient.On("DeletePlacementGroup", f.ctx, "pg-1").Return(assert.AnError).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, assert.AnError)
	f.storage.AssertNumberOfCalls(t, "PlacementGroupDeleted", 0)
}

func TestDeletePlacementGroupTaskDisabledZoneFinalizationError(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
		Return(f.meta, nil).Once()
	f.storage.On("PlacementGroupDeleted", f.ctx, "pg-1", mock.Anything).Return(assert.AnError).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, assert.AnError)
	f.cellSelector.AssertNumberOfCalls(t, "ResolveCells", 0)
	f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
}

func TestDeletePlacementGroupTaskCancelInDisabledZone(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
		Return(f.meta, nil).Once()
	f.storage.On("PlacementGroupDeleted", f.ctx, "pg-1", mock.Anything).Return(nil).Once()

	err := f.task.Cancel(f.ctx, f.execCtx)

	require.NoError(t, err)
	f.cellSelector.AssertNumberOfCalls(t, "ResolveCells", 0)
	f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
	f.nbsClient.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
}

func TestDeletePlacementGroupTaskCancelFinalizationError(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Once()
	f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
		Return(f.meta, nil).Once()
	f.storage.On("PlacementGroupDeleted", f.ctx, "pg-1", mock.Anything).Return(assert.AnError).Once()

	err := f.task.Cancel(f.ctx, f.execCtx)

	require.True(t, errors.Is(err, assert.AnError))
	f.cellSelector.AssertNumberOfCalls(t, "ResolveCells", 0)
	f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
}

func TestDeletePlacementGroupTaskDisabledZoneRetriesFinalization(t *testing.T) {
	f := newDeletePlacementGroupTaskTest(t)
	f.meta.ZoneID = "zone-d-shard1"
	f.task.disabledZones = common.NewDisabledZones([]string{"zone-d-shard1"})

	f.storage.On("GetPlacementGroupMeta", f.ctx, "pg-1").Return(f.meta, nil).Twice()
	f.storage.On("DeletePlacementGroup", f.ctx, "pg-1", "toplevel_task_id", mock.Anything).
		Return(f.meta, nil).Twice()
	f.storage.On("PlacementGroupDeleted", f.ctx, "pg-1", mock.Anything).Return(assert.AnError).Once()
	f.storage.On("PlacementGroupDeleted", f.ctx, "pg-1", mock.Anything).Return(nil).Once()

	err := f.task.Run(f.ctx, f.execCtx)

	requireDeletePlacementGroupRetryError(t, err, assert.AnError)

	err = f.task.Run(f.ctx, f.execCtx)

	require.NoError(t, err)
	f.cellSelector.AssertNumberOfCalls(t, "ResolveCells", 0)
	f.nbsFactory.AssertNumberOfCalls(t, "GetClient", 0)
	f.nbsClient.AssertNumberOfCalls(t, "DeletePlacementGroup", 0)
}
