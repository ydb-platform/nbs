package cells

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/protos"
	cells_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestCollectClusterCapacityTask(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a-cell1", "zone-a"}},
			"zone-b": {Cells: []string{"zone-b"}},
		},
	}

	capacityInfo := nbs.ClusterCapacityInfo{
		DiskKind:   types.DiskKind_DISK_KIND_SSD,
		FreeBytes:  1024,
		TotalBytes: 2048,
	}

	deleteOlderThan := time.Now().Add(-time.Hour)
	deleteOlderThanExpectation := mock.MatchedBy(func(t time.Time) bool {
		return t.After(deleteOlderThan.Add(-15*time.Minute)) &&
			t.Before(deleteOlderThan.Add(15*time.Minute))
	})

	nbsFactory.On(
		"GetClient",
		ctx,
		mock.Anything, // For each cell we return the same mock client.
	).Return(nbsClient, nil).Times(3)
	nbsClient.On("GetClusterCapacity", mock.Anything).Return(
		[]nbs.ClusterCapacityInfo{capacityInfo},
		nil,
	).Times(3)

	storage.On("UpdateClusterCapacities",
		ctx,
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     "zone-a",
				CellID:     "zone-a-cell1",
				Kind:       types.DiskKind_DISK_KIND_SSD,
				FreeBytes:  1024,
				TotalBytes: 2048,
			},
		},
		deleteOlderThanExpectation,
	).Return(nil).Once()
	storage.On("UpdateClusterCapacities",
		ctx,
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     "zone-a",
				CellID:     "zone-a",
				Kind:       types.DiskKind_DISK_KIND_SSD,
				FreeBytes:  1024,
				TotalBytes: 2048,
			},
		},
		deleteOlderThanExpectation,
	).Return(nil).Once()
	storage.On("UpdateClusterCapacities",
		ctx,
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     "zone-b",
				CellID:     "zone-b",
				Kind:       types.DiskKind_DISK_KIND_SSD,
				FreeBytes:  1024,
				TotalBytes: 2048,
			},
		},
		deleteOlderThanExpectation,
	).Return(nil).Once()

	execCtx.On("SaveState", ctx).Return(nil).Times(3)

	task := collectClusterCapacityTask{
		config:            config,
		storage:           storage,
		nbsFactory:        nbsFactory,
		state:             &protos.CollectClusterCapacityTaskState{},
		expirationTimeout: time.Hour, // Can be any, storage is mocked.
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]string{"zone-a-cell1", "zone-a", "zone-b"},
		task.state.ProcessedCells,
	)

	mock.AssertExpectationsForObjects(
		t,
		execCtx,
		storage,
		nbsFactory,
		nbsClient,
	)
}

func TestCollectClusterCapacityFailureNbsReturnsError(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient1 := nbs_mocks.NewClientMock()
	nbsClient2 := nbs_mocks.NewClientMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a-cell1", "zone-a"}},
		},
	}

	capacityInfo := nbs.ClusterCapacityInfo{
		DiskKind:   types.DiskKind_DISK_KIND_SSD,
		FreeBytes:  1024,
		TotalBytes: 2048,
	}

	nbsFactory.On("GetClient", ctx, "zone-a").Return(nbsClient1, nil).Once()
	nbsFactory.On(
		"GetClient",
		ctx,
		"zone-a-cell1",
	).Return(nbsClient2, nil).Once()

	nbsClient1.On("GetClusterCapacity", ctx).Return(
		[]nbs.ClusterCapacityInfo{capacityInfo},
		nil,
	).Once()
	nbsClient2.On("GetClusterCapacity", ctx).Return(
		[]nbs.ClusterCapacityInfo{},
		assert.AnError,
	).Once()

	// Only the successful cell should be updated
	storage.On("UpdateClusterCapacities",
		ctx,
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     "zone-a",
				CellID:     "zone-a",
				Kind:       types.DiskKind_DISK_KIND_SSD,
				FreeBytes:  1024,
				TotalBytes: 2048,
			},
		},
		mock.Anything, // deleteOlderThan.
	).Return(nil).Once()

	execCtx.On("SaveState", ctx).Return(nil).Once()

	task := collectClusterCapacityTask{
		config:            config,
		storage:           storage,
		nbsFactory:        nbsFactory,
		state:             &protos.CollectClusterCapacityTaskState{},
		expirationTimeout: time.Hour, // Can be any, storage is mocked.
	}

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.Equal(t, err, assert.AnError)
	// Only the successful cell should be in ProcessedCells.
	require.ElementsMatch(t, []string{"zone-a"}, task.state.ProcessedCells)

	mock.AssertExpectationsForObjects(
		t,
		execCtx,
		storage,
		nbsFactory,
		nbsClient1,
		nbsClient2,
	)
}

func TestCollectClusterCapacityFailureStorageReturnsError(t *testing.T) {
	ctx := newContext()
	execCtx := tasks_mocks.NewExecutionContextMock()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	config := &cells_config.CellsConfig{
		Cells: map[string]*cells_config.ZoneCells{
			"zone-a": {Cells: []string{"zone-a-cell1", "zone-a"}},
		},
	}

	capacityInfo := nbs.ClusterCapacityInfo{
		DiskKind:   types.DiskKind_DISK_KIND_SSD,
		FreeBytes:  1024,
		TotalBytes: 2048,
	}

	nbsFactory.On(
		"GetClient",
		ctx,
		mock.Anything, // For each cell we return the same mock client.
	).Return(nbsClient, nil).Twice()

	nbsClient.On("GetClusterCapacity", ctx).Return(
		[]nbs.ClusterCapacityInfo{capacityInfo},
		nil,
	).Twice()

	storage.On("UpdateClusterCapacities",
		ctx,
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     "zone-a",
				CellID:     "zone-a",
				Kind:       types.DiskKind_DISK_KIND_SSD,
				FreeBytes:  1024,
				TotalBytes: 2048,
			},
		},
		mock.Anything, // deleteOlderThan.
	).Return(nil).Once()
	storage.On("UpdateClusterCapacities",
		ctx,
		[]cells_storage.ClusterCapacity{
			{
				ZoneID:     "zone-a",
				CellID:     "zone-a-cell1",
				Kind:       types.DiskKind_DISK_KIND_SSD,
				FreeBytes:  1024,
				TotalBytes: 2048,
			},
		},
		mock.Anything, // deleteOlderThan.
	).Return(assert.AnError).Once()

	execCtx.On("SaveState", ctx).Return(nil).Once()

	task := collectClusterCapacityTask{
		config:            config,
		storage:           storage,
		nbsFactory:        nbsFactory,
		state:             &protos.CollectClusterCapacityTaskState{},
		expirationTimeout: time.Hour, // Can be any, storage is mocked.
	}

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.Equal(t, err, assert.AnError)
	// Only the successful cell should be in ProcessedCells.
	require.ElementsMatch(t, []string{"zone-a"}, task.state.ProcessedCells)

	mock.AssertExpectationsForObjects(
		t,
		execCtx,
		storage,
		nbsFactory,
		nbsClient,
	)
}
