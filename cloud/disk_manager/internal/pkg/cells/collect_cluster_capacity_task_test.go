package cells

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/protos"
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

	capacity := nbs.ClusterCapacityInfo{
		DiskKind:   types.DiskKind_DISK_KIND_SSD,
		FreeBytes:  1024,
		TotalBytes: 2048,
	}

	nbsFactory.On(
		"GetClient",
		mock.Anything,
		mock.Anything,
	).Return(nbsClient, nil).Times(3)
	nbsClient.On("GetClusterCapacity", mock.Anything).Return(
		[]nbs.ClusterCapacityInfo{capacity},
		nil,
	).Times(3)

	storage.On("UpdateClusterCapacities",
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil).Times(3)

	execCtx.On("SaveState", mock.Anything).Return(nil).Once()

	task := collectClusterCapacityTask{
		config:     config,
		storage:    storage,
		nbsFactory: nbsFactory,
		state:      &protos.GetClusterCapacityState{},
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(
		t,
		execCtx,
		storage,
		nbsFactory,
		nbsClient,
	)
}
