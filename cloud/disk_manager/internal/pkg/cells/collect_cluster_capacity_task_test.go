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
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCollectClusterCapacityTask(t *testing.T) {
	ctx := context.Background()
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
		ctx,
		mock.Anything,
	).Return(nbsClient, nil).Times(3)
	nbsClient.On("GetClusterCapacity", ctx).Return(
		[]nbs.ClusterCapacityInfo{capacity},
		nil,
	).Times(3)

	storage.On("UpdateClusterCapacities",
		ctx,
		mock.Anything,
		mock.Anything,
	).Return(nil).Times(3)

	execCtx.On("SaveState", ctx).Return(nil).Times(3)

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
