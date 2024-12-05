package snapshots

import (
	"context"
	"testing"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func newExecutionContextMock() *tasks_mocks.ExecutionContextMock {
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("toplevel_task_id")
	return execCtx
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromDiskTaskFailure(t *testing.T) {
	ctx := context.Background()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := newExecutionContextMock()

	diskID := t.Name()
	zoneID := "zone"
	snapshotID := "snapshot"
	checkpointEnsuringError := errors.NewRetriableErrorf(
		"Creating checkpoint for disk with id %v ended with an error.",
		diskID,
	)

	diskParams := nbs.DiskParams{}
	request := &protos.CreateSnapshotFromDiskRequest{
		SrcDisk: &types.Disk{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		DstSnapshotId: snapshotID,
	}

	task := &createSnapshotFromDiskTask{
		performanceConfig: &performance_config.PerformanceConfig{},
		storage:           storage,
		scheduler:         scheduler,
		nbsFactory:        nbsFactory,
		request:           request,
		state:             &protos.CreateSnapshotFromDiskTaskState{},
	}

	nbsFactory.On("GetClient", ctx, zoneID).Return(nbsClient, nil)
	nbsClient.On("Describe", ctx, diskID).Return(diskParams, nil)

	storage.On("CreateSnapshot", ctx, mock.Anything).Return(
		resources.SnapshotMeta{
			Ready: false,
		},
		nil,
	)

	nbsClient.On("CreateCheckpoint", ctx, nbs.CheckpointParams{
		DiskID:       diskID,
		CheckpointID: snapshotID,
	}).Return(nil)

	nbsClient.On("EnsureCheckpointReady", ctx, diskID, snapshotID).Return(checkpointEnsuringError)

	err := task.Run(ctx, execCtx)

	require.Equal(t, err, checkpointEnsuringError)
	mock.AssertExpectationsForObjects(t, storage, scheduler, nbsFactory, nbsClient, execCtx)
}
