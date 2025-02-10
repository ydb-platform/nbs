package snapshots

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

// TODO:_ do we need these tests?

func TestCreateSnapshotFromDiskUpdatesCheckpointsCorrectly(t *testing.T) {
	zoneID := "zone-a"
	diskID := t.Name()

	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := resources_storage_mocks.NewStorageMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	request := &protos.CreateSnapshotFromDiskRequest{
		SrcDisk: &types.Disk{
			ZoneId: zoneID,
			DiskId: diskID,
		},
		DstSnapshotId:       "snapshotID",
		FolderId:            "folder",
		UseS3:               true,
		UseProxyOverlayDisk: true,
	}

	task := &createSnapshotFromDiskTask{
		performanceConfig: &performance_config.PerformanceConfig{},
		scheduler:         scheduler,
		storage:           storage,
		nbsFactory:        nbsFactory,
		request:           request,
		state:             &protos.CreateSnapshotFromDiskTaskState{},
	}

	require.Equal(t, int32(0), task.state.CheckpointIteration)
	require.Empty(t, task.state.CheckpointID)

	// Create first checkpoint and get checkpoint status 'Error'.
	execCtx.On("GetTaskID").Return("create_snapshot_from_disk_task")
	execCtx.On("SaveState", ctx).Return(nil)
	nbsFactory.On("GetClient", ctx, zoneID).Return(nbsClient, nil)
	nbsClient.On("Describe", ctx, diskID).Return(nbs.DiskParams{}, nil)
	storage.On("CreateSnapshot", ctx, mock.Anything).Return(
		resources.SnapshotMeta{Ready: false}, nil,
	)

	nbsClient.On("CreateCheckpoint", ctx, nbs.CheckpointParams{
		DiskID:       request.SrcDisk.DiskId,
		CheckpointID: "snapshotID",
	}).Return(nil)
	nbsClient.On(
		"GetCheckpointStatus",
		ctx,
		diskID,
		"snapshotID", // checkpointID
	).Return(nbs.CheckpointStatusError, nil)

	err := task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.CanRetry(err))
	require.Equal(t, int32(1), task.state.CheckpointIteration)
	require.Empty(t, task.state.CheckpointID)
	mock.AssertExpectationsForObjects(t, scheduler, storage, nbsFactory, nbsClient, execCtx)

	// Create second checkpoint and get checkpoint status 'Ready'.
	// Schedule dataplane task and start waiting for it.
	nbsClient.On("CreateCheckpoint", ctx, nbs.CheckpointParams{
		DiskID:       request.SrcDisk.DiskId,
		CheckpointID: "snapshotID",
	}).Unset()
	nbsClient.On(
		"DeleteCheckpoint", ctx, diskID, "snapshotID",
	).Return(nil).Once() // Should not update checkpoints after checkpoint got ready.
	nbsClient.On("CreateCheckpoint", ctx, nbs.CheckpointParams{
		DiskID:       request.SrcDisk.DiskId,
		CheckpointID: "snapshotID_1",
	}).Return(nil).Once()
	nbsClient.On(
		"GetCheckpointStatus",
		ctx,
		diskID,
		"snapshotID_1", // checkpointID
	).Return(nbs.CheckpointStatusReady, nil).Once()

	scheduler.On(
		"ScheduleZonalTask",
		mock.Anything, // ctx
		"dataplane.CreateSnapshotFromDisk",
		mock.Anything, // description
		zoneID,
		&dataplane_protos.CreateSnapshotFromDiskRequest{
			SrcDisk:             request.SrcDisk,
			SrcDiskCheckpointId: "snapshotID_1",
			DstSnapshotId:       request.DstSnapshotId,
			UseS3:               request.UseS3,
			UseProxyOverlayDisk: request.UseProxyOverlayDisk,
		},
	).Return("dataplane_task_id", nil)
	scheduler.On("WaitTask", ctx, execCtx, "dataplane_task_id").Return(
		mock.Anything, errors.NewInterruptExecutionError(),
	)

	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.Equal(t, int32(1), task.state.CheckpointIteration)
	require.Equal(t, task.state.CheckpointID, "snapshotID_1")
	mock.AssertExpectationsForObjects(t, scheduler, storage, nbsFactory, nbsClient, execCtx)

	// Finish waiting for the dataplane task, finish creating snapshot.
	scheduler.On(
		"WaitTask", ctx, execCtx, "dataplane_task_id",
	).Unset().On(
		"WaitTask", ctx, execCtx, "dataplane_task_id",
	).Return(
		&dataplane_protos.CreateSnapshotFromDiskResponse{}, nil,
	)
	execCtx.On("SetEstimate", mock.Anything)
	storage.On(
		"SnapshotCreated",
		ctx,
		request.DstSnapshotId,
		"snapshotID_1", // checkpointID
		mock.Anything,  // createdAt
		mock.Anything,  // snapshotSize
		mock.Anything,  // snapshotStorageSize
	).Return(nil)
	nbsClient.On(
		"DeleteCheckpointData", ctx, diskID, "snapshotID_1",
	).Return(nil)

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, int32(1), task.state.CheckpointIteration)
	require.Equal(t, task.state.CheckpointID, "snapshotID_1")
	mock.AssertExpectationsForObjects(t, scheduler, storage, nbsFactory, nbsClient, execCtx)

	// Cancel the task.
	// Use fresh nbs client in order not to interfere with previous calls.
	nbsClientForCancel := nbs_mocks.NewClientMock()
	nbsFactory.On(
		"GetClient", ctx, zoneID,
	).Unset().On(
		"GetClient", ctx, zoneID,
	).Return(nbsClientForCancel, nil)

	nbsClientForCancel.On(
		"DeleteCheckpoint", ctx, diskID, "snapshotID",
	).Return(nil)
	nbsClientForCancel.On(
		"DeleteCheckpoint", ctx, diskID, "snapshotID_1",
	).Return(nil)

	var m *resources.SnapshotMeta
	storage.On(
		"DeleteSnapshot",
		ctx,
		request.DstSnapshotId,
		"create_snapshot_from_disk_task",
		mock.Anything, //deletingAt
	).Return(m, nil)

	err = task.Cancel(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, int32(1), task.state.CheckpointIteration)
	require.Equal(t, task.state.CheckpointID, "snapshotID_1")
	mock.AssertExpectationsForObjects(
		t,
		scheduler,
		storage,
		nbsFactory,
		nbsClient,
		nbsClientForCancel,
		execCtx,
	)
}
