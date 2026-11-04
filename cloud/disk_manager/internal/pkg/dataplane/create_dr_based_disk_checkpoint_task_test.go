package dataplane

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestCreateDRBasedDiskCheckpointTaskSavesEstimateOnce(t *testing.T) {
	ctx := newContext()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()

	execCtx := tasks_mocks.NewExecutionContextMock()
	resumedExecCtx := tasks_mocks.NewExecutionContextMock()
	performanceConfig := &performance_config.PerformanceConfig{
		CreateDRBasedDiskCheckpointBandwidthMiBs: proto.Uint64(1),
	}

	request, err := proto.Marshal(&protos.CreateDRBasedDiskCheckpointRequest{
		Disk:               &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CheckpointIdPrefix: "checkpoint",
	})
	require.NoError(t, err)

	task := &createDRBasedDiskCheckpointTask{
		performanceConfig: performanceConfig,
		nbsFactory:        nbsFactory,
	}
	require.NoError(t, task.Load(request, nil))

	nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil).Times(3)
	nbsClient.On("Describe", ctx, "disk").Return(nbs_client.DiskParams{
		BlocksCount: 256,
		BlockSize:   4096,
	}, nil).Once()

	var estimatedStallingDuration time.Duration
	execCtx.On("SetEstimatedStallingDuration", time.Second).Run(
		func(args mock.Arguments) {
			estimatedStallingDuration = args.Get(0).(time.Duration)
		},
	).Once()

	var savedState []byte
	execCtx.On("SaveState", ctx).Run(func(mock.Arguments) {
		require.Equal(t, time.Second, estimatedStallingDuration)
		var saveErr error
		savedState, saveErr = task.Save()
		require.NoError(t, saveErr)
		state := &protos.CreateDRBasedDiskCheckpointTaskState{}
		require.NoError(t, proto.Unmarshal(savedState, state))
		require.True(t, state.EstimateSaved)
	}).Return(nil).Once()

	nbsClient.On("CreateCheckpoint", ctx, nbs_client.CheckpointParams{
		DiskID:       "disk",
		CheckpointID: "checkpoint_0",
	}).Run(func(mock.Arguments) {
		require.NotEmpty(t, savedState)
	}).Return(nil).Twice()

	interruptErr := errors.NewInterruptExecutionError()
	nbsClient.On(
		"EnsureCheckpointReady", ctx, "disk", "checkpoint_0",
	).Return(interruptErr).Twice()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, interruptErr))

	resumedTask := &createDRBasedDiskCheckpointTask{
		performanceConfig: performanceConfig,
		nbsFactory:        nbsFactory,
	}
	require.NoError(t, resumedTask.Load(request, savedState))

	err = resumedTask.Run(ctx, resumedExecCtx)
	require.True(t, errors.Is(err, interruptErr))
	resumedExecCtx.AssertNotCalled(
		t, "SetEstimatedStallingDuration", mock.Anything,
	)
	resumedExecCtx.AssertNotCalled(t, "SaveState", mock.Anything)
	mock.AssertExpectationsForObjects(
		t, nbsFactory, nbsClient, execCtx, resumedExecCtx,
	)
}
