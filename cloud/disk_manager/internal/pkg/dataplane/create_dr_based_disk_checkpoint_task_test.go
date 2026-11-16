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

// Checks that initial and resumed runs save the estimate before waiting for a checkpoint.
func TestCreateDRBasedDiskCheckpointSavesEstimate(t *testing.T) {
	ctx := newContext()
	performanceConfig := &performance_config.PerformanceConfig{
		CreateDRBasedDiskCheckpointBandwidthMiBs: proto.Uint64(1),
	}

	request, err := proto.Marshal(&protos.CreateDRBasedDiskCheckpointRequest{
		Disk:               &types.Disk{ZoneId: "zone", DiskId: "disk"},
		CheckpointIdPrefix: "checkpoint",
	})
	require.NoError(t, err)

	run := func(state []byte) []byte {
		t.Helper()
		nbsFactory := nbs_mocks.NewFactoryMock()
		nbsClient := nbs_mocks.NewClientMock()
		execCtx := tasks_mocks.NewExecutionContextMock()
		task := &createDRBasedDiskCheckpointTask{
			performanceConfig: performanceConfig,
			nbsFactory:        nbsFactory,
		}
		require.NoError(t, task.Load(request, state))

		nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil).Twice()
		nbsClient.On("Describe", ctx, "disk").Return(nbs_client.DiskParams{
			BlocksCount: 256,
			BlockSize:   4096,
		}, nil).Once()

		setEstimate := execCtx.On("SetEstimatedStallingDuration", time.Second).Once()
		var savedState []byte
		saveState := execCtx.On("SaveState", ctx).Run(func(mock.Arguments) {
			var saveErr error
			savedState, saveErr = task.Save()
			require.NoError(t, saveErr)
		}).Return(nil).Once().NotBefore(setEstimate)

		nbsClient.On("CreateCheckpoint", ctx, nbs_client.CheckpointParams{
			DiskID:       "disk",
			CheckpointID: "checkpoint_0",
		}).Return(nil).Once().NotBefore(saveState)

		interruptErr := errors.NewInterruptExecutionError()
		nbsClient.On(
			"EnsureCheckpointReady", ctx, "disk", "checkpoint_0",
		).Return(interruptErr).Once().NotBefore(saveState)

		err := task.Run(ctx, execCtx)

		require.True(t, errors.Is(err, interruptErr))
		mock.AssertExpectationsForObjects(t, nbsFactory, nbsClient, execCtx)
		return savedState
	}

	savedState := run(nil)
	run(savedState)
}
