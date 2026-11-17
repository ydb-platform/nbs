package dataplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func newCreateDRCheckpointDisabledZonesTask(
	t *testing.T,
	nbsFactory nbs.Factory,
	disabledZones *common.DisabledZones,
	zoneID string,
	state *protos.CreateDRBasedDiskCheckpointTaskState,
) *createDRBasedDiskCheckpointTask {

	t.Helper()

	task := &createDRBasedDiskCheckpointTask{
		disabledZones: disabledZones,
		nbsFactory:    nbsFactory,
	}

	requestBytes, err := proto.Marshal(&protos.CreateDRBasedDiskCheckpointRequest{
		Disk: &types.Disk{
			ZoneId: zoneID,
			DiskId: "disk",
		},
		CheckpointIdPrefix: "checkpoint",
	})
	require.NoError(t, err)

	stateBytes, err := proto.Marshal(state)
	require.NoError(t, err)

	require.NoError(t, task.Load(requestBytes, stateBytes))

	return task
}

// Checks that disabled zones reject new and restored tasks with internal Aborted
// details, while Run and Cancel leave state unchanged and make no NBS calls.
func TestCreateDRCheckpointDisabledZone(t *testing.T) {
	testCases := []struct {
		name         string
		zoneID       string
		iteration    int32
		checkpointID string
	}{
		// A new task fails before the NBS-backed duration estimate.
		{
			name:   "new task",
			zoneID: "zone-d",
		},
		// A restored retry does not delete the previous checkpoint or create another.
		{
			name:      "restored iteration",
			zoneID:    "zone-d",
			iteration: 3,
		},
		// A stored checkpoint result does not bypass the disabled-zone check.
		{
			name:         "restored checkpoint",
			zoneID:       "zone-d",
			iteration:    3,
			checkpointID: "checkpoint_3",
		},
		// An exact cell entry also blocks execution and checkpoint cleanup.
		{
			name:         "disabled cell",
			zoneID:       "zone-d-shard1",
			iteration:    2,
			checkpointID: "checkpoint_2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := logging.SetLogger(
				context.Background(),
				logging.NewStderrLogger(logging.WarnLevel),
			)
			nbsFactory := nbs_mocks.NewFactoryMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := newCreateDRCheckpointDisabledZonesTask(
				t,
				nbsFactory,
				common.NewDisabledZones([]string{tc.zoneID}),
				tc.zoneID,
				&protos.CreateDRBasedDiskCheckpointTaskState{
					CheckpointIteration: tc.iteration,
					CheckpointId:        tc.checkpointID,
				},
			)
			before, err := task.Save()
			require.NoError(t, err)

			err = task.Run(ctx, execCtx)

			var nonRetriable *errors.NonRetriableError
			require.ErrorAs(t, err, &nonRetriable)

			var detailed *errors.DetailedError
			require.ErrorAs(t, err, &detailed)
			expectedDetails := &errors.ErrorDetails{
				Code: codes.Aborted,
				Message: fmt.Sprintf(
					"can't create checkpoint %q for disk %q in disabled zone %q",
					fmt.Sprintf("checkpoint_%v", tc.iteration),
					"disk",
					tc.zoneID,
				),
				Internal: true,
			}
			require.Equal(t, expectedDetails, detailed.Details)

			var taskState tasks_storage.TaskState
			taskState.SetError(err)
			require.Equal(t, expectedDetails, taskState.ErrorDetails)

			afterRun, err := task.Save()
			require.NoError(t, err)
			require.Equal(t, before, afterRun)
			require.Empty(t, nbsFactory.Calls)
			require.Empty(t, execCtx.Calls)

			err = task.Cancel(ctx, execCtx)

			require.NoError(t, err)

			afterCancel, err := task.Save()
			require.NoError(t, err)
			require.Equal(t, before, afterCancel)
			require.Empty(t, nbsFactory.Calls)
			require.Empty(t, execCtx.Calls)
		})
	}
}

// Checks that missing, empty, and non-matching policies preserve the original
// GetClient or Describe error, including literal regional-zone and cell matching.
func TestCreateDRCheckpointAllowedZone(t *testing.T) {
	testCases := []struct {
		name          string
		disabledZones *common.DisabledZones
		zoneID        string
		describe      bool
	}{
		// A missing policy preserves the client lookup error.
		{
			name:   "missing policy",
			zoneID: "zone-a",
		},
		// An empty policy preserves the Describe error during estimation.
		{
			name:          "empty policy",
			disabledZones: common.NewDisabledZones(nil),
			zoneID:        "zone-a",
			describe:      true,
		},
		// Disabling an unrelated zone does not alter the Describe error.
		{
			name:          "unrelated zone",
			disabledZones: common.NewDisabledZones([]string{"zone-d"}),
			zoneID:        "zone-a",
			describe:      true,
		},
		// A regional entry does not disable a cell with a different literal ID.
		{
			name:          "region does not match cell",
			disabledZones: common.NewDisabledZones([]string{"zone-d"}),
			zoneID:        "zone-d-shard1",
		},
		// A cell entry does not disable the corresponding regional endpoint.
		{
			name:          "cell does not match region",
			disabledZones: common.NewDisabledZones([]string{"zone-d-shard1"}),
			zoneID:        "zone-d",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsClient := nbs_mocks.NewClientMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := newCreateDRCheckpointDisabledZonesTask(
				t,
				nbsFactory,
				tc.disabledZones,
				tc.zoneID,
				&protos.CreateDRBasedDiskCheckpointTaskState{},
			)
			expectedErr := errors.NewRetriableError(assert.AnError)

			if tc.describe {
				nbsFactory.On("GetClient", ctx, tc.zoneID).
					Return(nbsClient, nil).Once()
				nbsClient.On("Describe", ctx, "disk").
					Return(nbs.DiskParams{}, expectedErr).Once()
			} else {
				nbsFactory.On("GetClient", ctx, tc.zoneID).
					Return(nil, expectedErr).Once()
			}

			err := task.Run(ctx, execCtx)

			require.Same(t, expectedErr, err)
			require.Empty(t, execCtx.Calls)
			mock.AssertExpectationsForObjects(t, nbsFactory, nbsClient)
		})
	}
}

// Checks that allowed-zone Cancel retains checkpoint deletion and its existing
// error behavior, including the ignored client lookup error.
func TestCreateDRCheckpointCancelAllowedZone(t *testing.T) {
	testCases := []struct {
		name          string
		disabledZones *common.DisabledZones
		iteration     int32
		clientErr     error
		previousErr   error
		currentErr    error
	}{
		// Without a policy, a fresh task deletes its current checkpoint.
		{
			name: "missing policy",
		},
		// An allowed restored task deletes both previous and current checkpoints.
		{
			name:          "restored iteration",
			disabledZones: common.NewDisabledZones([]string{"zone-d"}),
			iteration:     3,
		},
		// The existing Cancel path treats a client lookup failure as success.
		{
			name:          "client error",
			disabledZones: common.NewDisabledZones([]string{"zone-d"}),
			iteration:     3,
			clientErr:     assert.AnError,
		},
		// A previous-checkpoint deletion error stops cleanup before the current one.
		{
			name:          "previous checkpoint error",
			disabledZones: common.NewDisabledZones([]string{"zone-d"}),
			iteration:     3,
			previousErr:   assert.AnError,
		},
		// A current-checkpoint deletion error is returned unchanged.
		{
			name:          "current checkpoint error",
			disabledZones: common.NewDisabledZones([]string{"zone-d"}),
			iteration:     3,
			currentErr:    assert.AnError,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsClient := nbs_mocks.NewClientMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := newCreateDRCheckpointDisabledZonesTask(
				t,
				nbsFactory,
				tc.disabledZones,
				"zone-a",
				&protos.CreateDRBasedDiskCheckpointTaskState{
					CheckpointIteration: tc.iteration,
				},
			)

			nbsFactory.On("GetClient", ctx, "zone-a").
				Return(nbsClient, tc.clientErr).Once()
			if tc.clientErr == nil {
				if tc.iteration > 0 {
					nbsClient.On(
						"DeleteCheckpoint",
						ctx,
						"disk",
						fmt.Sprintf("checkpoint_%v", tc.iteration-1),
					).Return(tc.previousErr).Once()
				}
				if tc.previousErr == nil {
					nbsClient.On(
						"DeleteCheckpoint",
						ctx,
						"disk",
						fmt.Sprintf("checkpoint_%v", tc.iteration),
					).Return(tc.currentErr).Once()
				}
			}

			err := task.Cancel(ctx, execCtx)

			if tc.previousErr != nil {
				require.Same(t, tc.previousErr, err)
			} else if tc.currentErr != nil {
				require.Same(t, tc.currentErr, err)
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, execCtx.Calls)
			mock.AssertExpectationsForObjects(t, nbsFactory, nbsClient)
		})
	}
}
