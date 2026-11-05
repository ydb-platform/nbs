package dataplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	internal_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotDiskZoneTest struct {
	ctx        context.Context
	storage    *storage_mocks.StorageMock
	nbsFactory *nbs_mocks.FactoryMock
	nbsClient  *nbs_mocks.ClientMock
	execCtx    *tasks_mocks.ExecutionContextMock
	task       *createSnapshotFromDiskTask
}

func newCreateSnapshotDiskZoneTest(
	t *testing.T,
	zoneID string,
	useProxy bool,
	state *protos.CreateSnapshotFromDiskTaskState,
	disabledZones *internal_common.DisabledZones,
) *createSnapshotDiskZoneTest {

	t.Helper()

	f := &createSnapshotDiskZoneTest{
		ctx:        newContext(),
		storage:    storage_mocks.NewStorageMock(),
		nbsFactory: nbs_mocks.NewFactoryMock(),
		nbsClient:  nbs_mocks.NewClientMock(),
		execCtx:    tasks_mocks.NewExecutionContextMock(),
	}
	f.task = &createSnapshotFromDiskTask{
		config:        &config.DataplaneConfig{ProxyOverlayDiskIdPrefix: proto.String("proxy-")},
		disabledZones: disabledZones,
		storage:       f.storage,
		nbsFactory:    f.nbsFactory,
	}
	mocks := []*mock.Mock{
		&f.storage.Mock,
		&f.nbsFactory.Mock,
		&f.nbsClient.Mock,
		&f.execCtx.Mock,
	}

	for _, m := range mocks {
		m.Test(t)

		t.Cleanup(func() { m.AssertExpectations(t) })
	}

	request, err := proto.Marshal(&protos.CreateSnapshotFromDiskRequest{
		SrcDisk:             &types.Disk{DiskId: "disk", ZoneId: zoneID},
		SrcDiskCheckpointId: "checkpoint",
		DstSnapshotId:       "snapshot",
		UseProxyOverlayDisk: useProxy,
	})
	require.NoError(t, err)

	stateBytes, err := proto.Marshal(state)
	require.NoError(t, err)

	require.NoError(t, f.task.Load(request, stateBytes))

	return f
}

// Checks that disabled zones reject new and resumed transfers before any calls,
// preserve state, and expose the abort details through persisted task errors.
func TestCreateSnapshotFromDiskDisabledZone(t *testing.T) {
	testCases := []struct {
		name   string
		zoneID string
		state  *protos.CreateSnapshotFromDiskTaskState
	}{
		// A new task fails before creating snapshot metadata or contacting NBS.
		{
			name:   "new task",
			zoneID: "zone-d",
			state:  &protos.CreateSnapshotFromDiskTaskState{},
		},
		// A restored transfer keeps its progress, lock, and proxy state unchanged.
		{
			name:   "resumed task",
			zoneID: "zone-d-cell1",
			state: &protos.CreateSnapshotFromDiskTaskState{
				BaseSnapshotId:          "base",
				BaseCheckpointId:        "base-checkpoint",
				MilestoneChunkIndex:     3,
				TransferredChunkCount:   3,
				ChunkCount:              10,
				Progress:                0.3,
				ProxyOverlayDiskCreated: proto.Bool(true),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCreateSnapshotDiskZoneTest(
				t, tc.zoneID, true, tc.state,
				internal_common.NewDisabledZones([]string{tc.zoneID}),
			)

			err := f.task.Run(f.ctx, f.execCtx)

			var nonRetriable *errors.NonRetriableError
			require.ErrorAs(t, err, &nonRetriable)
			require.NotErrorIs(t, err, errors.NewEmptyAbortedError())
			require.NotErrorIs(t, err, errors.NewEmptyNonCancellableError())

			expectedDetails := &errors.ErrorDetails{
				Code: codes.Aborted,
				Message: fmt.Sprintf(
					"can't create snapshot %q from disk %q in disabled zone %q",
					"snapshot", "disk", tc.zoneID,
				),
				Internal: true,
			}
			var detailed *errors.DetailedError
			require.ErrorAs(t, err, &detailed)
			require.Equal(t, expectedDetails, detailed.Details)

			persisted := tasks_storage.TaskState{}
			persisted.SetError(err)
			require.Equal(t, expectedDetails, persisted.ErrorDetails)

			require.True(t, proto.Equal(tc.state, f.task.state))
			require.Empty(t, f.storage.Calls)
			require.Empty(t, f.nbsFactory.Calls)
			require.Empty(t, f.nbsClient.Calls)
			require.Empty(t, f.execCtx.Calls)
		})
	}
}

// Checks that allowed zones still create metadata and preserve an NBS Describe
// error, including when no disabled-zone policy is configured.
func TestCreateSnapshotFromDiskAllowedZone(t *testing.T) {
	testCases := []struct {
		name          string
		disabledZones []string
	}{
		// A missing policy retains the existing NBS path and error.
		{name: "missing policy"},
		// Disabling another zone does not affect this disk.
		{name: "another zone disabled", disabledZones: []string{"zone-b"}},
		// DisabledZones matches exact IDs and does not infer a cell's parent zone.
		{name: "parent zone disabled", disabledZones: []string{"zone-a"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCreateSnapshotDiskZoneTest(
				t, "zone-a-cell1", false,
				&protos.CreateSnapshotFromDiskTaskState{},
				internal_common.NewDisabledZones(tc.disabledZones),
			)
			// Keep Describe after metadata creation by supplying the known block size.
			f.task.request.BlockSize = 4096
			originalErr := errors.NewRetriableError(assert.AnError)

			f.execCtx.On("GetTaskID").Return("create").Twice()
			f.storage.On("CreateSnapshot", f.ctx, storage.SnapshotMeta{
				BlockSize:    4096,
				ID:           "snapshot",
				Disk:         f.task.request.SrcDisk,
				CheckpointID: "checkpoint",
				CreateTaskID: "create",
			}).Return(&storage.SnapshotMeta{ID: "snapshot"}, nil).Once()
			f.nbsFactory.On("GetClient", f.ctx, "zone-a-cell1").
				Return(f.nbsClient, nil).Once()
			f.nbsClient.On("Describe", f.ctx, "disk").
				Return(nbs_client.DiskParams{}, originalErr).Once()

			err := f.task.Run(f.ctx, f.execCtx)

			require.Same(t, originalErr, err)
		})
	}
}

// Checks that cancellation in a disabled zone skips NBS while preserving
// metadata deletion, saved base snapshot unlocking, and their errors.
func TestCreateSnapshotFromDiskCancelDisabled(t *testing.T) {
	originalErr := errors.NewRetriableError(assert.AnError)
	testCases := []struct {
		name        string
		useProxy    bool
		state       *protos.CreateSnapshotFromDiskTaskState
		deletingErr error
		unlockErr   error
	}{
		// A fresh task without a proxy still marks its snapshot as deleting.
		{name: "no proxy", state: &protos.CreateSnapshotFromDiskTaskState{}},
		// A proxy may exist even when its creation result was not saved.
		{name: "unsaved proxy", useProxy: true, state: &protos.CreateSnapshotFromDiskTaskState{}},
		// A restored task releases the saved base lock without deleting its proxy.
		{
			name:     "resumed task",
			useProxy: true,
			state: &protos.CreateSnapshotFromDiskTaskState{
				BaseSnapshotId:          "base",
				Progress:                0.5,
				MilestoneChunkIndex:     5,
				ProxyOverlayDiskCreated: proto.Bool(true),
			},
		},
		// A metadata error stops cleanup before attempting to unlock the base.
		{
			name:        "metadata error",
			useProxy:    true,
			state:       &protos.CreateSnapshotFromDiskTaskState{BaseSnapshotId: "base"},
			deletingErr: originalErr,
		},
		// An unlock error remains visible after metadata cleanup succeeds.
		{
			name:      "unlock error",
			useProxy:  true,
			state:     &protos.CreateSnapshotFromDiskTaskState{BaseSnapshotId: "base"},
			unlockErr: originalErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCreateSnapshotDiskZoneTest(
				t, "zone-d", tc.useProxy, tc.state,
				internal_common.NewDisabledZones([]string{"zone-d"}),
			)
			taskIDCalls := 1
			shouldUnlock := tc.state.BaseSnapshotId != "" && tc.deletingErr == nil
			if shouldUnlock {
				taskIDCalls++
			}

			f.execCtx.On("GetTaskID").Return("create").Times(taskIDCalls)
			f.storage.On("DeletingSnapshot", f.ctx, "snapshot", "create").
				Return(&storage.SnapshotMeta{}, tc.deletingErr).Once()
			if shouldUnlock {
				f.storage.On("UnlockSnapshot", f.ctx, "base", "create").
					Return(tc.unlockErr).Once()
			}

			err := f.task.Cancel(f.ctx, f.execCtx)

			if tc.deletingErr != nil {
				require.Same(t, tc.deletingErr, err)
			} else if tc.unlockErr != nil {
				require.Same(t, tc.unlockErr, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, proto.Equal(tc.state, f.task.state))
			require.Empty(t, f.nbsFactory.Calls)
			require.Empty(t, f.nbsClient.Calls)
		})
	}
}

// Checks that allowed-zone cancellation retains proxy deletion and stops on
// its original errors before metadata cleanup.
func TestCreateSnapshotFromDiskCancelAllowed(t *testing.T) {
	originalErr := errors.NewRetriableError(assert.AnError)
	testCases := []struct {
		name          string
		disabledZones []string
		clientErr     error
		deleteErr     error
	}{
		// A missing policy deletes the proxy and completes metadata cleanup.
		{name: "missing policy"},
		// Disabling another zone preserves the same cleanup path.
		{name: "another zone disabled", disabledZones: []string{"zone-d"}},
		// A client lookup error is returned before any cleanup mutation.
		{name: "client error", clientErr: originalErr},
		// A proxy deletion error prevents metadata deletion and base unlocking.
		{name: "proxy error", disabledZones: []string{"zone-d"}, deleteErr: originalErr},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCreateSnapshotDiskZoneTest(
				t, "zone-a", true,
				&protos.CreateSnapshotFromDiskTaskState{BaseSnapshotId: "base"},
				internal_common.NewDisabledZones(tc.disabledZones),
			)

			f.nbsFactory.On("GetClient", f.ctx, "zone-a").
				Return(f.nbsClient, tc.clientErr).Once()
			if tc.clientErr == nil {
				f.nbsClient.On("Delete", f.ctx, "proxy-disk_snapshot").
					Return(tc.deleteErr).Once()
			}
			if tc.clientErr == nil && tc.deleteErr == nil {
				f.execCtx.On("GetTaskID").Return("create").Twice()
				f.storage.On("DeletingSnapshot", f.ctx, "snapshot", "create").
					Return(&storage.SnapshotMeta{}, nil).Once()
				f.storage.On("UnlockSnapshot", f.ctx, "base", "create").
					Return(nil).Once()
			}

			err := f.task.Cancel(f.ctx, f.execCtx)

			if tc.clientErr != nil || tc.deleteErr != nil {
				require.Same(t, originalErr, err)
				require.Empty(t, f.storage.Calls)
				require.Empty(t, f.execCtx.Calls)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
