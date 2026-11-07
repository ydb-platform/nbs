package dataplane

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteSnapshotTaskDisabledZones(t *testing.T) {
	disabledZone := common.NewDisabledZones([]string{"zone-d"})
	disabledShard := common.NewDisabledZones([]string{"zone-d-shard1"})

	testCases := []struct {
		name          string
		disabledZones *common.DisabledZones
		zoneID        string
		noCheckpoint  bool
		expectNBS     bool
		cancel        bool
		unlockErr     error
	}{
		{name: "disabled zone", disabledZones: disabledZone, zoneID: "zone-d"},
		{name: "disabled shard", disabledZones: disabledShard},
		{name: "regional zone does not disable shard", disabledZones: disabledZone, expectNBS: true},
		{name: "allowed zone", disabledZones: common.NewDisabledZones([]string{"zone-a"}), expectNBS: true},
		{name: "missing policy", expectNBS: true},
		{name: "empty policy", disabledZones: common.NewDisabledZones(nil), expectNBS: true},
		{name: "empty checkpoint", disabledZones: disabledShard, noCheckpoint: true},
		{name: "cancel disabled", disabledZones: disabledShard, cancel: true},
		{name: "unlock error after skip", disabledZones: disabledShard, unlockErr: assert.AnError},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newContext()
			storageMock := storage_mocks.NewStorageMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsClient := nbs_mocks.NewClientMock()
			execCtx := tasks_mocks.NewExecutionContextMock()

			zoneID := tc.zoneID
			if len(zoneID) == 0 {
				zoneID = "zone-d-shard1"
			}

			snapshotMeta := &storage.SnapshotMeta{
				Disk:           &types.Disk{ZoneId: zoneID, DiskId: "disk"},
				CheckpointID:   "checkpoint",
				BaseSnapshotID: "base-snapshot",
				CreateTaskID:   "create-task",
			}
			if tc.noCheckpoint {
				snapshotMeta.CheckpointID = ""
				snapshotMeta.Disk = nil
			}

			execCtx.On("GetTaskID").Return("delete-task").Once()
			storageMock.On("DeletingSnapshot", ctx, "snapshot", "delete-task").Return(snapshotMeta, nil).Once()
			storageMock.On("UnlockSnapshot", ctx, "base-snapshot", "create-task").Return(tc.unlockErr).Once()
			if tc.expectNBS {
				nbsFactory.On("GetClient", ctx, zoneID).Return(nbsClient, nil).Once()
				nbsClient.On("DeleteCheckpoint", ctx, "disk", "checkpoint").Return(nil).Once()
			}

			task := &deleteSnapshotTask{
				storage:       storageMock,
				nbsFactory:    nbsFactory,
				disabledZones: tc.disabledZones,
				request:       &protos.DeleteSnapshotRequest{SnapshotId: "snapshot"},
			}
			var err error
			if tc.cancel {
				err = task.Cancel(ctx, execCtx)
			} else {
				err = task.Run(ctx, execCtx)
			}
			if tc.unlockErr != nil {
				require.True(t, errors.Is(err, tc.unlockErr))
			} else {
				require.NoError(t, err)
			}

			if !tc.expectNBS {
				nbsFactory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
				nbsClient.AssertNotCalled(t, "DeleteCheckpoint", mock.Anything, mock.Anything, mock.Anything)
			}
			mock.AssertExpectationsForObjects(t, storageMock, nbsFactory, nbsClient, execCtx)
		})
	}
}
