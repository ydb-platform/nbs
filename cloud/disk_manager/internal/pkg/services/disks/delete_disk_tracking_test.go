package disks

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestDeleteDiskTaskTrackedDeletion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prepared bool
		sync     bool
		errorAt  string
	}{
		{name: "legacy async", prepared: true}, {name: "legacy sync", prepared: true, sync: true},
		{name: "completed deletion"}, {name: "reservation read error", errorAt: "PrepareDiskCleanup"},
		{name: "incremental retry keeps reservation", prepared: true, errorAt: "WaitTask"},
		{name: "NBS retry keeps reservation", prepared: true, errorAt: "Delete"},
		{name: "completion callback failure", prepared: true, errorAt: "CompleteDiskCleanup"},
		{name: "task commit failure", prepared: true, errorAt: "FinishWithPreparation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			storage := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			factory := nbs_mocks.NewFactoryMock()
			client := nbs_mocks.NewClientMock()
			execCtx := newExecutionContextMock()
			task := &deleteDiskTask{storage: storage, scheduler: scheduler, nbsFactory: factory}
			request, err := proto.Marshal(&protos.DeleteDiskRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: "cell"}, DeleteIfNonExistent: true, Sync: tc.sync})
			require.NoError(t, err)
			require.NoError(t, task.Load(request, nil))
			id := "legacy_disk_cleanup_toplevel_task_id"
			errorFor := func(step string) error {
				if tc.errorAt == step {
					return assert.AnError
				}
				return nil
			}
			storage.On("DeleteDisk", ctx, "disk", "toplevel_task_id", mock.Anything, false).Return((*resources.DiskMeta)(nil), nil).Once()
			storage.On("PrepareDiskCleanup", ctx, "disk", []string{id}, true).Return(tc.prepared, errorFor("PrepareDiskCleanup")).Once()
			setup := func() {
				if !tc.prepared || tc.errorAt == "PrepareDiskCleanup" {
					return
				}
				scheduler.On("ScheduleTask", headers.SetIncomingIdempotencyKey(ctx, "toplevel_task_id_delete_disk_from_incremental"), "dataplane.DeleteDiskFromIncremental", "", &dataplane_protos.DeleteDiskFromIncrementalRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: "cell"}}).Return("incremental", nil).Once()
				scheduler.On("WaitTask", ctx, execCtx, "incremental").Return(nil, errorFor("WaitTask")).Once()
				if tc.errorAt == "WaitTask" {
					return
				}
				factory.On("GetClient", ctx, "cell").Return(client, nil).Once()
				method := "Delete"
				if tc.sync {
					method = "DeleteSync"
				}
				client.On(method, ctx, "disk").Return(errorFor("Delete")).Once()
				if tc.errorAt == "Delete" {
					return
				}
				tx := &persistence.Transaction{}
				storage.On("CompleteDiskCleanup", ctx, tx, "disk", id).Return(errorFor("CompleteDiskCleanup")).Once()
				finishErr := errorFor("CompleteDiskCleanup")
				if finishErr == nil {
					finishErr = errorFor("FinishWithPreparation")
				}
				execCtx.On("FinishWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
					prep := args.Get(1).(func(context.Context, *persistence.Transaction) error)
					require.Equal(t, errorFor("CompleteDiskCleanup"), prep(ctx, tx))
				}).Return(finishErr).Once()
			}
			setup()
			err = task.Run(ctx, execCtx)
			if tc.errorAt != "" {
				require.True(t, errors.Is(err, assert.AnError))
			} else {
				require.NoError(t, err)
			}
			mock.AssertExpectationsForObjects(t, storage, scheduler, factory, client, execCtx)
			storage.AssertNotCalled(t, "DiskDeleted", mock.Anything, mock.Anything, mock.Anything)
			scheduler.AssertNotCalled(t, "ScheduleNonCancellableTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		})
	}
}
