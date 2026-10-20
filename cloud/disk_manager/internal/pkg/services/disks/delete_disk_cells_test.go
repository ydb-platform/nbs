package disks

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	pools_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestDeleteDiskTaskBackgroundCells(t *testing.T) {
	for _, sync := range []bool{false, true} {
		for _, failScheduling := range []bool{false, true} {
			t.Run(fmt.Sprintf("sync=%t/scheduling_error=%t", sync, failScheduling), func(t *testing.T) {
				ctx := context.Background()
				storage := storage_mocks.NewStorageMock()
				scheduler := tasks_mocks.NewSchedulerMock()
				factory := nbs_mocks.NewFactoryMock()
				pool := pools_mocks.NewServiceMock()
				execCtx := newExecutionContextMock()
				config := &cells_config.CellsConfig{Cells: map[string]*cells_config.ZoneCells{"zone": {Cells: []string{"cell-a", "cell-b"}}}}
				task := &deleteDiskTask{storage: storage, scheduler: scheduler, nbsFactory: factory, poolService: pool,
					cellSelector: cells.NewCellSelector(config, nil, nil, nil),
					request:      &protos.DeleteDiskRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, DeleteIfNonExistent: true, Sync: sync}, state: &protos.DeleteDiskTaskState{}}
				ids := []string{"toplevel_task_id_delete_disk_from_cell_cell-a", "toplevel_task_id_delete_disk_from_cell_cell-b"}
				storage.On("DeleteDisk", ctx, "disk", "toplevel_task_id", mock.Anything).Return((*resources.DiskMeta)(nil), nil).Once()
				storage.On("PrepareDiskCleanup", ctx, "disk", ids).Return(true, nil).Once()
				execCtx.On("SaveState", ctx).Return(nil).Twice()
				schedule := func(i int, err error) {
					scheduler.On("ScheduleNonCancellableTask", headers.SetIncomingIdempotencyKey(ctx, ids[i]), "disks.DeleteDiskFromCell", "", "",
						&protos.DeleteDiskFromCellRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: []string{"cell-a", "cell-b"}[i]}, Sync: sync, CleanupId: ids[i]}).Return("worker", err).Once()
				}
				schedule(0, nil)
				if failScheduling {
					schedule(1, assert.AnError)
				} else {
					schedule(1, nil)
				}
				err := task.Run(ctx, execCtx)
				if failScheduling {
					require.True(t, errors.Is(err, assert.AnError))
					// A resumed parent must not resolve a different cell set or touch metadata again.
					state, err := task.Save()
					require.NoError(t, err)
					request, err := proto.Marshal(task.request)
					require.NoError(t, err)
					require.NoError(t, task.Load(request, state))
					task.cellSelector = nil
					schedule(0, nil)
					schedule(1, nil)
					require.NoError(t, task.Run(ctx, execCtx))
				} else {
					require.NoError(t, err)
				}
				mock.AssertExpectationsForObjects(t, storage, scheduler, factory, pool, execCtx)
				scheduler.AssertNotCalled(t, "WaitTask", mock.Anything, mock.Anything, mock.Anything)
				factory.AssertNotCalled(t, "GetClient", mock.Anything, mock.Anything)
				storage.AssertNotCalled(t, "DiskDeleted", mock.Anything, mock.Anything, mock.Anything)
			})
		}
	}
}

func TestDeleteDiskFromCellTask(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pending  bool
		sync     bool
		errorAt  string
		notFound bool
	}{
		{name: "ordinary", pending: true}, {name: "sync", pending: true, sync: true},
		{name: "already completed"}, {name: "not found", pending: true, notFound: true},
		{name: "reservation read failure", pending: true, errorAt: "IsDiskCleanupPending"},
		{name: "incremental failure", pending: true, errorAt: "WaitTask"},
		{name: "unavailable cell", pending: true, errorAt: "GetClient"},
		{name: "delete failure", pending: true, errorAt: "Delete"},
		{name: "completion failure", pending: true, errorAt: "CompleteDiskCleanup"},
		{name: "task commit failure", pending: true, errorAt: "FinishWithPreparation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			storage := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			factory := nbs_mocks.NewFactoryMock()
			client := nbs_mocks.NewClientMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := &deleteDiskFromCellTask{deleteDiskTask: deleteDiskTask{storage: storage, scheduler: scheduler, nbsFactory: factory}}
			request, err := proto.Marshal(&protos.DeleteDiskFromCellRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: "cell"}, Sync: tc.sync, CleanupId: "cleanup"})
			require.NoError(t, err)
			require.NoError(t, task.Load(request, nil))
			errorFor := func(step string) error {
				if step == tc.errorAt {
					return assert.AnError
				}
				return nil
			}
			setup := func() {
				storage.On("IsDiskCleanupPending", ctx, "disk", "cleanup").Return(tc.pending, errorFor("IsDiskCleanupPending")).Once()
				if !tc.pending || tc.errorAt == "IsDiskCleanupPending" {
					return
				}
				scheduler.On("ScheduleTask", headers.SetIncomingIdempotencyKey(ctx, "cleanup_delete_disk_from_incremental"), "dataplane.DeleteDiskFromIncremental", "",
					&dataplane_protos.DeleteDiskFromIncrementalRequest{Disk: &types.Disk{DiskId: "disk", ZoneId: "cell"}}).Return("incremental", nil).Once()
				scheduler.On("WaitTask", ctx, execCtx, "incremental").Return(nil, errorFor("WaitTask")).Once()
				if tc.errorAt == "WaitTask" {
					return
				}
				factory.On("GetClient", ctx, "cell").Return(client, errorFor("GetClient")).Once()
				if tc.errorAt == "GetClient" {
					return
				}
				method := "Delete"
				if tc.sync {
					method = "DeleteSync"
				}
				deleteErr := errorFor("Delete")
				if tc.notFound {
					deleteErr = &nbs_client.ClientError{Code: nbs_client.E_NOT_FOUND}
				}
				client.On(method, ctx, "disk").Return(deleteErr).Once()
				if tc.errorAt == "Delete" {
					return
				}
				tx := &persistence.Transaction{}
				storage.On("CompleteDiskCleanup", ctx, tx, "disk", "cleanup").Return(errorFor("CompleteDiskCleanup")).Once()
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
				retry, ok := err.(*errors.RetriableError)
				require.True(t, ok)
				require.True(t, retry.IgnoreRetryLimit)
			} else {
				require.NoError(t, err)
				storage.On("IsDiskCleanupPending", ctx, "disk", "cleanup").Return(false, nil).Once()
				require.NoError(t, task.Run(ctx, execCtx))
			}
			mock.AssertExpectationsForObjects(t, storage, scheduler, factory, client, execCtx)
		})
	}
}

func TestDeleteDiskBackgroundReservationFailure(t *testing.T) {
	for _, prepareError := range []bool{false, true} {
		ctx := context.Background()
		storage := storage_mocks.NewStorageMock()
		scheduler := tasks_mocks.NewSchedulerMock()
		execCtx := newExecutionContextMock()
		task := &deleteDiskTask{storage: storage, scheduler: scheduler, request: &protos.DeleteDiskRequest{Disk: &types.Disk{DiskId: "disk"}}, state: &protos.DeleteDiskTaskState{CleanupCellIds: []string{"cell"}}}
		var e error
		if prepareError {
			e = assert.AnError
		}
		storage.On("PrepareDiskCleanup", ctx, "disk", []string{"toplevel_task_id_delete_disk_from_cell_cell"}).Return(false, e).Once()
		err := task.Run(ctx, execCtx)
		if prepareError {
			require.True(t, errors.Is(err, e))
		} else {
			require.NoError(t, err)
		}
		mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
	}
}
