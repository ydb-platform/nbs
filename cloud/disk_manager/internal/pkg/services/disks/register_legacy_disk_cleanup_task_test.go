package disks

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	resource_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

func legacyDeleteForRegistration(t *testing.T, id, diskID string) tasks_storage.TaskState {
	request, err := proto.Marshal(&protos.DeleteDiskRequest{Disk: &types.Disk{DiskId: diskID, ZoneId: "unavailable-cell"}, DeleteIfNonExistent: true})
	require.NoError(t, err)
	return tasks_storage.TaskState{ID: id, Request: request, Status: tasks_storage.TaskStatusWaitingToRun}
}

func TestRegisterLegacyDiskCleanupResumesPagesWithoutWaitingForCells(t *testing.T) {
	ctx := context.Background()
	storage := resource_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	task := &registerLegacyDiskCleanupTask{storage: storage, scheduler: scheduler}
	require.NoError(t, task.Load(nil, nil))
	var saved []byte
	execCtx.On("SaveState", ctx).Run(func(mock.Arguments) {
		var err error
		saved, err = task.Save()
		require.NoError(t, err)
	}).Return(nil).Twice()
	first := []tasks_storage.TaskState{legacyDeleteForRegistration(t, "a", "disk"), legacyDeleteForRegistration(t, "b", "disk")}
	scheduler.On("ListUnfinishedTasks", ctx, "disks.DeleteDisk", "", legacyDiskCleanupPageSize).Return(first, nil).Once()
	for _, id := range []string{"a", "b"} {
		key := legacyDiskCleanupPrefix + id
		storage.On("PrepareLegacyDiskCleanup", ctx, "disk", key).Return(true, nil).Once()
		scheduler.On("WaitTaskEndedWithTimeout", ctx, id, time.Duration(0)).Return(errors.NewInterruptExecutionError()).Once()
		scheduler.On("ScheduleNonCancellableTask", headers.SetIncomingIdempotencyKey(ctx, key+"_finish"), "disks.FinishLegacyDiskCleanup", "", "",
			&protos.FinishLegacyDiskCleanupRequest{DiskId: "disk", LegacyTaskId: id, CleanupId: key}).Return("observer-"+id, nil).Once()
	}
	require.ErrorIs(t, task.Run(ctx, execCtx), errors.NewInterruptExecutionError())
	require.NoError(t, task.Load(nil, saved))
	second := []tasks_storage.TaskState{legacyDeleteForRegistration(t, "c", "known-disk")}
	scheduler.On("ListUnfinishedTasks", ctx, "disks.DeleteDisk", "b", legacyDiskCleanupPageSize).Return(second, nil).Once()
	storage.On("PrepareLegacyDiskCleanup", ctx, "known-disk", legacyDiskCleanupPrefix+"c").Return(false, nil).Once()
	require.ErrorIs(t, task.Run(ctx, execCtx), errors.NewInterruptExecutionError())
	require.NoError(t, task.Load(nil, saved))
	scheduler.On("ListUnfinishedTasks", ctx, "disks.DeleteDisk", "c", legacyDiskCleanupPageSize).Return([]tasks_storage.TaskState(nil), nil).Once()
	require.NoError(t, task.Run(ctx, execCtx))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
	scheduler.AssertNotCalled(t, "WaitTask", mock.Anything, mock.Anything, mock.Anything)
}

func TestRegisterLegacyDiskCleanupRetriesWithoutSkippingPage(t *testing.T) {
	for _, failure := range []string{"list", "reserve", "status", "schedule", "save"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			storage := resource_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := &registerLegacyDiskCleanupTask{storage: storage, scheduler: scheduler,
				state: &protos.RegisterLegacyDiskCleanupTaskState{AfterTaskId: "previous"}}
			persisted, err := task.Save()
			require.NoError(t, err)
			page := []tasks_storage.TaskState{legacyDeleteForRegistration(t, "parent", "disk")}
			key := legacyDiskCleanupPrefix + "parent"
			prepare := func(fail string) {
				errFor := func(step string) error {
					if step == fail {
						return assert.AnError
					}
					return nil
				}
				scheduler.On("ListUnfinishedTasks", ctx, "disks.DeleteDisk", "previous", legacyDiskCleanupPageSize).Return(page, errFor("list")).Once()
				if fail == "list" {
					return
				}
				storage.On("PrepareLegacyDiskCleanup", ctx, "disk", key).Return(true, errFor("reserve")).Once()
				if fail == "reserve" {
					return
				}
				var statusErr error = errors.NewInterruptExecutionError()
				if fail == "status" {
					statusErr = assert.AnError
				}
				scheduler.On("WaitTaskEndedWithTimeout", ctx, "parent", time.Duration(0)).Return(statusErr).Once()
				if fail == "status" {
					return
				}
				scheduler.On("ScheduleNonCancellableTask", mock.Anything, "disks.FinishLegacyDiskCleanup", "", "",
					&protos.FinishLegacyDiskCleanupRequest{DiskId: "disk", LegacyTaskId: "parent", CleanupId: key}).Return("observer", errFor("schedule")).Once()
				if fail == "schedule" {
					return
				}
				execCtx.On("SaveState", ctx).Return(errFor("save")).Once()
			}
			prepare(failure)
			require.ErrorIs(t, task.Run(ctx, execCtx), assert.AnError)
			require.NoError(t, task.Load(nil, persisted))
			prepare("")
			require.ErrorIs(t, task.Run(ctx, execCtx), errors.NewInterruptExecutionError())
			require.Equal(t, "parent", task.state.AfterTaskId)
			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
		})
	}
}

func TestRegisterLegacyDiskCleanupSkipsBackgroundAndNonDeletingRequests(t *testing.T) {
	ctx := context.Background()
	storage := resource_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	task := &registerLegacyDiskCleanupTask{storage: storage, scheduler: scheduler,
		state: &protos.RegisterLegacyDiskCleanupTaskState{}}
	var page []tasks_storage.TaskState
	for _, request := range []*protos.DeleteDiskRequest{
		{Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}, DeleteIfNonExistent: true, BackgroundCleanup: true},
		{Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}},
		{Disk: &types.Disk{DiskId: "disk"}, DeleteIfNonExistent: true},
	} {
		data, err := proto.Marshal(request)
		require.NoError(t, err)
		page = append(page, tasks_storage.TaskState{ID: "task", Request: data})
	}
	resumed := legacyDeleteForRegistration(t, "resumed-background", "disk")
	var err error
	resumed.State, err = proto.Marshal(&protos.DeleteDiskTaskState{CleanupCellIds: []string{"cell"}})
	require.NoError(t, err)
	page = append(page, resumed)
	scheduler.On("ListUnfinishedTasks", ctx, "disks.DeleteDisk", "", legacyDiskCleanupPageSize).Return(page, nil).Once()
	execCtx.On("SaveState", ctx).Return(nil).Once()
	require.ErrorIs(t, task.Run(ctx, execCtx), errors.NewInterruptExecutionError())
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestBackgroundCleanupWaitsOnlyForAutomaticRegistration(t *testing.T) {
	ctx := headers.SetIncomingIdempotencyKey(headers.SetAccountID(context.Background(), "customer"), "customer-key")
	storage := resource_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := newExecutionContextMock()
	task := &deleteDiskTask{storage: storage, scheduler: scheduler,
		request: &protos.DeleteDiskRequest{Disk: &types.Disk{DiskId: "disk"}},
		state:   &protos.DeleteDiskTaskState{CleanupCellIds: []string{"unavailable-cell"}}}
	scheduler.On("ScheduleNonCancellableTask", mock.MatchedBy(func(c context.Context) bool {
		return headers.GetAccountID(c) == "" && headers.GetIdempotencyKey(c) == registerLegacyDiskCleanupTaskType
	}), registerLegacyDiskCleanupTaskType, "", "", mock.Anything).Return("registration", nil).Once()
	execCtx.On("SaveState", ctx).Return(nil).Times(3)
	scheduler.On("WaitTask", ctx, execCtx, "registration").Return(nil, errors.NewInterruptExecutionError()).Once()
	require.ErrorIs(t, task.Run(ctx, execCtx), errors.NewInterruptExecutionError())
	storage.AssertNotCalled(t, "PrepareDiskCleanup", mock.Anything, mock.Anything, mock.Anything)
	require.Equal(t, "customer-key", headers.GetIdempotencyKey(ctx))
	saved, err := task.Save()
	require.NoError(t, err)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)
	require.NoError(t, task.Load(request, saved))
	// Disabling the flag does not abandon a parent that already saved its cells.
	require.False(t, task.request.BackgroundCleanup)
	scheduler.On("WaitTask", ctx, execCtx, "registration").Return(nil, nil).Once()
	ids := []string{"toplevel_task_id_delete_disk_from_cell_unavailable-cell"}
	storage.On("PrepareDiskCleanup", ctx, "disk", ids).Return(true, nil).Once()
	storage.On("GetDiskCleanupIDs", ctx, "disk").Return([]string(nil), nil).Once()
	scheduler.On("ScheduleNonCancellableTask", mock.MatchedBy(func(c context.Context) bool {
		return headers.GetIdempotencyKey(c) == ids[0] && headers.GetAccountID(c) == "customer"
	}), "disks.DeleteDiskFromCell", "", "", mock.Anything).Return("cell-worker", nil).Once()
	require.NoError(t, task.Run(ctx, execCtx))
	require.True(t, task.state.LegacyCleanupRegistered)
	scheduler.AssertNotCalled(t, "WaitTask", mock.Anything, mock.Anything, "cell-worker")
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestAutomaticRegistrationGateRetriesFailures(t *testing.T) {
	for _, failure := range []string{"schedule", "save-id", "wait", "save-completion", "collected"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := &deleteDiskTask{scheduler: scheduler, state: &protos.DeleteDiskTaskState{}}
			errFor := func(step string) error {
				if step == failure {
					return assert.AnError
				}
				return nil
			}
			scheduler.On("ScheduleNonCancellableTask", mock.Anything, registerLegacyDiskCleanupTaskType, "", "", mock.Anything).Return("migration", errFor("schedule")).Once()
			if failure != "schedule" {
				execCtx.On("SaveState", ctx).Return(errFor("save-id")).Once()
				if failure != "save-id" {
					waitErr := errFor("wait")
					if failure == "collected" {
						waitErr = errors.NewEmptyNotFoundError()
					}
					scheduler.On("WaitTask", ctx, execCtx, "migration").Return(nil, waitErr).Once()
					if failure != "wait" {
						execCtx.On("SaveState", ctx).Return(errFor("save-completion")).Once()
					}
				}
			}
			err := task.registerLegacyCleanup(ctx, execCtx)
			if failure == "collected" {
				require.NoError(t, err)
				require.True(t, task.state.LegacyCleanupRegistered)
			} else {
				require.ErrorIs(t, err, assert.AnError)
			}
			mock.AssertExpectationsForObjects(t, scheduler, execCtx)
		})
	}
}

func TestRegistrationDoesNotRetainParentThatEndedAfterPageRead(t *testing.T) {
	for _, collected := range []bool{false, true} {
		for _, commitFails := range []bool{false, true} {
			ctx := context.Background()
			storage := resource_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			task := &registerLegacyDiskCleanupTask{storage: storage, scheduler: scheduler, state: &protos.RegisterLegacyDiskCleanupTaskState{}}
			parent := legacyDeleteForRegistration(t, "parent", "disk")
			key := legacyDiskCleanupPrefix + parent.ID
			scheduler.On("ListUnfinishedTasks", ctx, "disks.DeleteDisk", "", legacyDiskCleanupPageSize).Return([]tasks_storage.TaskState{parent}, nil).Once()
			storage.On("PrepareLegacyDiskCleanup", ctx, "disk", key).Return(true, nil).Once()
			var endedErr error
			if collected {
				endedErr = errors.NewEmptyNotFoundError()
			}
			scheduler.On("WaitTaskEndedWithTimeout", ctx, parent.ID, time.Duration(0)).Return(endedErr).Once()
			tx := &persistence.Transaction{}
			storage.On("CompleteDiskCleanup", ctx, tx, "disk", key).Return(nil).Once()
			var commitErr error
			if commitFails {
				commitErr = assert.AnError
			}
			execCtx.On("SaveStateWithPreparation", ctx, mock.Anything).Run(func(args mock.Arguments) {
				prepare := args.Get(1).(func(context.Context, *persistence.Transaction) error)
				require.NoError(t, prepare(ctx, tx))
			}).Return(commitErr).Once()
			if !commitFails {
				execCtx.On("SaveState", ctx).Return(nil).Once()
			}
			err := task.Run(ctx, execCtx)
			if commitFails {
				require.ErrorIs(t, err, assert.AnError)
				require.Empty(t, task.state.AfterTaskId)
			} else {
				require.ErrorIs(t, err, errors.NewInterruptExecutionError())
			}
			// Reusing an already-finished observer must not be needed to clear the pin.
			scheduler.AssertNotCalled(t, "ScheduleNonCancellableTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
		}
	}
}
