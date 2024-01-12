package dataplane

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	storage_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestCollectSnapshotsTaskNothingToCollect(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	limit := 10
	task := &collectSnapshotsTask{
		scheduler:                       scheduler,
		storage:                         storage,
		snapshotCollectionTimeout:       time.Second,
		snapshotCollectionInflightLimit: limit,
		state:                           &protos.CollectSnapshotsTaskState{},
	}

	execCtx.On("GetTaskID").Return("")
	storage.On("GetSnapshotsToDelete", ctx, mock.Anything, limit).Return(
		[]*storage_protos.DeletingSnapshotKey{},
		nil,
	)
	execCtx.On("SaveState", ctx, mock.Anything).Return(nil)

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}

func TestCollectSnapshotsTaskErrorInSaveState(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	limit := 10
	task := &collectSnapshotsTask{
		scheduler:                       scheduler,
		storage:                         storage,
		snapshotCollectionTimeout:       time.Second,
		snapshotCollectionInflightLimit: limit,
		state:                           &protos.CollectSnapshotsTaskState{},
	}

	execCtx.On("GetTaskID").Return("")
	storage.On("GetSnapshotsToDelete", ctx, mock.Anything, limit).Return(
		[]*storage_protos.DeletingSnapshotKey{},
		nil,
	)
	execCtx.On("SaveState", ctx, mock.Anything).Return(assert.AnError)

	err := task.Run(ctx, execCtx)
	require.Equal(t, assert.AnError, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}

func TestCollectSnapshotsTaskCollectSeveralSnapshots(t *testing.T) {
	ctx := newContext()
	scheduler := tasks_mocks.NewSchedulerMock()
	storage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	limit := 2
	task := &collectSnapshotsTask{
		scheduler:                       scheduler,
		storage:                         storage,
		snapshotCollectionTimeout:       time.Second,
		snapshotCollectionInflightLimit: limit,
		state:                           &protos.CollectSnapshotsTaskState{},
	}

	snapshot1 := &storage_protos.DeletingSnapshotKey{
		SnapshotId: "snapshot1",
	}
	snapshot2 := &storage_protos.DeletingSnapshotKey{
		SnapshotId: "snapshot2",
	}
	snapshot3 := &storage_protos.DeletingSnapshotKey{
		SnapshotId: "snapshot3",
	}
	snapshot4 := &storage_protos.DeletingSnapshotKey{
		SnapshotId: "snapshot4",
	}
	snapshots := []*storage_protos.DeletingSnapshotKey{snapshot1, snapshot2}

	execCtx.On("GetTaskID").Return("")

	storage.On("GetSnapshotsToDelete", ctx, mock.Anything, limit).Return(
		snapshots,
		nil,
	).Times(1)
	execCtx.On("SaveState", ctx, mock.Anything).Return(nil).Times(1)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshotData",
		"",
		mock.Anything,
		"",
		"",
	).Return("task1", nil).Times(1)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshotData",
		"",
		mock.Anything,
		"",
		"",
	).Return("task2", nil).Times(1)
	scheduler.On(
		"WaitAnyTasks",
		ctx,
		[]string{"task1", "task2"},
	).Return([]string{"task1"}, nil).Times(1)
	storage.On(
		"ClearDeletingSnapshots",
		ctx,
		[]*storage_protos.DeletingSnapshotKey{snapshot1},
	).Return(nil).Times(1)
	execCtx.On("SaveState", ctx, mock.Anything).Return(nil).Times(1)

	snapshots = []*storage_protos.DeletingSnapshotKey{snapshot2, snapshot3}
	storage.On("GetSnapshotsToDelete", ctx, mock.Anything, limit).Return(
		snapshots,
		nil,
	).Times(1)
	execCtx.On("SaveState", ctx, mock.Anything).Return(nil).Times(1)
	// ScheduleTask should not be called for task2 as it has been called
	// recently.
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshotData",
		"",
		mock.Anything,
		"",
		"",
	).Return("task3", nil).Times(1)
	scheduler.On(
		"WaitAnyTasks",
		ctx,
		[]string{"task2", "task3"},
	).Return([]string{"task3"}, nil).Times(1)
	storage.On(
		"ClearDeletingSnapshots",
		ctx,
		[]*storage_protos.DeletingSnapshotKey{snapshot3},
	).Return(nil).Times(1)
	execCtx.On("SaveState", ctx, mock.Anything).Return(assert.AnError).Times(1)

	err := task.Run(ctx, execCtx)
	require.Equal(t, assert.AnError, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)

	// Check that everything works after task is restarted.
	snapshots = []*storage_protos.DeletingSnapshotKey{snapshot2, snapshot4}
	storage.On("GetSnapshotsToDelete", ctx, mock.Anything, limit).Return(
		snapshots,
		nil,
	).Times(1)
	execCtx.On("SaveState", ctx, mock.Anything).Return(nil).Times(1)
	// ScheduleTask should be called for task2, because task was restarted.
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshotData",
		"",
		mock.Anything,
		"",
		"",
	).Return("task2", nil).Times(1)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteSnapshotData",
		"",
		mock.Anything,
		"",
		"",
	).Return("task4", nil).Times(1)
	scheduler.On(
		"WaitAnyTasks",
		ctx,
		[]string{"task2", "task4"},
	).Return([]string{"task4"}, nil).Times(1)
	storage.On(
		"ClearDeletingSnapshots",
		ctx,
		[]*storage_protos.DeletingSnapshotKey{snapshot4},
	).Return(nil).Times(1)
	execCtx.On("SaveState", ctx, mock.Anything).Return(assert.AnError).Times(1)

	err = task.Run(ctx, execCtx)
	require.Equal(t, assert.AnError, err)
	mock.AssertExpectationsForObjects(t, scheduler, storage, execCtx)
}
