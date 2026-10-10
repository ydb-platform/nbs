package dataplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
)

////////////////////////////////////////////////////////////////////////////////

func newScheduleBackupChunksTasksTest(
	t *testing.T,
) (context.Context, *storage_mocks.StorageMock, *tasks_mocks.SchedulerMock, *tasks_mocks.ExecutionContextMock, *scheduleBackupChunksTasks) {

	ctx := logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)

	storage := storage_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("dispatcher")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	task := &scheduleBackupChunksTasks{
		scheduler: scheduler,
		storage:   storage,
		maxTasks:  50,
		batchSize: 100,
		state:     &protos.ScheduleBackupChunksTasksState{},
	}
	return ctx, storage, scheduler, execCtx, task
}

func expectWorkerScheduled(
	scheduler *tasks_mocks.SchedulerMock,
	index int,
) {

	key := fmt.Sprintf("backup_chunks_dispatcher_%v", index)
	scheduler.On(
		"ScheduleTask",
		mock.MatchedBy(func(ctx context.Context) bool {
			return headers.GetIdempotencyKey(ctx) == key
		}),
		"dataplane.BackupChunks",
		"",
		mock.Anything,
	).Return(fmt.Sprintf("worker_%v", index), nil).Once()
}

func expectWorkerDone(
	scheduler *tasks_mocks.SchedulerMock,
	id string,
	done bool,
) {

	scheduler.On("GetOperation", mock.Anything, id).Return(
		&operation.Operation{Id: id, Done: done},
		nil,
	).Once()
}

////////////////////////////////////////////////////////////////////////////////

func TestScheduleBackupChunksTasksSchedulesNothingForEmptyQueue(t *testing.T) {
	ctx, storage, scheduler, execCtx, task := newScheduleBackupChunksTasksTest(t)
	storage.On("CountQueuedBackupChunks", mock.Anything, 5000).Return(0, nil)

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	require.Empty(t, task.state.WorkerTaskIds)
}

func TestScheduleBackupChunksTasksSchedulesOneWorkerPerBatch(t *testing.T) {
	ctx, storage, scheduler, execCtx, task := newScheduleBackupChunksTasksTest(t)
	storage.On("CountQueuedBackupChunks", mock.Anything, 5000).Return(250, nil)
	for i := 0; i < 3; i++ {
		expectWorkerScheduled(scheduler, i)
	}

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	require.Equal(
		t,
		[]string{"worker_0", "worker_1", "worker_2"},
		task.state.WorkerTaskIds,
	)
	require.EqualValues(t, 3, task.state.ScheduledCount)
}

func TestScheduleBackupChunksTasksSchedulesAtMostLimit(t *testing.T) {
	ctx, storage, scheduler, execCtx, task := newScheduleBackupChunksTasksTest(t)
	task.maxTasks = 2
	storage.On("CountQueuedBackupChunks", mock.Anything, 200).Return(200, nil)
	expectWorkerScheduled(scheduler, 0)
	expectWorkerScheduled(scheduler, 1)

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	require.Len(t, task.state.WorkerTaskIds, 2)
}

func TestScheduleBackupChunksTasksReplacesEndedWorkersOnly(t *testing.T) {
	ctx, storage, scheduler, execCtx, task := newScheduleBackupChunksTasksTest(t)
	task.state.WorkerTaskIds = []string{"worker_0", "worker_1"}
	task.state.ScheduledCount = 2
	storage.On("CountQueuedBackupChunks", mock.Anything, 5000).Return(250, nil)
	expectWorkerDone(scheduler, "worker_0", true)
	expectWorkerDone(scheduler, "worker_1", false)
	expectWorkerScheduled(scheduler, 2)
	expectWorkerScheduled(scheduler, 3)

	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	require.Equal(
		t,
		[]string{"worker_1", "worker_2", "worker_3"},
		task.state.WorkerTaskIds,
	)
	require.EqualValues(t, 4, task.state.ScheduledCount)
}

func TestScheduleBackupChunksTasksKeepsEnoughWorkers(t *testing.T) {
	ctx, storage, scheduler, execCtx, task := newScheduleBackupChunksTasksTest(t)
	task.state.WorkerTaskIds = []string{"worker_0", "worker_1", "worker_2"}
	task.state.ScheduledCount = 3
	storage.On("CountQueuedBackupChunks", mock.Anything, 5000).Return(100, nil)
	for _, id := range task.state.WorkerTaskIds {
		expectWorkerDone(scheduler, id, false)
	}

	// Three live workers for one batch: nothing is scheduled, nothing is
	// cancelled, they end on their own.
	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	require.Len(t, task.state.WorkerTaskIds, 3)
}

func TestScheduleBackupChunksTasksDropsClearedWorker(t *testing.T) {
	ctx, storage, scheduler, execCtx, task := newScheduleBackupChunksTasksTest(t)
	task.state.WorkerTaskIds = []string{"worker_0"}
	task.state.ScheduledCount = 1
	storage.On("CountQueuedBackupChunks", mock.Anything, 5000).Return(100, nil)
	scheduler.On("GetOperation", mock.Anything, "worker_0").Return(
		nil,
		errors.NewNonRetriableError(
			errors.NewNotFoundErrorWithTaskID("worker_0"),
		),
	).Once()
	expectWorkerScheduled(scheduler, 1)

	// The task of worker_0 is gone from the task storage: it ended and was
	// cleared, so a new worker replaces it.
	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	require.Equal(t, []string{"worker_1"}, task.state.WorkerTaskIds)
}
