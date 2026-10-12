package tasks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/storage/mocks"
)

func TestReconcileDelayedTasksResumesPersistedCursor(t *testing.T) {
	ctx := newContext()
	s := mocks.NewStorageMock()
	task := &reconcileDelayedTasksTask{storage: s, limit: 1, folder: "tasks"}
	require.NoError(t, task.Load(nil, nil))
	start := task.cursor
	page := storage.DelayedQueueCursor{
		StorageFolder: "tasks",
		After:         &storage.DelayedQueueKey{AvailableAt: time.Unix(100, 0).UTC(), ID: "a"},
		Upper:         &storage.DelayedQueueKey{AvailableAt: time.Unix(200, 0).UTC(), ID: "z"},
	}
	s.On("ReconcileReadyToRunDelayed", ctx, 1, start).Return(page, nil).Once()
	execCtx := &executionContextMock{}
	var persisted []byte
	execCtx.On("SaveState", ctx).Return(nil).Once().Run(func(_ mock.Arguments) {
		var err error
		persisted, err = task.Save()
		require.NoError(t, err)
	})
	require.ErrorIs(t, task.Run(ctx, execCtx), errors.NewInterruptExecutionError())

	// Simulate a new worker constructing the task from its stored state.
	restarted := &reconcileDelayedTasksTask{storage: s, limit: 1, folder: "tasks"}
	require.NoError(t, restarted.Load(nil, persisted))
	require.Equal(t, page, restarted.cursor)
	done := page
	done.Done = true
	s.On("ReconcileReadyToRunDelayed", ctx, 1, page).Return(done, nil).Once()
	lastExec := &executionContextMock{}
	lastExec.On("SaveState", ctx).Return(nil).Once()
	require.NoError(t, restarted.Run(ctx, lastExec))
	s.AssertExpectations(t)
	execCtx.AssertExpectations(t)
	lastExec.AssertExpectations(t)
}

func TestReconcileDelayedTasksDoesNotPersistFailedPage(t *testing.T) {
	ctx := newContext()
	s := mocks.NewStorageMock()
	task := &reconcileDelayedTasksTask{storage: s, limit: 1, folder: "tasks"}
	require.NoError(t, task.Load(nil, nil))
	before, err := task.Save()
	require.NoError(t, err)
	failure := errors.NewRetriableErrorf("unavailable delayed table")
	s.On("ReconcileReadyToRunDelayed", ctx, 1, task.cursor).
		Return(storage.DelayedQueueCursor{Done: true}, failure).Once()
	require.ErrorIs(t, task.Run(ctx, nil), failure)
	after, err := task.Save()
	require.NoError(t, err)
	require.Equal(t, before, after)
	s.AssertExpectations(t)
}

func TestReconcileDelayedTasksRemainsRegisteredWhenDisabled(t *testing.T) {
	ctx := newContext()
	enabled := false
	legacy := "legacy"
	s := &scheduler{registry: NewRegistry()}
	err := s.registerAndScheduleDelayedQueueReconciliation(ctx, &tasks_config.TasksConfig{
		ReconcileReadyToRunDelayedEnabled: &enabled,
		LegacyStorageFolder:               &legacy,
	})
	require.NoError(t, err)
	for _, name := range []string{"tasks.ReconcileReadyToRunDelayed", "tasks.ReconcileLegacyReadyToRunDelayed"} {
		_, err := s.registry.NewTask(name)
		require.NoError(t, err)
	}
}
