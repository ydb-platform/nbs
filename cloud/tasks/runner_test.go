package tasks

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/storage/mocks"
	grpc_codes "google.golang.org/grpc/codes"
)

////////////////////////////////////////////////////////////////////////////////

type mockCallback struct {
	mock.Mock
}

func (c *mockCallback) Run() {
	c.Called()
}

////////////////////////////////////////////////////////////////////////////////

type mockRunnerMetrics struct {
	mock.Mock
}

func (m *mockRunnerMetrics) OnExecutionStarted(execCtx ExecutionContext) {
	m.Called(execCtx)
}

func (m *mockRunnerMetrics) OnExecutionStopped() {
	m.Called()
}

func (m *mockRunnerMetrics) OnExecutionError(err error) {
	m.Called(err)
}

func (m *mockRunnerMetrics) OnError(err error) {
	m.Called(err)
}

////////////////////////////////////////////////////////////////////////////////

type mockRunner struct {
	mock.Mock
}

func (r *mockRunner) receiveTask(ctx context.Context) (taskHandle, error) {
	args := r.Called()
	return args.Get(0).(taskHandle), args.Error(1)
}

func (r *mockRunner) lockTask(
	ctx context.Context,
	taskInfo storage.TaskInfo,
) (storage.TaskState, error) {

	args := r.Called(ctx, taskInfo)
	return args.Get(0).(storage.TaskState), args.Error(1)
}

func (r *mockRunner) executeTask(
	ctx context.Context,
	execCtx *executionContext,
	task Task,
) {

	r.Called(ctx, execCtx, task)
}

func (r *mockRunner) lockAndExecuteTask(
	ctx context.Context,
	taskInfo storage.TaskInfo,
) error {

	args := r.Called(ctx, taskInfo)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func matchesState(
	t *testing.T,
	expected storage.TaskState,
) func(storage.TaskState) bool {

	modifiedAtLowBound := time.Now()
	return func(actual storage.TaskState) bool {
		ok := true

		modifiedAtHighBound := time.Now()
		duration := modifiedAtHighBound.Sub(modifiedAtLowBound) / 2
		pivot := modifiedAtLowBound.Add(duration)
		ok = assert.WithinDuration(t, pivot, time.Time(actual.ModifiedAt), duration) && ok

		var zeroTime time.Time
		actual.ModifiedAt = zeroTime

		ok = assert.Contains(t, actual.ErrorMessage, expected.ErrorMessage) && ok
		expected.ErrorMessage = ""
		actual.ErrorMessage = ""

		ok = assert.Equal(t, expected, actual) && ok
		return ok
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestExecutionContextSaveState(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:      "taskId",
			Request: []byte{1, 2, 3},
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID:      "taskId",
		Request: []byte{1, 2, 3},
		State:   []byte{2, 3, 4},
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	err := execCtx.SaveState(ctx)
	mock.AssertExpectationsForObjects(t, task, taskStorage)
	require.NoError(t, err)
}

func TestExecutionContextSaveStateFailOnError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:      "taskId",
			Request: []byte{1, 2, 3},
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID:      "taskId",
		Request: []byte{1, 2, 3},
		State:   []byte{2, 3, 4},
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, assert.AnError)

	err := execCtx.SaveState(ctx)
	mock.AssertExpectationsForObjects(t, task, taskStorage)
	require.Equal(t, assert.AnError, err)
}

func TestExecutionContextGetTaskType(t *testing.T) {
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:       "taskId",
			TaskType: "taskType",
		},
		time.Hour,
		2,
	)

	require.Equal(t, "taskType", execCtx.GetTaskType())
}

func TestExecutionContextGetTaskID(t *testing.T) {
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID: "taskId",
		},
		time.Hour,
		2,
	)

	require.Equal(t, "taskId", execCtx.GetTaskID())
}

func TestExecutionContextAddTaskDependency(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:           "taskId1",
			Dependencies: storage.NewStringSet(),
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID:           "taskId1",
		Dependencies: storage.NewStringSet("taskId2"),
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	err := execCtx.AddTaskDependency(ctx, "taskId2")
	mock.AssertExpectationsForObjects(t, task, taskStorage)
	require.NoError(t, err)
}

func TestExecutionContextHasEvent(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Events: []int64{1},
		},
		time.Hour,
		2,
	)

	require.Equal(t, true, execCtx.HasEvent(ctx, 1))
	require.Equal(t, false, execCtx.HasEvent(ctx, 0))
}

func TestExecutionContextHasEventWithEmptyEvents(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID: "taskId",
		},
		time.Hour,
		2,
	)

	require.Equal(t, false, execCtx.HasEvent(ctx, 1))
}

func TestExecutionContextAddAnotherTaskDependency(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage, storage.TaskState{
			ID:           "taskId1",
			Dependencies: storage.NewStringSet("taskId2"),
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID:           "taskId1",
		Dependencies: storage.NewStringSet("taskId2", "taskId3"),
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	err := execCtx.AddTaskDependency(ctx, "taskId3")
	mock.AssertExpectationsForObjects(t, task, taskStorage)
	require.NoError(t, err)
}

func TestExecutionContextShouldNotBeHangingByDefault(t *testing.T) {
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:        "taskId",
			CreatedAt: time.Now(),
		},
		time.Hour, // hangingTaskTimeout
		2,
	)

	require.Equal(t, false, execCtx.IsHanging())
}

////////////////////////////////////////////////////////////////////////////////

func TestRunnerForRun(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:      "taskId",
			Status:  storage.TaskStatusReadyToRun,
			Request: []byte{1, 2, 3},
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID:      "taskId",
		Status:  storage.TaskStatusFinished,
		Request: []byte{1, 2, 3},
		State:   []byte{2, 3, 4},
	}
	task.On("Save").Return(state.State, nil)
	task.On("Run", mock.Anything, execCtx).Return(nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToRun,
		},
		time.Hour,
		2,
	)

	task.On("Run", mock.Anything, execCtx).Run(func(args mock.Arguments) {
		cancel()
	}).Return(assert.AnError)

	runnerMetrics := &mockRunnerMetrics{}

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunGotAbortedError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:      "taskId",
			Status:  storage.TaskStatusRunning,
			Request: []byte{1, 2, 3},
			State:   []byte{2, 3, 4},
		},
		time.Hour,
		2,
	)

	err := errors.NewAbortedError(assert.AnError)

	task.On("Run", mock.Anything, execCtx).Run(func(args mock.Arguments) {}).Return(err)

	state := storage.TaskState{
		ID:      "taskId",
		Status:  storage.TaskStatusRunning,
		Request: []byte{1, 2, 3},
	}
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForRun{
		metrics: runnerMetrics,
	}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunGotPanic(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusRunning,
		},
		time.Hour,
		2,
	)

	task.On("Run", mock.Anything, execCtx).Run(func(args mock.Arguments) {
		panic("test panic")
	}).Return(nil)

	state := storage.TaskState{
		ID:         "taskId",
		Status:     storage.TaskStatusRunning,
		PanicCount: 1,
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", mock.AnythingOfType("*errors.PanicError")).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics, maxPanicCount: 10}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunPanicCountExceeded(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:         "taskId",
			Status:     storage.TaskStatusRunning,
			PanicCount: 1,
		},
		time.Hour,
		2,
	)

	task.On("Run", mock.Anything, execCtx).Run(func(args mock.Arguments) {
		panic("test panic")
	}).Return(nil)

	state := storage.TaskState{
		ID:           "taskId",
		Status:       storage.TaskStatusReadyToCancel,
		ErrorCode:    grpc_codes.Unknown,
		ErrorMessage: "panic: test panic",
		PanicCount:   1,
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", mock.AnythingOfType("*errors.PanicError")).Return().Once()
	runnerMetrics.On("OnExecutionError", mock.AnythingOfType("*errors.NonRetriableError")).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics, maxPanicCount: 1}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunGotRetriableError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:                  "taskId",
			Status:              storage.TaskStatusRunning,
			RetriableErrorCount: 0,
		},
		time.Hour,
		2,
	)

	err := errors.NewRetriableError(assert.AnError)

	task.On("Run", mock.Anything, execCtx).Run(func(args mock.Arguments) {}).Return(err)

	state := storage.TaskState{
		ID:                  "taskId",
		Status:              storage.TaskStatusRunning,
		RetriableErrorCount: 1,
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForRun{
		metrics:                       runnerMetrics,
		maxRetriableErrorCountDefault: 1,
	}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunRetriableErrorCountExceeded(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:                  "taskId",
			Status:              storage.TaskStatusRunning,
			RetriableErrorCount: 1,
		},
		time.Hour,
		2,
	)

	err := errors.NewRetriableError(assert.AnError)

	task.On("Run", mock.Anything, execCtx).Run(func(args mock.Arguments) {}).Return(err)

	state := storage.TaskState{
		ID:                  "taskId",
		Status:              storage.TaskStatusReadyToCancel,
		ErrorCode:           grpc_codes.Unknown,
		ErrorMessage:        err.Error(),
		RetriableErrorCount: 1,
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()
	runnerMetrics.On("OnExecutionError", mock.AnythingOfType("*errors.NonRetriableError")).Return().Once()

	runner := runnerForRun{
		metrics:                       runnerMetrics,
		maxRetriableErrorCountDefault: 1,
	}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunIgnoreRetryLimit(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:                  "taskId",
			Status:              storage.TaskStatusRunning,
			RetriableErrorCount: 1,
		},
		time.Hour,
		2,
	)

	err := errors.NewRetriableErrorWithIgnoreRetryLimit(assert.AnError)
	task.On("Run", mock.Anything, execCtx).Run(func(args mock.Arguments) {
	}).Return(err)

	state := storage.TaskState{
		ID:                  "taskId",
		Status:              storage.TaskStatusRunning,
		RetriableErrorCount: 2,
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForRun{
		metrics:                       runnerMetrics,
		maxRetriableErrorCountDefault: 1,
	}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunGotNonRetriableError1(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToRun,
		},
		time.Hour,
		2,
	)

	// Non retriable error beats retriable error.
	failure := errors.NewNonRetriableError(errors.NewRetriableError(assert.AnError))
	task.On("Run", mock.Anything, execCtx).Return(failure)

	state := storage.TaskState{
		ID:           "taskId",
		Status:       storage.TaskStatusReadyToCancel,
		ErrorCode:    grpc_codes.Unknown,
		ErrorMessage: failure.Error(),
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", failure).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunGotNonRetriableError2(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToRun,
		},
		time.Hour,
		2,
	)

	// Non retriable error beats retriable error.
	failure := errors.NewRetriableError(errors.NewNonRetriableError(assert.AnError))
	task.On("Run", mock.Anything, execCtx).Return(failure)

	state := storage.TaskState{
		ID:           "taskId",
		Status:       storage.TaskStatusReadyToCancel,
		ErrorCode:    grpc_codes.Unknown,
		ErrorMessage: failure.Error(),
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", failure).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunGotNonCancellableError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToRun,
		},
		time.Hour,
		2,
	)

	failure := errors.NewRetriableError(errors.NewNonCancellableError(assert.AnError))
	task.On("Run", mock.Anything, execCtx).Return(failure)

	state := storage.TaskState{
		ID:           "taskId",
		Status:       storage.TaskStatusCancelled,
		ErrorCode:    grpc_codes.Unknown,
		ErrorMessage: failure.Error(),
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", failure).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunWrongGeneration(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToRun,
		},
		time.Hour,
		2,
	)

	err := errors.NewWrongGenerationError()

	task.On("Run", mock.Anything, execCtx).Return(err)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunWrongGenerationWrappedIntoRetriableError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:                  "taskId",
			Status:              storage.TaskStatusReadyToRun,
			RetriableErrorCount: 0,
		},
		time.Hour,
		2,
	)

	err := errors.NewRetriableError(errors.NewWrongGenerationError())

	task.On("Run", mock.Anything, execCtx).Return(err)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForRun{
		metrics:                       runnerMetrics,
		maxRetriableErrorCountDefault: 0,
	}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunInterruptExecution(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToRun,
		},
		time.Hour,
		2,
	)

	err := errors.NewInterruptExecutionError()

	task.On("Run", mock.Anything, execCtx).Return(err)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunInterruptExecutionWrappedIntoRetriableError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:                  "taskId",
			Status:              storage.TaskStatusReadyToRun,
			RetriableErrorCount: 0,
		},
		time.Hour,
		2,
	)

	err := errors.NewRetriableError(errors.NewInterruptExecutionError())

	task.On("Run", mock.Anything, execCtx).Return(err)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForRun{
		metrics:                       runnerMetrics,
		maxRetriableErrorCountDefault: 0,
	}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForRunFailWithError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToRun,
		},
		time.Hour,
		2,
	)

	task.On("Run", mock.Anything, execCtx).Return(assert.AnError)
	state := storage.TaskState{
		ID:           "taskId",
		Status:       storage.TaskStatusReadyToCancel,
		ErrorCode:    grpc_codes.Unknown,
		ErrorMessage: assert.AnError.Error(),
	}
	task.On("Save").Return(state.State, nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", assert.AnError).Return().Once()

	runner := runnerForRun{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForCancel(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:      "taskId",
			Status:  storage.TaskStatusReadyToCancel,
			Request: []byte{1, 2, 3},
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID:      "taskId",
		Status:  storage.TaskStatusCancelled,
		Request: []byte{1, 2, 3},
		State:   []byte{2, 3, 4},
	}
	task.On("Save").Return(state.State, nil)
	task.On("Cancel", mock.Anything, execCtx).Return(nil)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}

	runner := runnerForCancel{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForCancelCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToCancel,
		},
		time.Hour,
		2,
	)

	task.On("Cancel", mock.Anything, execCtx).Run(func(args mock.Arguments) {
		cancel()
	}).Return(assert.AnError)

	runnerMetrics := &mockRunnerMetrics{}

	runner := runnerForCancel{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForCancelWrongGeneration(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToCancel,
		},
		time.Hour,
		2,
	)

	err := errors.NewWrongGenerationError()

	task.On("Cancel", mock.Anything, execCtx).Return(err)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForCancel{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForCancelFailWithError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:     "taskId",
			Status: storage.TaskStatusReadyToCancel,
		},
		time.Hour,
		2,
	)

	task.On("Cancel", mock.Anything, execCtx).Return(assert.AnError)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", assert.AnError).Return().Once()

	runner := runnerForCancel{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForCancelGotNonRetriableError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:      "taskId",
			Status:  storage.TaskStatusReadyToCancel,
			Request: []byte{1, 2, 3},
		},
		time.Hour,
		2,
	)

	err := errors.NewNonRetriableError(assert.AnError)

	state := storage.TaskState{
		ID:      "taskId",
		Status:  storage.TaskStatusCancelled,
		Request: []byte{1, 2, 3},
		State:   []byte{2, 3, 4},
	}
	task.On("Save").Return(state.State, nil)
	task.On("Cancel", mock.Anything, execCtx).Return(err)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForCancel{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestRunnerForCancelGotNonCancellableError(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID:      "taskId",
			Status:  storage.TaskStatusReadyToCancel,
			Request: []byte{1, 2, 3},
		},
		time.Hour,
		2,
	)

	err := errors.NewNonCancellableError(assert.AnError)

	state := storage.TaskState{
		ID:      "taskId",
		Status:  storage.TaskStatusCancelled,
		Request: []byte{1, 2, 3},
		State:   []byte{2, 3, 4},
	}
	task.On("Save").Return(state.State, nil)
	task.On("Cancel", mock.Anything, execCtx).Return(err)
	taskStorage.On("UpdateTask", ctx, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	runnerMetrics := &mockRunnerMetrics{}
	runnerMetrics.On("OnExecutionError", err).Return().Once()

	runner := runnerForCancel{metrics: runnerMetrics}
	runner.executeTask(ctx, execCtx, task)
	mock.AssertExpectationsForObjects(t, task, taskStorage, runnerMetrics)
}

func TestTaskPingerOnce(t *testing.T) {
	pingPeriod := 100 * time.Millisecond
	pingTimeout := 100 * time.Second
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()
	callback := &mockCallback{}

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID: "taskId",
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID: "taskId",
	}
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Return(state, nil)

	go func() {
		// Cancel runner loop on first iteration.
		// TODO: This is bad.
		<-time.After(pingPeriod / 2)
		cancel()
	}()

	taskPinger(ctx, execCtx, pingPeriod, pingTimeout, callback.Run)
	mock.AssertExpectationsForObjects(t, task, taskStorage, callback)
}

func TestTaskPingerImmediateFailure(t *testing.T) {
	pingPeriod := 100 * time.Millisecond
	pingTimeout := 100 * time.Second
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()
	callback := &mockCallback{}

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID: "taskId",
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID: "taskId",
	}
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Return(state, assert.AnError)
	callback.On("Run")

	go func() {
		// Cancel runner loop on first iteration.
		// TODO: This is bad.
		<-time.After(pingPeriod / 2)
		cancel()
	}()

	taskPinger(ctx, execCtx, pingPeriod, pingTimeout, callback.Run)
	mock.AssertExpectationsForObjects(t, task, taskStorage, callback)
}

func TestTaskPingerTwice(t *testing.T) {
	pingPeriod := 100 * time.Millisecond
	pingTimeout := 100 * time.Second
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()
	callback := &mockCallback{}

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID: "taskId",
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID: "taskId",
	}
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Return(state, nil).Twice()

	go func() {
		// Cancel runner loop on second iteration.
		// TODO: This is bad.
		<-time.After(pingPeriod + pingPeriod/2)
		cancel()
	}()

	taskPinger(ctx, execCtx, pingPeriod, pingTimeout, callback.Run)
	mock.AssertExpectationsForObjects(t, task, taskStorage, callback)
}

func TestTaskPingerFailureOnSecondIteration(t *testing.T) {
	pingPeriod := 100 * time.Millisecond
	pingTimeout := 100 * time.Second
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()
	callback := &mockCallback{}

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID: "taskId",
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID: "taskId",
	}
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Return(state, nil).Once()
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Return(state, assert.AnError).Once()
	callback.On("Run")

	go func() {
		// Cancel runner loop on second iteration.
		// TODO: This is bad.
		<-time.After(pingPeriod + pingPeriod/2)
		cancel()
	}()

	taskPinger(ctx, execCtx, pingPeriod, pingTimeout, callback.Run)
	mock.AssertExpectationsForObjects(t, task, taskStorage, callback)
}

func TestTaskPingerCancelledContextInUpdateTask(t *testing.T) {
	pingPeriod := 100 * time.Millisecond
	pingTimeout := 100 * time.Second
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	task := NewTaskMock()
	callback := &mockCallback{}

	execCtx := newExecutionContext(
		task,
		taskStorage,
		storage.TaskState{
			ID: "taskId",
		},
		time.Hour,
		2,
	)

	state := storage.TaskState{
		ID: "taskId",
	}
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Run(func(args mock.Arguments) {
		cancel()
	}).Return(state, context.Canceled).Once()

	taskPinger(ctx, execCtx, pingPeriod, pingTimeout, callback.Run)
	mock.AssertExpectationsForObjects(t, task, taskStorage, callback)
}

func TestTryExecutingTask(t *testing.T) {
	pingPeriod := 100 * time.Millisecond
	pingTimeout := 100 * time.Second
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	registry := NewRegistry()
	runner := &mockRunner{}
	runnerMetrics := &mockRunnerMetrics{}

	task := NewTaskMock()
	err := registry.RegisterForExecution("task", func() Task { return task })
	require.NoError(t, err)

	taskInfo := storage.TaskInfo{
		ID:           "taskId",
		GenerationID: 2,
	}

	state := storage.TaskState{
		ID:           "taskId",
		TaskType:     "task",
		Request:      []byte{1, 2, 3},
		State:        []byte{2, 3, 4},
		GenerationID: 3,
	}
	runner.On("lockTask", ctx, taskInfo).Return(state, nil)
	task.On("Load", state.Request, state.State).Return(nil)
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Return(state, nil).Maybe()
	runnerMetrics.On("OnExecutionStarted", mock.Anything)
	runner.On("executeTask", mock.Anything, mock.Anything, task)
	runnerMetrics.On("OnExecutionStopped")

	err = lockAndExecuteTask(
		ctx,
		taskStorage,
		registry,
		runnerMetrics,
		pingPeriod,
		pingTimeout,
		runner,
		taskInfo,
		time.Hour,
		2,
		100,
	)
	mock.AssertExpectationsForObjects(t, taskStorage, runner, runnerMetrics, task)
	require.NoError(t, err)
}

func TestTryExecutingTaskFailToPing(t *testing.T) {
	pingPeriod := 100 * time.Millisecond
	pingTimeout := 100 * time.Second
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	registry := NewRegistry()
	runner := &mockRunner{}
	runnerMetrics := &mockRunnerMetrics{}

	task := NewTaskMock()
	err := registry.RegisterForExecution("task", func() Task { return task })
	require.NoError(t, err)

	taskInfo := storage.TaskInfo{
		ID:           "taskId",
		GenerationID: 2,
	}

	state := storage.TaskState{
		ID:           "taskId",
		TaskType:     "task",
		Request:      []byte{1, 2, 3},
		State:        []byte{2, 3, 4},
		GenerationID: 3,
	}

	updateTaskErr := errors.NewWrongGenerationError()

	runner.On("lockTask", ctx, taskInfo).Return(state, nil)
	task.On("Load", state.Request, state.State).Return(nil)
	taskStorage.On("UpdateTask", mock.Anything, mock.MatchedBy(matchesState(t, state))).Return(
		state,
		updateTaskErr,
	).Once()

	runnerMetrics.On("OnExecutionStarted", mock.Anything)
	runner.On("executeTask", mock.Anything, mock.Anything, task).Run(func(args mock.Arguments) {
		// Wait for pingPeriod, so that the first ping has had a chance to run.
		// TODO: This is bad.
		<-time.After(pingPeriod)
		require.Error(t, args.Get(0).(context.Context).Err())
	})
	runnerMetrics.On("OnExecutionStopped")

	err = lockAndExecuteTask(
		ctx,
		taskStorage,
		registry,
		runnerMetrics,
		pingPeriod,
		pingTimeout,
		runner,
		taskInfo,
		time.Hour,
		2,
		100,
	)
	mock.AssertExpectationsForObjects(t, taskStorage, runner, runnerMetrics, task)
	require.NoError(t, err)
}

func TestRunnerLoopReceiveTaskFailure(t *testing.T) {
	ctx := newContext()
	taskStorage := mocks.NewStorageMock()
	registry := NewRegistry()
	runner := &mockRunner{}
	handle := taskHandle{
		task: storage.TaskInfo{
			ID: "taskId",
		},
		onClose: func() {},
	}

	runner.On("receiveTask", mock.Anything).Return(handle, assert.AnError)
	runnerLoop(ctx, registry, runner)
	mock.AssertExpectationsForObjects(t, taskStorage, runner)
}

func TestRunnerLoopSucceeds(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	taskStorage := mocks.NewStorageMock()
	registry := NewRegistry()
	runner := &mockRunner{}
	handle := taskHandle{
		task: storage.TaskInfo{
			ID: "taskId",
		},
		onClose: func() {},
	}

	runner.On("receiveTask", mock.Anything).Return(handle, nil)
	runner.On("lockAndExecuteTask", ctx, handle.task).Return(nil)

	go func() {
		// Cancel runner loop on some iteration.
		// TODO: This is bad.
		<-time.After(time.Second)
		cancel()
	}()
	runnerLoop(ctx, registry, runner)
	mock.AssertExpectationsForObjects(t, taskStorage, runner)
}

func testListerLoop(
	t *testing.T,
	listenersPerChannel int,
	channelCount int,
	taskCount int,
) {

	ctx, cancel := context.WithCancel(newContext())
	tasks := make([]storage.TaskInfo, 0)
	for i := 0; i < taskCount; i++ {
		tasks = append(tasks, storage.TaskInfo{
			ID: fmt.Sprintf("TaskID%v", i),
		})
	}
	lister := newLister(
		ctx,
		func(ctx context.Context, limit uint64) ([]storage.TaskInfo, error) {
			return tasks, nil
		},
		uint64(channelCount),   // channelsCount
		100,                    // tasksToListLimit
		50*time.Millisecond,    // pollForTasksPeriodMin
		100*time.Millisecond,   // pollForTasksPeriodMax
		make(map[string]int64), // inflightTaskLimits
	)

	receivedTasks := make([]storage.TaskInfo, 0)
	var receivedTasksMutex sync.Mutex

	var wg sync.WaitGroup
	listenerCount := channelCount * listenersPerChannel
	wg.Add(listenerCount)

	listenerCtx := context.Background()

	for i := 0; i < channelCount; i++ {
		channel := lister.channels[i]

		for j := 0; j < listenersPerChannel; j++ {
			go func() {
				defer wg.Done()

				handle, _ := channel.receive(listenerCtx)
				defer handle.close()

				receivedTasksMutex.Lock()
				defer receivedTasksMutex.Unlock()
				receivedTasks = append(receivedTasks, handle.task)
			}()
		}
	}
	wg.Wait()

	require.Equal(t, listenerCount, len(receivedTasks))
	for _, task := range receivedTasks {
		require.Contains(t, tasks, task)
	}

	cancel()
	// Ensure that channels are closed.
	for _, channel := range lister.channels {
		_, err := channel.receive(listenerCtx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "closed")
	}
}

func TestListerLoop(t *testing.T) {
	testListerLoop(t, 1, 1, 2)
	testListerLoop(t, 1, 2, 1)
	testListerLoop(t, 1, 10, 3)
	testListerLoop(t, 1, 10, 21)
	testListerLoop(t, 3, 1, 2)
	testListerLoop(t, 3, 2, 1)
	testListerLoop(t, 3, 10, 3)
	testListerLoop(t, 3, 10, 21)
}

func TestListerLoopCancellingWhileReceiving(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	taskCount := 10
	channelCount := taskCount

	tasks := make([]storage.TaskInfo, 0)
	for i := 0; i < taskCount; i++ {
		tasks = append(tasks, storage.TaskInfo{
			ID: fmt.Sprintf("TaskID%v", i),
		})
	}
	lister := newLister(
		ctx,
		func(ctx context.Context, limit uint64) ([]storage.TaskInfo, error) {
			return tasks, nil
		},
		uint64(channelCount),   // channelsCount
		100,                    // tasksToListLimit
		50*time.Millisecond,    // pollForTasksPeriodMin
		100*time.Millisecond,   // pollForTasksPeriodMax
		make(map[string]int64), // inflightTaskLimits
	)

	receivedTasks := make([]storage.TaskInfo, 0)
	var receivedTasksMutex sync.Mutex

	var wg sync.WaitGroup
	wg.Add(channelCount)

	for i := 0; i < channelCount; i++ {
		channel := lister.channels[i]
		go func() {
			defer wg.Done()

			handle, err := channel.receive(ctx)
			if err == nil {
				defer handle.close()
				receivedTasksMutex.Lock()
				defer receivedTasksMutex.Unlock()
				receivedTasks = append(receivedTasks, handle.task)
			}

			if rand.Intn(2) == 0 {
				cancel()
			}
		}()
	}
	wg.Wait()

	for _, task := range receivedTasks {
		require.Contains(t, tasks, task)
	}
}

func TestHeartbeats(t *testing.T) {
	const host = "host-1"
	ctx, cancel := context.WithCancel(newContext())

	taskStorage := mocks.NewStorageMock()

	idx := uint32(0)
	wg := sync.WaitGroup{}
	inflightTasksReporter := func() uint32 {
		idx += 1
		return idx
	}

	taskStorage.On(
		"HeartbeatNode",
		mock.Anything,
		host,
		mock.Anything,
		uint32(1),
	).Return(nil).Once()
	taskStorage.On(
		"HeartbeatNode",
		mock.Anything,
		host,
		mock.Anything,
		uint32(2),
	).Return(nil).Once()
	taskStorage.On(
		"HeartbeatNode",
		mock.Anything,
		host,
		mock.Anything,
		uint32(3),
	).Run(
		func(args mock.Arguments) {
			cancel()
			wg.Done()
		},
	).Return(nil).Once()

	wg.Add(1)
	go startHeartbeats(ctx, 10*time.Millisecond, host, taskStorage, inflightTasksReporter)
	wg.Wait()
	mock.AssertExpectationsForObjects(t, taskStorage)
}
