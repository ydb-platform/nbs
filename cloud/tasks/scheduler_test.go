package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	metrics_empty "github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/operation"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/storage/mocks"
	"github.com/ydb-platform/nbs/library/go/test/assertpb"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

func defaultConfig() *tasks_config.TasksConfig {
	pollForTaskUpdatesPeriod := "100ms"
	taskWaitingTimeout := "500ms"
	scheduleRegularTasksPeriodMin := "100ms"
	scheduleRegularTasksPeriodMax := "200ms"
	endedTaskExpirationTimeout := "1s"
	clearEndedTasksTaskScheduleInterval := "1s"

	return &tasks_config.TasksConfig{
		PollForTaskUpdatesPeriod:            &pollForTaskUpdatesPeriod,
		TaskWaitingTimeout:                  &taskWaitingTimeout,
		ScheduleRegularTasksPeriodMin:       &scheduleRegularTasksPeriodMin,
		ScheduleRegularTasksPeriodMax:       &scheduleRegularTasksPeriodMax,
		EndedTaskExpirationTimeout:          &endedTaskExpirationTimeout,
		ClearEndedTasksTaskScheduleInterval: &clearEndedTasksTaskScheduleInterval,
	}
}

////////////////////////////////////////////////////////////////////////////////

// It's impossible to use ExecutionContextMock from mocks/, because of circular
// package dependency.
type executionContextMock struct {
	mock.Mock
}

func (c *executionContextMock) SaveState(ctx context.Context) error {
	args := c.Called(ctx)
	return args.Error(0)
}

func (c *executionContextMock) SaveStateWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	args := c.Called(ctx, preparation)
	return args.Error(0)
}

func (c *executionContextMock) GetTaskType() string {
	args := c.Called()
	return args.String(0)
}

func (c *executionContextMock) GetTaskID() string {
	args := c.Called()
	return args.String(0)
}

func (c *executionContextMock) AddTaskDependency(
	ctx context.Context,
	taskID string,
) error {

	args := c.Called(ctx, taskID)
	return args.Error(0)
}

func (c *executionContextMock) IsHanging() bool {
	args := c.Called()
	return args.Bool(0)
}

func (c *executionContextMock) SetEstimate(estimatedDuration time.Duration) {
	c.Called(estimatedDuration)
}

func (c *executionContextMock) HasEvent(ctx context.Context, event int64) bool {
	args := c.Called(ctx, event)
	return args.Bool(0)
}

func (c *executionContextMock) FinishWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	args := c.Called(ctx, preparation)
	return args.Error(0)
}

func newExecutionContextMock() ExecutionContext {
	execCtx := &executionContextMock{}
	execCtx.On("GetTaskID").Return("toplevel_task_id")
	return execCtx
}

////////////////////////////////////////////////////////////////////////////////

func TestSchedulerScheduleTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	request := &wrappers.UInt64Value{
		Value: 123,
	}
	marshalledRequest, err := proto.Marshal(request)
	require.NoError(t, err)

	storage.On("CreateTask", mock.Anything, mock.MatchedBy(func(state tasks_storage.TaskState) bool {
		ok := true
		ok = assert.Zero(t, state.ID) && ok
		ok = assert.Equal(t, "task", state.TaskType) && ok
		ok = assert.Equal(t, "Some task", state.Description) && ok
		ok = assert.EqualValues(t, 0, state.GenerationID) && ok
		ok = assert.Equal(t, tasks_storage.TaskStatusReadyToRun, state.Status) && ok
		ok = assert.Equal(t, grpc_codes.OK, state.ErrorCode) && ok
		ok = assert.Equal(t, "", state.ErrorMessage) && ok
		ok = assert.Equal(t, marshalledRequest, state.Request) && ok
		ok = assert.Equal(t, "", state.ZoneID) && ok
		return ok
	})).Return("taskID", nil)

	taskID, err := scheduler.ScheduleTask(
		ctx,
		"task",
		"Some task",
		request,
	)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assert.Equal(t, "taskID", taskID)
}

func TestSchedulerScheduleZonalTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	request := &wrappers.UInt64Value{
		Value: 123,
	}
	marshalledRequest, err := proto.Marshal(request)
	require.NoError(t, err)

	storage.On("CreateTask", mock.Anything, mock.MatchedBy(func(state tasks_storage.TaskState) bool {
		ok := true
		ok = assert.Zero(t, state.ID) && ok
		ok = assert.Equal(t, "task", state.TaskType) && ok
		ok = assert.Equal(t, "Some task", state.Description) && ok
		ok = assert.EqualValues(t, 0, state.GenerationID) && ok
		ok = assert.Equal(t, tasks_storage.TaskStatusReadyToRun, state.Status) && ok
		ok = assert.Equal(t, grpc_codes.OK, state.ErrorCode) && ok
		ok = assert.Equal(t, "", state.ErrorMessage) && ok
		ok = assert.Equal(t, marshalledRequest, state.Request) && ok
		ok = assert.Equal(t, "zone", state.ZoneID) && ok
		return ok
	})).Return("taskID", nil)

	taskID, err := scheduler.ScheduleZonalTask(
		ctx,
		"task",
		"Some task",
		"zone",
		request,
	)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assert.Equal(t, "taskID", taskID)
}

func TestSchedulerScheduleTaskFailOnCreateTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	storage.On("CreateTask", mock.Anything, mock.Anything).Return("", assert.AnError)

	taskID, err := scheduler.ScheduleTask(
		ctx,
		"task",
		"Some task",
		&empty.Empty{},
	)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.Error(t, err)
	assert.Zero(t, taskID)
}

func TestSchedulerCancelTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	storage.On("MarkForCancellation", mock.Anything, "taskID", mock.Anything).Return(true, nil)

	cancelling, err := scheduler.CancelTask(ctx, "taskID")
	mock.AssertExpectationsForObjects(t, storage)
	assert.NoError(t, err)
	assert.True(t, cancelling)
}

func TestSchedulerGetTaskMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	taskID := "taskID"

	storage.On("GetTask", mock.Anything, taskID).Return(tasks_storage.TaskState{
		TaskType: "task",
		Request:  []byte{1, 2, 3},
	}, nil)
	task.On("Load", []byte{1, 2, 3}, []byte(nil)).Return(nil)
	task.On("GetMetadata", mock.Anything).Return(&empty.Empty{}, nil)

	metadata, err := scheduler.GetTaskMetadata(ctx, taskID)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assertpb.Equal(t, metadata, &empty.Empty{})
}

func TestSchedulerGetOperationReadyToRun(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	taskID := "taskID"

	storage.On("GetTask", mock.Anything, taskID).Return(tasks_storage.TaskState{
		TaskType:    "task",
		Request:     []byte{1, 2, 3},
		CreatedAt:   time.Unix(1, 2),
		ModifiedAt:  time.Unix(3, 4),
		ID:          taskID,
		Description: "Some task",
		CreatedBy:   "Some UID",
		Status:      tasks_storage.TaskStatusReadyToRun,
	}, nil)
	task.On("Load", []byte{1, 2, 3}, []byte(nil)).Return(nil)
	task.On("GetMetadata", mock.Anything).Return(&empty.Empty{}, nil)

	expectedMetadata, _ := ptypes.MarshalAny(&empty.Empty{})

	op, err := scheduler.GetOperation(ctx, taskID)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assertpb.Equal(t, op, &operation.Operation{
		Id:          taskID,
		Description: "Some task",
		CreatedAt: &timestamp.Timestamp{
			Seconds: 1,
			Nanos:   2,
		},
		CreatedBy: "Some UID",
		ModifiedAt: &timestamp.Timestamp{
			Seconds: 3,
			Nanos:   4,
		},
		Done:     false,
		Metadata: expectedMetadata,
	})
}

func TestSchedulerGetOperationRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	taskID := "taskID"

	storage.On("GetTask", mock.Anything, taskID).Return(tasks_storage.TaskState{
		TaskType:    "task",
		Request:     []byte{1, 2, 3},
		CreatedAt:   time.Unix(1, 2),
		ModifiedAt:  time.Unix(3, 4),
		ID:          taskID,
		Description: "Some task",
		CreatedBy:   "Some UID",
		Status:      tasks_storage.TaskStatusRunning,
	}, nil)
	task.On("Load", []byte{1, 2, 3}, []byte(nil)).Return(nil)
	task.On("GetMetadata", mock.Anything).Return(&empty.Empty{}, nil)

	expectedMetadata, _ := ptypes.MarshalAny(&empty.Empty{})

	op, err := scheduler.GetOperation(ctx, taskID)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assertpb.Equal(t, op, &operation.Operation{
		Id:          taskID,
		Description: "Some task",
		CreatedAt: &timestamp.Timestamp{
			Seconds: 1,
			Nanos:   2,
		},
		CreatedBy: "Some UID",
		ModifiedAt: &timestamp.Timestamp{
			Seconds: 3,
			Nanos:   4,
		},
		Done:     false,
		Metadata: expectedMetadata,
	})
}

func TestSchedulerGetOperationFinished(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	taskID := "taskID"

	storage.On("GetTask", mock.Anything, taskID).Return(tasks_storage.TaskState{
		TaskType:    "task",
		Request:     []byte{1, 2, 3},
		CreatedAt:   time.Unix(1, 2),
		ModifiedAt:  time.Unix(3, 4),
		ID:          taskID,
		Description: "Some task",
		CreatedBy:   "Some UID",
		Status:      tasks_storage.TaskStatusFinished,
	}, nil)
	task.On("Load", []byte{1, 2, 3}, []byte(nil)).Return(nil)
	task.On("GetMetadata", mock.Anything).Return(&empty.Empty{}, nil)
	task.On("GetResponse").Return(&empty.Empty{})

	expectedMetadata, _ := ptypes.MarshalAny(&empty.Empty{})
	expectedResponse, _ := ptypes.MarshalAny(&empty.Empty{})

	op, err := scheduler.GetOperation(ctx, taskID)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assertpb.Equal(t, op, &operation.Operation{
		Id:          taskID,
		Description: "Some task",
		CreatedAt: &timestamp.Timestamp{
			Seconds: 1,
			Nanos:   2,
		},
		CreatedBy: "Some UID",
		ModifiedAt: &timestamp.Timestamp{
			Seconds: 3,
			Nanos:   4,
		},
		Done:     true,
		Metadata: expectedMetadata,
		Result: &operation.Operation_Response{
			Response: expectedResponse,
		},
	})
}

func TestSchedulerGetOperationReadyToCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	taskError := grpc_status.New(grpc_codes.Canceled, "An error")
	taskID := "taskID"

	storage.On("GetTask", mock.Anything, taskID).Return(tasks_storage.TaskState{
		TaskType:     "task",
		Request:      []byte{1, 2, 3},
		CreatedAt:    time.Unix(1, 2),
		ModifiedAt:   time.Unix(3, 4),
		ID:           taskID,
		Description:  "Some task",
		CreatedBy:    "Some UID",
		Status:       tasks_storage.TaskStatusReadyToCancel,
		ErrorCode:    taskError.Code(),
		ErrorMessage: taskError.Message(),
	}, nil)
	task.On("Load", []byte{1, 2, 3}, []byte(nil)).Return(nil)
	task.On("GetMetadata", mock.Anything).Return(&empty.Empty{}, nil)

	expectedMetadata, _ := ptypes.MarshalAny(&empty.Empty{})

	op, err := scheduler.GetOperation(ctx, taskID)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assertpb.Equal(t, op, &operation.Operation{
		Id:          taskID,
		Description: "Some task",
		CreatedAt: &timestamp.Timestamp{
			Seconds: 1,
			Nanos:   2,
		},
		CreatedBy: "Some UID",
		ModifiedAt: &timestamp.Timestamp{
			Seconds: 3,
			Nanos:   4,
		},
		Done:     true,
		Metadata: expectedMetadata,
		Result: &operation.Operation_Error{
			Error: taskError.Proto(),
		},
	})
}

func TestSchedulerGetOperationCancelling(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	taskError := grpc_status.New(grpc_codes.Canceled, "An error")
	taskID := "taskID"

	storage.On("GetTask", mock.Anything, taskID).Return(tasks_storage.TaskState{
		TaskType:     "task",
		Request:      []byte{1, 2, 3},
		CreatedAt:    time.Unix(1, 2),
		ModifiedAt:   time.Unix(3, 4),
		ID:           taskID,
		Description:  "Some task",
		CreatedBy:    "Some UID",
		Status:       tasks_storage.TaskStatusCancelling,
		ErrorCode:    taskError.Code(),
		ErrorMessage: taskError.Message(),
	}, nil)
	task.On("Load", []byte{1, 2, 3}, []byte(nil)).Return(nil)
	task.On("GetMetadata", mock.Anything).Return(&empty.Empty{}, nil)

	expectedMetadata, _ := ptypes.MarshalAny(&empty.Empty{})

	op, err := scheduler.GetOperation(ctx, taskID)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assertpb.Equal(t, op, &operation.Operation{
		Id:          taskID,
		Description: "Some task",
		CreatedAt: &timestamp.Timestamp{
			Seconds: 1,
			Nanos:   2,
		},
		CreatedBy: "Some UID",
		ModifiedAt: &timestamp.Timestamp{
			Seconds: 3,
			Nanos:   4,
		},
		Done:     true,
		Metadata: expectedMetadata,
		Result: &operation.Operation_Error{
			Error: taskError.Proto(),
		},
	})
}

func TestSchedulerGetOperationCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	task := NewTaskMock()
	err = registry.Register("task", func() Task { return task })
	require.NoError(t, err)

	taskError := grpc_status.New(grpc_codes.Canceled, "An error")
	taskID := "taskID"

	storage.On("GetTask", mock.Anything, taskID).Return(tasks_storage.TaskState{
		TaskType:     "task",
		Request:      []byte{1, 2, 3},
		CreatedAt:    time.Unix(1, 2),
		ModifiedAt:   time.Unix(3, 4),
		ID:           taskID,
		Description:  "Some task",
		CreatedBy:    "Some UID",
		Status:       tasks_storage.TaskStatusCancelled,
		ErrorCode:    taskError.Code(),
		ErrorMessage: taskError.Message(),
	}, nil)
	task.On("Load", []byte{1, 2, 3}, []byte(nil)).Return(nil)
	task.On("GetMetadata", mock.Anything).Return(&empty.Empty{}, nil)

	expectedMetadata, _ := ptypes.MarshalAny(&empty.Empty{})

	op, err := scheduler.GetOperation(ctx, taskID)
	mock.AssertExpectationsForObjects(t, task, storage)
	assert.NoError(t, err)
	assertpb.Equal(t, op, &operation.Operation{
		Id:          taskID,
		Description: "Some task",
		CreatedAt: &timestamp.Timestamp{
			Seconds: 1,
			Nanos:   2,
		},
		CreatedBy: "Some UID",
		ModifiedAt: &timestamp.Timestamp{
			Seconds: 3,
			Nanos:   4,
		},
		Done:     true,
		Metadata: expectedMetadata,
		Result: &operation.Operation_Error{
			Error: taskError.Proto(),
		},
	})
}

func TestSchedulerGetTaskIDByIdempotencyKey(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	storage := mocks.NewStorageMock()
	registry := NewRegistry()
	scheduler, err := NewScheduler(
		ctx,
		registry,
		storage,
		defaultConfig(),
		metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	idempotencyKey := "idempotencyKey"

	storage.On(
		"GetTaskByIdempotencyKey",
		mock.Anything, // ctx
		idempotencyKey,
		mock.Anything, // accountID
	).Return(tasks_storage.TaskState{}, nil)

	scheduler.GetTaskIDByIdempotencyKey(
		headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
	)

	mock.AssertExpectationsForObjects(t, storage)
}
