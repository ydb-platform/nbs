package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) CreateTask(
	ctx context.Context,
	state tasks_storage.TaskState,
) (string, error) {

	args := s.Called(ctx, state)
	return args.String(0), args.Error(1)
}

func (s *StorageMock) CreateRegularTasks(
	ctx context.Context,
	state tasks_storage.TaskState,
	schedule tasks_storage.TaskSchedule,
) error {

	args := s.Called(ctx, state, schedule)
	return args.Error(0)
}

func (s *StorageMock) GetTask(
	ctx context.Context,
	taskID string,
) (tasks_storage.TaskState, error) {

	args := s.Called(ctx, taskID)
	return args.Get(0).(tasks_storage.TaskState), args.Error(1)
}

func (s *StorageMock) GetTaskByIdempotencyKey(
	ctx context.Context,
	idempotencyKey string,
	accountID string,
) (tasks_storage.TaskState, error) {

	args := s.Called(ctx, idempotencyKey, accountID)
	return args.Get(0).(tasks_storage.TaskState), args.Error(1)
}

func (s *StorageMock) ListTasksReadyToRun(
	ctx context.Context,
	limit uint64,
	taskTypeWhitelist []string,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, limit, taskTypeWhitelist)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}

func (s *StorageMock) ListTasksReadyToCancel(
	ctx context.Context,
	limit uint64,
	taskTypeWhitelist []string,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, limit, taskTypeWhitelist)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}

func (s *StorageMock) ListTasksStallingWhileRunning(
	ctx context.Context,
	hostname string,
	limit uint64,
	taskTypeWhitelist []string,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, hostname, limit, taskTypeWhitelist)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}

func (s *StorageMock) ListTasksStallingWhileCancelling(
	ctx context.Context,
	hostname string,
	limit uint64,
	taskTypeWhitelist []string,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, hostname, limit, taskTypeWhitelist)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}

func (s *StorageMock) ListTasksRunning(
	ctx context.Context,
	limit uint64,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, limit)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}
func (s *StorageMock) ListTasksCancelling(
	ctx context.Context,
	limit uint64,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, limit)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}

func (s *StorageMock) ListTasksWithStatus(
	ctx context.Context,
	status string,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, status)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}

func (s *StorageMock) ListHangingTasks(
	ctx context.Context,
	limit uint64,
) ([]tasks_storage.TaskInfo, error) {

	args := s.Called(ctx, limit)
	res, _ := args.Get(0).([]tasks_storage.TaskInfo)
	return res, args.Error(1)
}

func (s *StorageMock) ListFailedTasks(
	ctx context.Context,
	since time.Time,
) ([]string, error) {

	args := s.Called(ctx, since)
	res, _ := args.Get(0).([]string)
	return res, args.Error(1)
}

func (s *StorageMock) ListSlowTasks(
	ctx context.Context,
	since time.Time,
	estimateMiss time.Duration,
) ([]string, error) {

	args := s.Called(ctx, since, estimateMiss)
	res, _ := args.Get(0).([]string)
	return res, args.Error(1)
}

func (s *StorageMock) LockTaskToRun(
	ctx context.Context,
	taskInfo tasks_storage.TaskInfo,
	at time.Time,
	hostname string,
	runner string,
) (tasks_storage.TaskState, error) {

	args := s.Called(ctx, taskInfo, at, hostname, runner)
	return args.Get(0).(tasks_storage.TaskState), args.Error(1)
}

func (s *StorageMock) LockTaskToCancel(
	ctx context.Context,
	taskInfo tasks_storage.TaskInfo,
	at time.Time,
	hostname string,
	runner string,
) (tasks_storage.TaskState, error) {

	args := s.Called(ctx, taskInfo, at, hostname, runner)
	return args.Get(0).(tasks_storage.TaskState), args.Error(1)
}

func (s *StorageMock) MarkForCancellation(
	ctx context.Context,
	taskID string,
	now time.Time,
) (bool, error) {

	args := s.Called(ctx, taskID, now)
	return args.Bool(0), args.Error(1)
}

func (s *StorageMock) UpdateTaskWithPreparation(
	ctx context.Context,
	state tasks_storage.TaskState,
	preparation func(context.Context, *persistence.Transaction) error,
) (tasks_storage.TaskState, error) {

	args := s.Called(ctx, state, preparation)
	return args.Get(0).(tasks_storage.TaskState), args.Error(1)
}

func (s *StorageMock) UpdateTask(
	ctx context.Context,
	state tasks_storage.TaskState,
) (tasks_storage.TaskState, error) {

	args := s.Called(ctx, state)
	return args.Get(0).(tasks_storage.TaskState), args.Error(1)
}

func (s *StorageMock) SendEvent(
	ctx context.Context,
	taskID string,
	event int64,
) error {

	args := s.Called(ctx, taskID, event)
	return args.Error(0)
}

func (s *StorageMock) ClearEndedTasks(
	ctx context.Context,
	endedBefore time.Time,
	limit int,
) error {

	args := s.Called(ctx, endedBefore, limit)
	return args.Error(0)
}

func (s *StorageMock) ForceFinishTask(ctx context.Context, taskID string) error {
	args := s.Called(ctx, taskID)
	return args.Error(0)
}

func (s *StorageMock) PauseTask(ctx context.Context, taskID string) error {
	args := s.Called(ctx, taskID)
	return args.Error(0)
}

func (s *StorageMock) ResumeTask(ctx context.Context, taskID string) error {
	args := s.Called(ctx, taskID)
	return args.Error(0)
}

func (s *StorageMock) HeartbeatNode(
	ctx context.Context,
	host string,
	ts time.Time,
	inflightTaskCount uint32,
) error {

	args := s.Called(ctx, host, ts, inflightTaskCount)
	return args.Error(0)
}

func (s *StorageMock) GetAliveNodes(
	ctx context.Context,
) ([]tasks_storage.Node, error) {

	args := s.Called(ctx)
	return args.Get(0).([]tasks_storage.Node), args.Error(1)
}

func (s *StorageMock) GetNode(
	ctx context.Context,
	host string,
) (tasks_storage.Node, error) {

	args := s.Called(ctx)
	return args.Get(0).(tasks_storage.Node), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that StorageMock implements tasks_storage.Storage.
func assertStorageMockIsStorage(arg *StorageMock) tasks_storage.Storage {
	return arg
}
