package tests

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	metrics_empty "github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

const inflightLongTaskPerNodeLimit = 10

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(ctx context.Context, t *testing.T) *persistence.YDBClient {
	endpoint := os.Getenv("YDB_ENDPOINT")
	database := os.Getenv("YDB_DATABASE")
	rootPath := "tasks"

	db, err := persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metrics_empty.NewRegistry(),
	)

	require.NoError(t, err)
	return db
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *tasks_config.TasksConfig,
	metricsRegistry metrics.Registry,
) tasks_storage.Storage {

	folder := fmt.Sprintf("tasks_ydb_test/%v", t.Name())
	config.StorageFolder = &folder

	err := tasks_storage.CreateYDBTables(
		ctx,
		config,
		db,
		false, // dropUnusedColums
	)
	require.NoError(t, err)

	storage, err := tasks_storage.NewStorage(config, metricsRegistry, db)
	require.NoError(t, err)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

var lastReqNumber int

func getRequestContext(t *testing.T, ctx context.Context) context.Context {
	lastReqNumber++

	cookie := fmt.Sprintf("%v_%v", t.Name(), lastReqNumber)
	ctx = headers.SetIncomingIdempotencyKey(ctx, cookie)
	ctx = headers.SetIncomingRequestID(ctx, cookie)
	return ctx
}

func newDefaultConfig() *tasks_config.TasksConfig {
	pollForTaskUpdatesPeriod := "100ms"
	pollForTasksPeriodMin := "100ms"
	pollForTasksPeriodMax := "200ms"
	pollForStallingTasksPeriodMin := "100ms"
	pollForStallingTasksPeriodMax := "400ms"
	taskPingPeriod := "100ms"
	taskStallingTimeout := "1s"
	taskWaitingTimeout := "500ms"
	scheduleRegularTasksPeriodMin := "100ms"
	scheduleRegularTasksPeriodMax := "400ms"
	endedTaskExpirationTimeout := "300s"
	clearEndedTasksTaskScheduleInterval := "6s"
	clearEndedTasksLimit := uint64(10)
	maxRetriableErrorCount := uint64(2)
	hangingTaskTimeout := "24h"
	inflightHangingTaskTimeout := "100s"
	stallingHangingTaskTimeout := "30m"
	maxPanicCount := uint64(0)
	inflightTaskPerNodeLimits := map[string]int64{
		"long": inflightLongTaskPerNodeLimit,
	}

	return &tasks_config.TasksConfig{
		PollForTaskUpdatesPeriod:            &pollForTaskUpdatesPeriod,
		PollForTasksPeriodMin:               &pollForTasksPeriodMin,
		PollForTasksPeriodMax:               &pollForTasksPeriodMax,
		PollForStallingTasksPeriodMin:       &pollForStallingTasksPeriodMin,
		PollForStallingTasksPeriodMax:       &pollForStallingTasksPeriodMax,
		TaskPingPeriod:                      &taskPingPeriod,
		TaskStallingTimeout:                 &taskStallingTimeout,
		TaskWaitingTimeout:                  &taskWaitingTimeout,
		ScheduleRegularTasksPeriodMin:       &scheduleRegularTasksPeriodMin,
		ScheduleRegularTasksPeriodMax:       &scheduleRegularTasksPeriodMax,
		EndedTaskExpirationTimeout:          &endedTaskExpirationTimeout,
		ClearEndedTasksTaskScheduleInterval: &clearEndedTasksTaskScheduleInterval,
		ClearEndedTasksLimit:                &clearEndedTasksLimit,
		MaxRetriableErrorCount:              &maxRetriableErrorCount,
		HangingTaskTimeout:                  &hangingTaskTimeout,
		InflightHangingTaskTimeout:          &inflightHangingTaskTimeout,
		StallingHangingTaskTimeout:          &stallingHangingTaskTimeout,
		MaxPanicCount:                       &maxPanicCount,
		InflightTaskPerNodeLimits:           inflightTaskPerNodeLimits,
	}
}

////////////////////////////////////////////////////////////////////////////////

type services struct {
	config    *tasks_config.TasksConfig
	registry  *tasks.Registry
	scheduler tasks.Scheduler
	storage   tasks_storage.Storage
}

func createServicesWithConfig(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *tasks_config.TasksConfig,
	schedulerRegistry metrics.Registry,
) services {

	registry := tasks.NewRegistry()

	storage := newStorage(
		t,
		ctx,
		db,
		config,
		metrics_empty.NewRegistry(),
	)

	scheduler, err := tasks.NewScheduler(
		ctx,
		registry,
		storage,
		config,
		schedulerRegistry,
	)
	require.NoError(t, err)

	return services{
		config:    config,
		registry:  registry,
		scheduler: scheduler,
		storage:   storage,
	}
}

func createServicesWithMetricsRegistry(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	runnersCount uint64,
	metricsRegistry metrics.Registry,
) services {

	config := proto.Clone(newDefaultConfig()).(*tasks_config.TasksConfig)
	config.RunnersCount = &runnersCount
	config.StalkingRunnersCount = &runnersCount

	return createServicesWithConfig(
		t,
		ctx,
		db,
		config,
		metricsRegistry,
	)
}

func createServices(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	runnersCount uint64,
) services {

	return createServicesWithMetricsRegistry(
		t,
		ctx,
		db,
		runnersCount,
		metrics_empty.NewRegistry(),
	)
}

func (s *services) startRunners(ctx context.Context) error {
	return s.startRunnersWithMetricsRegistry(ctx, metrics_empty.NewRegistry())
}

func (s *services) startRunnersWithMetricsRegistry(
	ctx context.Context,
	metricsRegistry metrics.Registry,
) error {

	return tasks.StartRunners(
		ctx,
		s.storage,
		s.registry,
		metricsRegistry,
		s.config,
		"localhost",
	)
}

////////////////////////////////////////////////////////////////////////////////

type doublerTask struct {
	request *wrappers.UInt64Value
	state   *wrappers.UInt64Value
}

func (t *doublerTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *doublerTask) Load(request, state []byte) error {
	t.request = &wrappers.UInt64Value{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &wrappers.UInt64Value{}
	return proto.Unmarshal(state, t.state)
}

func (t *doublerTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	t.state.Value = 2 * t.request.Value
	return nil
}

func (t *doublerTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *doublerTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *doublerTask) GetResponse() proto.Message {
	return t.state
}

func registerDoublerTask(registry *tasks.Registry) error {
	return registry.RegisterForExecution("doubler", func() tasks.Task {
		return &doublerTask{}
	})
}

func scheduleDoublerTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	request uint64,
) (string, error) {

	return scheduler.ScheduleTask(ctx, "doubler", "Doubler task", &wrappers.UInt64Value{
		Value: request,
	})
}

////////////////////////////////////////////////////////////////////////////////

type longTask struct{}

func (t *longTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *longTask) Load(request, state []byte) error {
	return nil
}

func (t *longTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return nil
	}
}

func (t *longTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *longTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *longTask) GetResponse() proto.Message {
	return &wrappers.UInt64Value{
		Value: 1,
	}
}

func registerLongTask(registry *tasks.Registry) error {
	return registry.RegisterForExecution("long", func() tasks.Task {
		return &longTask{}
	})
}

func registerLongTaskNotForExecution(registry *tasks.Registry) error {
	return registry.Register("long", func() tasks.Task {
		return &longTask{}
	})
}

func scheduleLongTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
) (string, error) {

	return scheduler.ScheduleTask(ctx, "long", "Long task", &empty.Empty{})
}

////////////////////////////////////////////////////////////////////////////////

type hangingTask struct{}

func (t *hangingTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *hangingTask) Load(request []byte, state []byte) error {
	return nil
}

func (t *hangingTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (t *hangingTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *hangingTask) GetMetadata(ctx context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *hangingTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

func registerHangingTask(registry *tasks.Registry) error {
	return registry.RegisterForExecution(
		"tasks.hanging",
		func() tasks.Task {
			return &hangingTask{}
		},
	)
}

func scheduleHangingTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
) (string, error) {

	return scheduler.ScheduleTask(
		ctx,
		"tasks.hanging",
		"Hanging task",
		&empty.Empty{},
	)
}

////////////////////////////////////////////////////////////////////////////////

type waitingTaskWithSleep struct {
	request   *wrappers.StringValue
	scheduler tasks.Scheduler
}

func (t *waitingTaskWithSleep) Save() ([]byte, error) {
	return proto.Marshal(t.request)
}

func (t *waitingTaskWithSleep) Load(request, state []byte) error {
	t.request = &wrappers.StringValue{}
	return proto.Unmarshal(request, t.request)
}

func (t *waitingTaskWithSleep) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
	}

	_, err := t.scheduler.WaitTask(ctx, execCtx, t.request.Value)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return nil
	}
}

func (t *waitingTaskWithSleep) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *waitingTaskWithSleep) GetMetadata(ctx context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *waitingTaskWithSleep) GetResponse() proto.Message {
	return &wrappers.UInt64Value{
		Value: 1,
	}
}

func registerWaitingTaskWithSleep(registry *tasks.Registry, scheduler tasks.Scheduler) error {
	return registry.RegisterForExecution(
		"waiting",
		func() tasks.Task {
			return &waitingTaskWithSleep{scheduler: scheduler}
		},
	)
}

func scheduleWaitingTaskWithSleep(
	ctx context.Context,
	scheduler tasks.Scheduler,
	depTaskId string,
) (string, error) {

	return scheduler.ScheduleTask(
		ctx,
		"waiting",
		"Waiting task",
		&wrappers.StringValue{Value: depTaskId},
	)
}

////////////////////////////////////////////////////////////////////////////////

// Fails exactly n times in a row.
type unstableTask struct {
	// Represents 'number of failures until success'.
	request *wrappers.UInt64Value
	// Represents 'current number of failures'.
	state *wrappers.UInt64Value
}

func (t *unstableTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *unstableTask) Load(request, state []byte) error {

	t.request = &wrappers.UInt64Value{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &wrappers.UInt64Value{}
	return proto.Unmarshal(state, t.state)
}

func (t *unstableTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if t.state.Value == t.request.Value {
		return nil
	}

	t.state.Value++

	err := execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	return errors.NewRetriableError(assert.AnError)
}

func (t *unstableTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *unstableTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return t.state, nil
}

func (t *unstableTask) GetResponse() proto.Message {
	return t.state
}

func registerUnstableTask(registry *tasks.Registry) error {
	return registry.RegisterForExecution("unstable", func() tasks.Task {
		return &unstableTask{}
	})
}

func scheduleUnstableTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	failuresUntilSuccess uint64,
) (string, error) {

	return scheduler.ScheduleTask(
		ctx,
		"unstable",
		"Unstable task",
		&wrappers.UInt64Value{
			Value: failuresUntilSuccess,
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

type failureTask struct {
	failure error
}

func (t *failureTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *failureTask) Load(_, _ []byte) error {
	return nil
}

func (t *failureTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return t.failure
}

func (*failureTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *failureTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *failureTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

func registerFailureTask(registry *tasks.Registry, failure error) error {
	return registry.RegisterForExecution("failure", func() tasks.Task {
		return &failureTask{failure: failure}
	})
}

func scheduleFailureTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
) (string, error) {

	return scheduler.ScheduleTask(
		ctx,
		"failure",
		"Failure task",
		&empty.Empty{},
	)
}

////////////////////////////////////////////////////////////////////////////////

type sixTimesTask struct {
	scheduler tasks.Scheduler
	request   *wrappers.UInt64Value
	state     *wrappers.UInt64Value
}

func (t *sixTimesTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *sixTimesTask) Load(request, state []byte) error {
	t.request = &wrappers.UInt64Value{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &wrappers.UInt64Value{}
	return proto.Unmarshal(state, t.state)
}

func (t *sixTimesTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	id, err := scheduleDoublerTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()),
		t.scheduler,
		t.request.Value,
	)
	if err != nil {
		return err
	}

	response, err := t.scheduler.WaitTask(ctx, execCtx, id)
	if err != nil {
		return err
	}

	res := response.(*wrappers.UInt64Value).GetValue()
	t.state.Value = res * 3
	return nil
}

func (t *sixTimesTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *sixTimesTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *sixTimesTask) GetResponse() proto.Message {
	return &wrappers.UInt64Value{
		Value: t.state.Value,
	}
}

func registerSixTimesTask(
	registry *tasks.Registry,
	scheduler tasks.Scheduler,
) error {

	return registry.RegisterForExecution("sixTimes", func() tasks.Task {
		return &sixTimesTask{
			scheduler: scheduler,
		}
	})
}

func scheduleSixTimesTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	request uint64,
) (string, error) {

	return scheduler.ScheduleTask(ctx, "sixTimes", "SixTimes task", &wrappers.UInt64Value{
		Value: request,
	})
}

////////////////////////////////////////////////////////////////////////////////

var regularTaskMutex sync.Mutex
var regularTaskCounter int

type regularTask struct {
}

func (t *regularTask) Load(_, _ []byte) error {
	return nil
}

func (t *regularTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *regularTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	regularTaskMutex.Lock()
	regularTaskCounter++
	regularTaskMutex.Unlock()
	return nil
}

func (t *regularTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return nil
}

func (t *regularTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *regularTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

var defaultTimeout = 10 * time.Minute

func waitTaskWithTimeout(
	ctx context.Context,
	scheduler tasks.Scheduler,
	id string,
	timeout time.Duration,
) (uint64, error) {

	response, err := scheduler.WaitTaskSync(ctx, id, timeout)
	if err != nil {
		return 0, err
	}

	return response.(*wrappers.UInt64Value).GetValue(), nil
}

func waitTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	id string,
) (uint64, error) {

	return waitTaskWithTimeout(ctx, scheduler, id, defaultTimeout)
}

func getTaskMetadata(
	ctx context.Context,
	scheduler tasks.Scheduler,
	id string,
) (uint64, error) {

	metadata, err := scheduler.GetTaskMetadata(ctx, id)
	if err != nil {
		return 0, err
	}

	return metadata.(*wrappers.UInt64Value).GetValue(), nil
}

////////////////////////////////////////////////////////////////////////////////

func TestTasksInitInfra(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := s.startRunners(ctx)
	require.NoError(t, err)
}

func TestTasksRunningOneTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleDoublerTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id)
	require.NoError(t, err)
	require.EqualValues(t, 2*123, response)
}

func TestTasksInflightLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	registry := mocks.NewIgnoreUnknownCallsRegistryMock()
	defer registry.AssertAllExpectations(t)

	s := createServicesWithMetricsRegistry(
		t,
		ctx,
		db,
		3*inflightLongTaskPerNodeLimit, // runnersCount
		registry,
	)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = registerLongTask(s.registry)
	require.NoError(t, err)

	var inflightLongTasksCount atomic.Int32
	registry.GetGauge(
		"inflightTasks",
		map[string]string{"type": "long"},
	).On("Add", float64(1)).Return().Run(
		func(args mock.Arguments) {
			inflightLongTasksCount.Add(1)
		},
	)
	registry.GetGauge(
		"inflightTasks",
		map[string]string{"type": "long"},
	).On("Add", float64(-1)).Return().Run(
		func(args mock.Arguments) {
			inflightLongTasksCount.Add(-1)
		},
	)

	err = s.startRunnersWithMetricsRegistry(ctx, registry)
	require.NoError(t, err)

	longTaskIDs := []string{}
	scheduledLongTaskCount := 6 * inflightLongTaskPerNodeLimit
	for i := 0; i < scheduledLongTaskCount; i++ {
		reqCtx := getRequestContext(t, ctx)
		id, err := scheduleLongTask(reqCtx, s.scheduler)
		require.NoError(t, err)

		longTaskIDs = append(longTaskIDs, id)
	}

	endedLongTaskCount := 0
	longTaskErrs := make(chan error)
	for _, id := range longTaskIDs {
		go func(id string) {
			_, err := waitTask(ctx, s.scheduler, id)
			longTaskErrs <- err
		}(id)
	}

	doublerTaskIDs := []string{}
	scheduledDoublerTaskCount := 2 * inflightLongTaskPerNodeLimit
	for i := 0; i < scheduledDoublerTaskCount; i++ {
		reqCtx := getRequestContext(t, ctx)
		id, err := scheduleDoublerTask(reqCtx, s.scheduler, 1)
		require.NoError(t, err)

		doublerTaskIDs = append(doublerTaskIDs, id)
	}

	endedDoublerTaskCount := 0
	doublerTaskErrs := make(chan error)
	for _, id := range doublerTaskIDs {
		go func(id string) {
			// Doubler tasks must finish fast enough: long tasks must not
			// exhaust the whole limit on inflight tasks due to the limit on
			// infilght long tasks.
			_, err := waitTaskWithTimeout(
				ctx,
				s.scheduler,
				id,
				5*time.Second,
			)
			doublerTaskErrs <- err
		}(id)
	}

	ticker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			// Note that inflight task is not the same as task with status 'running'.
			// The task with status 'running' might not be executing right now
			// in case of retriable or interrupt execution error.
			count := int(inflightLongTasksCount.Load())
			logging.Debug(ctx, "There are %v inflight tasks of type long", count)
			// We have separate inflight per node limit for each lister.
			// So long tasks can be taken by listerReadyToRun or
			// listerStallingWhileRunning lister.
			// Thus we need to compare runningLongTaskCount with doubled inflightLongTaskPerNodeLimit.
			require.LessOrEqual(t, count, 2*inflightLongTaskPerNodeLimit)
		case err := <-doublerTaskErrs:
			require.NoError(t, err)
			endedDoublerTaskCount++
		case err := <-longTaskErrs:
			require.NoError(t, err)
			endedLongTaskCount++

			if endedLongTaskCount == scheduledLongTaskCount {
				require.EqualValues(t, scheduledDoublerTaskCount, endedDoublerTaskCount)
				return
			}
		}
	}
}

func TestTasksSendEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleDoublerTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	updateTask := func() (tasks_storage.TaskState, error) {
		ticker := time.NewTicker(20 * time.Millisecond)

		for range ticker.C {
			taskState, err := s.storage.GetTask(ctx, id)
			require.NoError(t, err)

			taskState, err = s.storage.UpdateTask(ctx, taskState)
			// Retry UpdateTask as it might encounter 'wrong generation'
			// because task might be locked/executed in parallel.
			if err == nil {
				return taskState, err
			}
		}

		return tasks_storage.TaskState{}, errors.NewPanicError("Should never reach this line")
	}

	err = s.scheduler.SendEvent(ctx, id, 10)
	require.NoError(t, err)

	// Should return up-to-date Events value.
	taskState, err := updateTask()
	require.NoError(t, err)
	require.EqualValues(t, []int64{10}, taskState.Events)

	// Events should be unique.
	err = s.scheduler.SendEvent(ctx, id, 10)
	require.NoError(t, err)

	// Should not update "events" field in task state.
	taskState, err = updateTask()
	require.NoError(t, err)
	require.EqualValues(t, []int64{10}, taskState.Events)

	err = s.scheduler.SendEvent(ctx, id, 11)
	require.NoError(t, err)

	// Should return up-to-date Events value.
	taskState, err = updateTask()
	require.NoError(t, err)
	require.EqualValues(t, []int64{10, 11}, taskState.Events)
}

func TestTasksShouldNotRunTasksThatWereNotRegisteredForExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = s.registry.Register("sixTimes", func() tasks.Task {
		return &sixTimesTask{}
	})
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleDoublerTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id)
	require.NoError(t, err)
	require.EqualValues(t, 2*123, response)

	// sixTimes wasn't registered for execution. Shouldn't be executed
	reqCtx = getRequestContext(t, ctx)
	id, err = scheduleSixTimesTask(reqCtx, s.scheduler, 100)
	require.NoError(t, err)

	_, err = waitTaskWithTimeout(
		ctx,
		s.scheduler,
		id,
		10*time.Second,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestTasksShouldRestoreRunningAfterRetriableError(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerUnstableTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleUnstableTask(
		reqCtx,
		s.scheduler,
		newDefaultConfig().GetMaxRetriableErrorCount(),
	)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id)
	require.NoError(t, err)
	require.EqualValues(t, 2, response)
}

func TestTasksShouldFailRunningAfterRetriableErrorCountExceeded(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerUnstableTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleUnstableTask(
		reqCtx,
		s.scheduler,
		newDefaultConfig().GetMaxRetriableErrorCount()+1,
	)
	require.NoError(t, err)

	_, err = waitTask(ctx, s.scheduler, id)
	require.Error(t, err)

	expected := errors.NewRetriableError(assert.AnError)

	status, ok := grpc_status.FromError(err)
	require.True(t, ok)
	require.Equal(t, expected.Error(), status.Message())
}

func TestTasksShouldFailRunningAfterRetriableErrorCountForTaskTypeExceeded(
	t *testing.T,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	config := proto.Clone(newDefaultConfig()).(*tasks_config.TasksConfig)
	runnersCount := uint64(2)
	config.RunnersCount = &runnersCount
	config.StalkingRunnersCount = &runnersCount

	unstableTaskMaxRetriesCount := uint64(6)
	defaultMaxRetriesCount := uint64(100)
	config.MaxRetriableErrorCount = &defaultMaxRetriesCount
	config.MaxRetriableErrorCountByTaskType = map[string]uint64{
		"unstable": unstableTaskMaxRetriesCount,
		"doubler":  uint64(3),
	}

	s := createServicesWithConfig(
		t,
		ctx,
		db,
		config,
		metrics_empty.NewRegistry())

	err := registerUnstableTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleUnstableTask(
		reqCtx,
		s.scheduler,
		unstableTaskMaxRetriesCount+1, // failuresUntilSuccess
	)
	require.NoError(t, err)

	_, err = waitTask(ctx, s.scheduler, id)
	require.Error(t, err)

	expected := errors.NewRetriableError(assert.AnError)

	status, ok := grpc_status.FromError(err)
	require.True(t, ok)
	require.Equal(t, expected.Error(), status.Message())

	failsCount, err := getTaskMetadata(ctx, s.scheduler, id)
	require.NoError(t, err)
	require.EqualValues(t, unstableTaskMaxRetriesCount+1, failsCount)
}

func TestTasksShouldNotRestoreRunningAfterNonRetriableError(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	failure := errors.NewNonRetriableError(assert.AnError)

	err := registerFailureTask(s.registry, failure)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleFailureTask(reqCtx, s.scheduler)
	require.NoError(t, err)

	_, err = waitTask(ctx, s.scheduler, id)
	require.Error(t, err)

	status, ok := grpc_status.FromError(err)
	require.True(t, ok)
	require.Equal(t, failure.Error(), status.Message())
}

func TestTasksRunningTwoConcurrentTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id1, err := scheduleDoublerTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	id2, err := scheduleDoublerTask(reqCtx, s.scheduler, 456)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id1)
	require.NoError(t, err)
	require.EqualValues(t, 2*123, response)

	response, err = waitTask(ctx, s.scheduler, id2)
	require.NoError(t, err)
	require.EqualValues(t, 2*456, response)
}

func TestTasksRunningTwoConcurrentTasksReverseWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id1, err := scheduleDoublerTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	id2, err := scheduleDoublerTask(reqCtx, s.scheduler, 456)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id2)
	require.NoError(t, err)
	require.EqualValues(t, 2*456, response)

	response, err = waitTask(ctx, s.scheduler, id1)
	require.NoError(t, err)
	require.EqualValues(t, 2*123, response)
}

// Need at least two runners here because we have infinitely long
// CollectListerMetrics task.
func TestTasksRunningTwoConcurrentTasksOnTwoRunners(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id1, err := scheduleDoublerTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	id2, err := scheduleDoublerTask(reqCtx, s.scheduler, 456)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id1)
	require.NoError(t, err)
	require.EqualValues(t, 2*123, response)

	response, err = waitTask(ctx, s.scheduler, id2)
	require.NoError(t, err)
	require.EqualValues(t, 2*456, response)
}

// Need at least two runners here because we have infinitely long
// CollectListerMetrics task.
func TestTasksRunningTwoConcurrentTasksOnTwoRunnersReverseWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id1, err := scheduleDoublerTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	id2, err := scheduleDoublerTask(reqCtx, s.scheduler, 456)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id2)
	require.NoError(t, err)
	require.EqualValues(t, 2*456, response)

	response, err = waitTask(ctx, s.scheduler, id1)
	require.NoError(t, err)
	require.EqualValues(t, 2*123, response)
}

func TestTasksRunningDependentTask(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = registerSixTimesTask(s.registry, s.scheduler)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleSixTimesTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id)
	require.NoError(t, err)
	require.EqualValues(t, 6*123, response)
}

// Need at least two runners here because we have infinitely long
// CollectListerMetrics task.
func TestTasksRunningDependentTaskOnTwoRunners(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerDoublerTask(s.registry)
	require.NoError(t, err)

	err = registerSixTimesTask(s.registry, s.scheduler)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleSixTimesTask(reqCtx, s.scheduler, 123)
	require.NoError(t, err)

	response, err := waitTask(ctx, s.scheduler, id)

	require.NoError(t, err)
	require.EqualValues(t, 6*123, response)
}

func TestTasksRunningRegularTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := s.registry.RegisterForExecution("regular", func() tasks.Task {
		return &regularTask{}
	})
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	regularTaskMutex.Lock()
	regularTaskCounter = 0
	regularTaskMutex.Unlock()

	s.scheduler.ScheduleRegularTasks(
		ctx,
		"regular",
		tasks.TaskSchedule{
			ScheduleInterval: time.Millisecond,
			MaxTasksInflight: 2,
		},
	)

	for {
		<-time.After(10 * time.Millisecond)

		regularTaskMutex.Lock()

		if regularTaskCounter > 4 {
			regularTaskMutex.Unlock()
			break
		}

		regularTaskMutex.Unlock()
	}
}

func newHangingTaskTestConfig() *tasks_config.TasksConfig {
	config := proto.Clone(newDefaultConfig()).(*tasks_config.TasksConfig)
	runnersCount := uint64(10)
	config.RunnersCount = &runnersCount
	config.StalkingRunnersCount = &runnersCount

	taskWaitingTimeout := "10s"
	config.TaskWaitingTimeout = &taskWaitingTimeout
	timeoutString := "5s"
	config.InflightHangingTaskTimeout = &timeoutString

	metricsCollectionInterval := "10ms"
	config.ListerMetricsCollectionInterval = &metricsCollectionInterval
	config.ExceptHangingTaskTypes = []string{
		"tasks.CollectListerMetrics",
		"tasks.ClearEndedTasks",
	}

	return config
}

func waitForValue(t *testing.T, ch chan (int), expectedValue int) {
	for {
		value, ok := <-ch
		if !ok {
			require.Fail(t, fmt.Sprintf(
				"Channel is closed, but value %v was not received",
				expectedValue,
			))
		}

		if value == expectedValue {
			return
		}
	}
}

func TestHangingTasksMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	registry := mocks.NewIgnoreUnknownCallsRegistryMock()

	config := newHangingTaskTestConfig()

	s := createServicesWithConfig(t, ctx, db, config, registry)
	err := registerHangingTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	taskID, err := scheduleHangingTask(reqCtx, s.scheduler)
	require.NoError(t, err)

	// When a value is set to a metric, this value is also sent to the
	// corresponging channel (without blocking).
	// The test reads values from these channels and validates them.
	totalHangingTasksChannel := make(chan int, 100000)
	hangingTasksChannel := make(chan int, 100000)

	registry.GetGauge(
		"totalHangingTaskCount",
		map[string]string{"type": "tasks.hanging"},
	).On("Set", float64(1)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			totalHangingTasksChannel <- 1
		},
	)
	registry.GetGauge(
		"hangingTasks",
		map[string]string{"type": "tasks.hanging", "id": taskID},
	).On("Set", float64(1)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			hangingTasksChannel <- 1
		},
	)

	registry.GetGauge(
		"totalHangingTaskCount",
		map[string]string{"type": "tasks.hanging"},
	).On("Set", float64(0)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			totalHangingTasksChannel <- 0
		},
	)
	registry.GetGauge(
		"hangingTasks",
		map[string]string{"type": "tasks.hanging", "id": taskID},
	).On("Set", float64(0)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			hangingTasksChannel <- 0
		},
	)

	// Must see value "1" when task becomes hanging.
	waitForValue(t, totalHangingTasksChannel, 1)
	waitForValue(t, hangingTasksChannel, 1)

	_, err = s.scheduler.CancelTask(ctx, taskID)
	require.NoError(t, err)
	_ = s.scheduler.WaitTaskEnded(ctx, taskID)

	// Must see value "0" when task stops being hanging.
	waitForValue(t, totalHangingTasksChannel, 0)
	waitForValue(t, hangingTasksChannel, 0)

	registry.AssertAllExpectations(t)
}

func TestHangingTasksMetricsAreSetEvenForTasksNotRegisteredForExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	registry := mocks.NewIgnoreUnknownCallsRegistryMock()

	config := newHangingTaskTestConfig()

	s := createServicesWithConfig(t, ctx, db, config, registry)
	err := registerLongTaskNotForExecution(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	gaugeSetChannel := make(chan int)

	registry.GetGauge(
		"totalHangingTaskCount",
		map[string]string{"type": "long"},
	).On("Set", float64(0)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			select {
			case gaugeSetChannel <- 0:
			default:
			}
		},
	)

	<-gaugeSetChannel
	registry.AssertAllExpectations(t)
}

func TestListerMetricsCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	// Do not defer db.Close(ctx) in this test.

	registry := mocks.NewIgnoreUnknownCallsRegistryMock()

	config := newHangingTaskTestConfig()

	s := createServicesWithConfig(t, ctx, db, config, registry)
	err := registerHangingTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	taskID, err := scheduleHangingTask(reqCtx, s.scheduler)
	require.NoError(t, err)

	// When a value is set to a metric, this value is also sent to the
	// corresponging channel (without blocking).
	// The test reads values from these channels and validates them.
	totalHangingTasksChannel := make(chan int, 100000)
	hangingTasksChannel := make(chan int, 100000)
	runningTasksChannel := make(chan int, 100000)

	registry.GetGauge(
		"running",
		map[string]string{"type": "tasks.hanging"},
	).On("Set", float64(1)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			runningTasksChannel <- 1
		},
	)
	registry.GetGauge(
		"totalHangingTaskCount",
		map[string]string{"type": "tasks.hanging"},
	).On("Set", float64(1)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			totalHangingTasksChannel <- 1
		},
	)
	registry.GetGauge(
		"hangingTasks",
		map[string]string{"type": "tasks.hanging", "id": taskID},
	).On("Set", float64(1)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			hangingTasksChannel <- 1
		},
	)

	registry.GetGauge(
		"running",
		map[string]string{"type": "tasks.hanging"},
	).On("Set", float64(0)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			runningTasksChannel <- 0
		},
	)
	registry.GetGauge(
		"totalHangingTaskCount",
		map[string]string{"type": "tasks.hanging"},
	).On("Set", float64(0)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			totalHangingTasksChannel <- 0
		},
	)
	registry.GetGauge(
		"hangingTasks",
		map[string]string{"type": "tasks.hanging", "id": taskID},
	).On("Set", float64(0)).Return(mock.Anything).Run(
		func(args mock.Arguments) {
			hangingTasksChannel <- 0
		},
	)

	waitForValue(t, runningTasksChannel, 1)
	// Must see value "1" when task becomes hanging.
	waitForValue(t, totalHangingTasksChannel, 1)
	waitForValue(t, hangingTasksChannel, 1)

	// Close connection to YDB to enforce collectListerMetricsTask failure.
	err = db.Close(ctx)
	require.NoError(t, err)

	// When collectListerMetricsTask fails, it must clean up metrics.
	waitForValue(t, runningTasksChannel, 0)
	waitForValue(t, totalHangingTasksChannel, 0)
	waitForValue(t, hangingTasksChannel, 0)

	registry.AssertAllExpectations(t)
}

func createTaskWithEndTime(
	t *testing.T,
	ctx context.Context,
	storage tasks_storage.Storage,
	idempotencyKey string,
	at time.Time,
	status tasks_storage.TaskStatus,
) string {

	taskId, err := storage.CreateTask(ctx, tasks_storage.TaskState{
		IdempotencyKey: idempotencyKey,
		TaskType:       "task1",
		Description:    "Some task",
		CreatedAt:      at,
		CreatedBy:      "some_user",
		ModifiedAt:     at,
		GenerationID:   0,
		Status:         status,
		State:          []byte{},
		Dependencies:   common.NewStringSet(),
		EndedAt:        at,
	})
	require.NoError(t, err)

	return taskId
}

func TestClearEndedTasksBulk(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	// Set chunk size to 1 so clearEndedTasksChunk will be called several times
	clearEndedTasksLimit := uint64(1)
	s.config.ClearEndedTasksLimit = &clearEndedTasksLimit

	err := s.startRunners(ctx)
	require.NoError(t, err)

	expirationTimeout, err := time.ParseDuration(
		*s.config.EndedTaskExpirationTimeout)
	require.NoError(t, err)

	clearScheduleInterval, err := time.ParseDuration(
		*s.config.ClearEndedTasksTaskScheduleInterval)
	require.NoError(t, err)

	expiredTime := time.Now().Add(-2 * expirationTimeout)
	remainingTaskIds := []string{
		createTaskWithEndTime(
			t,
			ctx,
			s.storage,
			fmt.Sprintf("%v_remaining_finished", t.Name()),
			time.Now(),
			tasks_storage.TaskStatusFinished,
		),
		createTaskWithEndTime(
			t,
			ctx,
			s.storage,
			fmt.Sprintf("%v_remaining_cancelled", t.Name()),
			time.Now(),
			tasks_storage.TaskStatusCancelled,
		),
		createTaskWithEndTime(
			t,
			ctx,
			s.storage,
			fmt.Sprintf("%v_remaining_running", t.Name()),
			expiredTime,
			tasks_storage.TaskStatusRunning,
		),
	}

	expiredTaskIds := []string{
		createTaskWithEndTime(
			t,
			ctx,
			s.storage,
			fmt.Sprintf("%v_expired_finished", t.Name()),
			expiredTime,
			tasks_storage.TaskStatusFinished,
		),
		createTaskWithEndTime(
			t,
			ctx,
			s.storage,
			fmt.Sprintf("%v_expired_cancelled", t.Name()),
			expiredTime,
			tasks_storage.TaskStatusCancelled,
		),
	}

	// Wait for ClearEndedTasks to run
	time.Sleep(2 * clearScheduleInterval)

	for _, taskId := range remainingTaskIds {
		_, err := s.storage.GetTask(ctx, taskId)
		require.NoError(t, err)
	}

	for _, taskId := range expiredTaskIds {
		_, err := s.storage.GetTask(ctx, taskId)
		require.ErrorIs(t, err, errors.NewNotFoundErrorWithTaskID(taskId))
	}
}

func TestTaskInflightDuration(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 2)

	err := registerLongTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleLongTask(reqCtx, s.scheduler)
	require.NoError(t, err)

	_, err = waitTask(ctx, s.scheduler, id)
	require.NoError(t, err)

	taskState, err := s.storage.GetTask(ctx, id)
	require.NoError(t, err)

	require.True(t, tasks_storage.IsEnded(taskState.Status))

	actualDuration := taskState.InflightDuration
	expectedDuration := 10 * time.Second
	require.GreaterOrEqual(t, actualDuration, expectedDuration)
}

func TestTaskInflightDurationDoesNotCountWaitingStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	// runnersCount must be at least the count of tasks + 1 (for CollectListerMetrics).
	// Otherwise, a race condition can occur where longTask is picked up before waitingTaskWithSleep.
	// https://github.com/ydb-platform/nbs/pull/4002
	s := createServices(t, ctx, db, 3 /* runnersCount */)

	err := registerLongTask(s.registry)
	require.NoError(t, err)

	err = registerWaitingTaskWithSleep(s.registry, s.scheduler)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	depTaskID, err := scheduleLongTask(reqCtx, s.scheduler)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	waitingTaskWithSleepID, err := scheduleWaitingTaskWithSleep(reqCtx, s.scheduler, depTaskID)
	require.NoError(t, err)

	timeout := 30 * time.Second

	_, err = waitTaskWithTimeout(ctx, s.scheduler, waitingTaskWithSleepID, timeout)
	require.NoError(t, err)
	_, err = waitTaskWithTimeout(ctx, s.scheduler, depTaskID, timeout)
	require.NoError(t, err)

	waitingState, err := s.storage.GetTask(ctx, waitingTaskWithSleepID)
	require.NoError(t, err)

	inflightDuration := waitingState.InflightDuration
	waitingDuration := waitingState.WaitingDuration
	totalDuration := waitingState.EndedAt.Sub(waitingState.CreatedAt)

	// The delay between task scheduling and runner task pickup is not included
	// in WaitingDuration, so it can be less than expected.
	waitingThreshold := 2 * time.Second

	// First waitingTaskWithSleep iteration: it waits for 2 seconds, then
	// adds a dependency and interrupts.
	// Second waitingTaskWithSleep iteration: it waits for 2 seconds, then
	// skips WaitTask (as the dependency is finished) and waits for 5 seconds.
	// Total: 2+2+5 = 9 seconds.
	require.GreaterOrEqual(t, inflightDuration, 9*time.Second)
	// Due to 2 seconds delay at the start, WaitingDuration will be 10-2 = 8 seconds.
	require.GreaterOrEqual(t, waitingDuration, 8*time.Second-waitingThreshold)
	require.GreaterOrEqual(t, totalDuration, 17*time.Second)
	require.LessOrEqual(t, inflightDuration+waitingDuration, totalDuration)
}

func TestTaskWaitingDurationInChain(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	s := createServices(t, ctx, db, 4)

	err := registerLongTask(s.registry)
	require.NoError(t, err)

	err = registerWaitingTaskWithSleep(s.registry, s.scheduler)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	task1ID, err := scheduleLongTask(reqCtx, s.scheduler)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	task2ID, err := scheduleWaitingTaskWithSleep(reqCtx, s.scheduler, task1ID)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	task3ID, err := scheduleWaitingTaskWithSleep(reqCtx, s.scheduler, task2ID)
	require.NoError(t, err)

	timeout := 30 * time.Second

	for _, taskID := range []string{task1ID, task2ID, task3ID} {
		_, err = waitTaskWithTimeout(ctx, s.scheduler, taskID, timeout)
		require.NoError(t, err)
	}

	waitingThreshold := 2 * time.Second

	state1, err := s.storage.GetTask(ctx, task1ID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, state1.InflightDuration, 10*time.Second)
	require.EqualValues(t, 0, state1.WaitingDuration)

	state2, err := s.storage.GetTask(ctx, task2ID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, state2.InflightDuration, 9*time.Second)
	require.GreaterOrEqual(
		t, state2.WaitingDuration, 8*time.Second-waitingThreshold,
	)

	task2TotalDuration := state2.EndedAt.Sub(state2.CreatedAt)
	require.LessOrEqual(
		t, state2.InflightDuration+state2.WaitingDuration, task2TotalDuration,
	)

	state3, err := s.storage.GetTask(ctx, task3ID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, state3.InflightDuration, 9*time.Second)
	require.GreaterOrEqual(
		t, state3.WaitingDuration, 8*time.Second-waitingThreshold,
	)

	task3TotalDuration := state3.EndedAt.Sub(state3.CreatedAt)
	require.LessOrEqual(
		t, state3.InflightDuration+state3.WaitingDuration, task3TotalDuration,
	)
}

func testTaskWithEstimatedDurationOverride(
	t *testing.T,
	overrideEstimatedInflightDuration bool,
	overrideEstimatedStallingDuration bool,
) {

	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db := newYDB(ctx, t)
	defer db.Close(ctx)

	runnersCount := uint64(2)

	config := newDefaultConfig()
	config.RunnersCount = &runnersCount
	if overrideEstimatedInflightDuration {
		config.OverrideEstimatedInflightDurationByTaskType = map[string]string{
			"long": "42h",
		}
	}
	if overrideEstimatedStallingDuration {
		config.OverrideEstimatedStallingDurationByTaskType = map[string]string{
			"long": "42s",
		}
	}

	s := createServicesWithConfig(t, ctx, db, config, metrics_empty.NewRegistry())

	// Schedule long task so pinger will have time to update tasks state in storage.
	err := registerLongTask(s.registry)
	require.NoError(t, err)

	err = s.startRunners(ctx)
	require.NoError(t, err)

	reqCtx := getRequestContext(t, ctx)
	id, err := scheduleLongTask(reqCtx, s.scheduler)
	require.NoError(t, err)

	_, err = waitTask(ctx, s.scheduler, id)
	require.NoError(t, err)

	taskState, err := s.storage.GetTask(ctx, id)
	require.NoError(t, err)

	if overrideEstimatedInflightDuration {
		require.Equal(t, 42*time.Hour, taskState.EstimatedInflightDuration)
	} else {
		require.EqualValues(t, 0, taskState.EstimatedInflightDuration)
	}

	if overrideEstimatedStallingDuration {
		require.Equal(t, 42*time.Second, taskState.EstimatedStallingDuration)
	} else {
		require.EqualValues(t, 0, taskState.EstimatedStallingDuration)
	}
}

func TestTaskWithEstimatedDurationOverride(t *testing.T) {
	testTaskWithEstimatedDurationOverride(t, false, false)
	testTaskWithEstimatedDurationOverride(t, true, false)
	testTaskWithEstimatedDurationOverride(t, false, true)
	testTaskWithEstimatedDurationOverride(t, true, true)
}
