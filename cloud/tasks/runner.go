package tasks

import (
	"context"
	"fmt"
	"time"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
)

////////////////////////////////////////////////////////////////////////////////

type taskHandle struct {
	task    storage.TaskInfo
	onClose func()
}

func (h *taskHandle) close() {
	h.onClose()
}

////////////////////////////////////////////////////////////////////////////////

type channel struct {
	handle chan taskHandle
}

func (c *channel) receive(ctx context.Context) (taskHandle, error) {
	select {
	case <-ctx.Done():
		return taskHandle{}, ctx.Err()
	case handle, more := <-c.handle:
		if !more {
			return taskHandle{}, fmt.Errorf("channel.handle is closed")
		}

		return handle, nil
	}
}

func (c *channel) send(handle taskHandle) bool {
	select {
	case c.handle <- handle:
		return true
	default:
		handle.close()
		return false
	}
}

func (c *channel) close() {
	close(c.handle)
}

////////////////////////////////////////////////////////////////////////////////

type runner interface {
	receiveTask(ctx context.Context) (taskHandle, error)
	lockTask(context.Context, storage.TaskInfo) (storage.TaskState, error)
	executeTask(context.Context, *executionContext, Task)
	lockAndExecuteTask(context.Context, storage.TaskInfo) error
}

////////////////////////////////////////////////////////////////////////////////

type runnerForRun struct {
	storage                          storage.Storage
	registry                         *Registry
	metrics                          runnerMetrics
	channel                          *channel
	pingPeriod                       time.Duration
	pingTimeout                      time.Duration
	host                             string
	id                               string
	maxRetriableErrorCountDefault    uint64
	maxRetriableErrorCountByTaskType map[string]uint64
	maxPanicCount                    uint64

	inflightDurationHangTimeout       time.Duration
	stallingDurationHangTimeout       time.Duration
	totalDurationHangTimeout          time.Duration
	missedEstimatesUntilTaskIsHanging uint64
	maxSampledTaskGeneration          uint64
}

func (r *runnerForRun) receiveTask(
	ctx context.Context,
) (taskHandle, error) {

	return r.channel.receive(ctx)
}

func (r *runnerForRun) lockTask(
	ctx context.Context,
	taskInfo storage.TaskInfo,
) (storage.TaskState, error) {

	logging.Info(
		ctx,
		"locking task with taskInfo %v, host %v, runner id %v",
		taskInfo,
		r.host,
		r.id,
	)
	return r.storage.LockTaskToRun(ctx, taskInfo, time.Now(), r.host, r.id)
}

func (r *runnerForRun) getMaxRetriableErrorCount(taskType string) uint64 {
	count, ok := r.maxRetriableErrorCountByTaskType[taskType]
	if !ok {
		return r.maxRetriableErrorCountDefault
	}

	return count
}

func (r *runnerForRun) executeTask(
	ctx context.Context,
	execCtx *executionContext,
	task Task,
) {

	taskType := execCtx.GetTaskType()
	taskID := execCtx.GetTaskID()

	logging.Info(
		ctx,
		"started execution of %v with task id %v (run)",
		taskType,
		taskID,
	)

	err := r.run(ctx, execCtx, task)

	if ctx.Err() != nil {
		logging.Info(
			ctx,
			"ctx cancelled for %v with task id %v",
			taskType,
			taskID,
		)
		// If context was cancelled, just return.
		return
	}

	if err == nil {
		// If there was no error, task has completed successfully.
		err = execCtx.finish(ctx)
		if err != nil {
			logError(
				ctx,
				err,
				"failed to commit finishing for %v with task id %v",
				taskType,
				taskID,
			)
			r.metrics.OnError(err)
		}
		return
	}

	logError(
		ctx,
		err,
		"got error for %v with task id %v",
		taskType,
		taskID,
	)
	r.metrics.OnExecutionError(err)

	if errors.IsPanicError(err) {
		if execCtx.taskState.PanicCount >= r.maxPanicCount {
			logError(
				ctx,
				err,
				"panic count exceeded for %v with task id %v",
				taskType,
				taskID,
			)
			// Wrap into NonRetriableError to indicate failure.
			wrappedErr := errors.NewNonRetriableError(err)
			r.metrics.OnExecutionError(wrappedErr)

			err = execCtx.setError(ctx, err)
			if err != nil {
				r.metrics.OnError(err)
			}
			return
		}

		err = execCtx.incrementPanicCount(ctx)
		if err != nil {
			logError(
				ctx,
				err,
				"failed to increment panic count for %v with task id %v",
				taskType,
				taskID,
			)
			r.metrics.OnError(err)
		}
		return
	}

	if errors.Is(err, errors.NewWrongGenerationError()) ||
		errors.Is(err, errors.NewInterruptExecutionError()) {
		return
	}

	if errors.Is(err, errors.NewEmptyNonCancellableError()) {
		err = execCtx.setNonCancellableError(ctx, err)
		if err != nil {
			logError(
				ctx,
				err,
				"failed to commit non cancellable error for %v with task id %v",
				taskType,
				taskID,
			)
			r.metrics.OnError(err)
		}
		return
	}

	if errors.Is(err, errors.NewEmptyNonRetriableError()) {
		err = execCtx.setError(ctx, err)
		if err != nil {
			r.metrics.OnError(err)
		}
		return
	}

	if errors.Is(err, errors.NewEmptyAbortedError()) {
		// Restart task from the beginning.
		err = execCtx.clearState(ctx)
		if err != nil {
			logError(
				ctx,
				err,
				"failed to clear state for %v with task id %v",
				taskType,
				taskID,
			)
			r.metrics.OnError(err)
		}
		return
	}

	retriableError := errors.NewEmptyRetriableError()
	if errors.As(err, &retriableError) {
		maxRetriableErrorCountReached := execCtx.getRetriableErrorCount() >=
			r.getMaxRetriableErrorCount(taskType)

		if !retriableError.IgnoreRetryLimit && maxRetriableErrorCountReached {
			logError(
				ctx,
				err,
				"retriable error count exceeded for %v with task id %v",
				taskType,
				taskID,
			)
			// Wrap into NonRetriableError to indicate failure.
			wrappedErr := errors.NewNonRetriableError(err)
			r.metrics.OnExecutionError(wrappedErr)

			err = execCtx.setError(ctx, err)
			if err != nil {
				r.metrics.OnError(err)
			}
			return
		}

		err = execCtx.incrementRetriableErrorCount(ctx)
		if err != nil {
			logError(
				ctx,
				err,
				"failed to increment retriable error count for %v with task id %v",
				taskType,
				taskID,
			)
			r.metrics.OnError(err)
		}
		return
	}

	// This is a significant error, task must be cancelled
	// to undo the damage.
	err = execCtx.setError(ctx, err)
	if err != nil {
		r.metrics.OnError(err)
	}
}

func (r *runnerForRun) run(
	ctx context.Context,
	execCtx *executionContext,
	task Task,
) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewPanicError(r)
		}
	}()

	return task.Run(
		logging.WithTaskID(
			logging.WithComponent(ctx, logging.ComponentTask),
			execCtx.GetTaskID(),
		),
		execCtx,
	)
}

func (r *runnerForRun) lockAndExecuteTask(
	ctx context.Context,
	taskInfo storage.TaskInfo,
) error {

	return lockAndExecuteTask(
		ctx,
		r.storage,
		r.registry,
		r.metrics,
		r.pingPeriod,
		r.pingTimeout,
		r,
		taskInfo,
		r.inflightDurationHangTimeout,
		r.stallingDurationHangTimeout,
		r.totalDurationHangTimeout,
		r.missedEstimatesUntilTaskIsHanging,
		r.maxSampledTaskGeneration,
	)
}

////////////////////////////////////////////////////////////////////////////////

type runnerForCancel struct {
	storage     storage.Storage
	registry    *Registry
	metrics     runnerMetrics
	channel     *channel
	pingPeriod  time.Duration
	pingTimeout time.Duration
	host        string
	id          string

	inflightDurationHangTimeout       time.Duration
	stallingDurationHangTimeout       time.Duration
	totalDurationHangTimeout          time.Duration
	missedEstimatesUntilTaskIsHanging uint64
	maxSampledTaskGeneration          uint64
}

func (r *runnerForCancel) receiveTask(
	ctx context.Context,
) (taskHandle, error) {

	return r.channel.receive(ctx)
}

func (r *runnerForCancel) lockTask(
	ctx context.Context,
	taskInfo storage.TaskInfo,
) (storage.TaskState, error) {

	logging.Info(
		ctx,
		"locking task with taskInfo %v, host %v, runner id %v",
		taskInfo,
		r.host,
		r.id,
	)
	return r.storage.LockTaskToCancel(ctx, taskInfo, time.Now(), r.host, r.id)
}

func (r *runnerForCancel) executeTask(
	ctx context.Context,
	execCtx *executionContext,
	task Task,
) {

	taskType := execCtx.GetTaskType()
	taskID := execCtx.GetTaskID()

	logging.Info(
		ctx,
		"started execution of %v with task id %v (cancel)",
		taskType,
		taskID,
	)

	err := task.Cancel(
		logging.WithTaskID(
			logging.WithComponent(ctx, logging.ComponentTask),
			execCtx.GetTaskID(),
		),
		execCtx,
	)

	if ctx.Err() != nil {
		logging.Info(
			ctx,
			"ctx cancelled for %v with task id %v",
			taskType,
			taskID,
		)
		// If context was cancelled, just return.
		return
	}

	if err != nil {
		logError(
			ctx,
			err,
			"got error for %v with task id %v",
			taskType,
			taskID,
		)
		r.metrics.OnExecutionError(err)
		// Treat any errors (other than NonRetriableError or NonCancellableError)
		// as retriable.
		if !errors.Is(err, errors.NewEmptyNonRetriableError()) &&
			!errors.Is(err, errors.NewEmptyNonCancellableError()) {
			// In case of any retriable errors, we must not drop the cancel job.
			// Let someone else deal with it.
			// TODO: Maybe do need to drop after Nth retry?
			return
		}
	}

	// If we have no error or have non retriable error, the cancellation has
	// completed.
	err = execCtx.setCancelled(ctx)
	if err != nil {
		logError(
			ctx,
			err,
			"failed to commit cancellation for %v with task id %v",
			taskType,
			taskID,
		)
		r.metrics.OnError(err)
	}
}

func (r *runnerForCancel) lockAndExecuteTask(
	ctx context.Context,
	taskInfo storage.TaskInfo,
) error {

	return lockAndExecuteTask(
		ctx,
		r.storage,
		r.registry,
		r.metrics,
		r.pingPeriod,
		r.pingTimeout,
		r,
		taskInfo,
		r.inflightDurationHangTimeout,
		r.stallingDurationHangTimeout,
		r.totalDurationHangTimeout,
		r.missedEstimatesUntilTaskIsHanging,
		r.maxSampledTaskGeneration,
	)
}

////////////////////////////////////////////////////////////////////////////////

func taskPinger(
	ctx context.Context,
	execCtx *executionContext,
	pingPeriod time.Duration,
	pingTimeout time.Duration,
	onError func(),
) {

	logging.Debug(
		ctx,
		"started pinging of task %v",
		execCtx.GetTaskID(),
	)

	for {
		// Use separate func to ensure that "defer cancel()" is called after
		// each iteration.
		ping := func() error {
			pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
			defer cancel()
			return execCtx.ping(pingCtx)
		}

		err := ping()
		// Pinger being cancelled does not constitute an error.
		// It is crucial to check original ctx here.
		if err != nil && ctx.Err() == nil {
			logError(
				ctx,
				err,
				"failed to ping %v",
				execCtx.GetTaskID(),
			)
			onError()
			return
		}
		select {
		case <-ctx.Done():
			logging.Debug(
				ctx,
				"stopped pinging of task %v",
				execCtx.GetTaskID(),
			)
			return
		case <-time.After(pingPeriod):
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func lockAndExecuteTask(
	ctx context.Context,
	taskStorage storage.Storage,
	registry *Registry,
	runnerMetrics runnerMetrics,
	pingPeriod time.Duration,
	pingTimeout time.Duration,
	runner runner,
	taskInfo storage.TaskInfo,
	inflightDurationHangTimeout time.Duration,
	stallingDurationHangTimeout time.Duration,
	totalDurationHangTimeout time.Duration,
	missedEstimatesUntilTaskIsHanging uint64,
	maxSampledTaskGeneration uint64,
) error {

	taskState, err := runner.lockTask(ctx, taskInfo)
	if err != nil {
		logError(
			ctx,
			err,
			"failed to lock task %v",
			taskInfo,
		)
		runnerMetrics.OnError(err)
		return err
	}

	task, err := registry.NewTask(taskState.TaskType)
	if err != nil {
		logError(
			ctx,
			err,
			"failed to construct task %v",
			taskInfo,
		)
		runnerMetrics.OnError(err)
		// If we've failed to construct this task probably because our
		// version does not support it yet.
		return err
	}

	err = task.Load(taskState.Request, taskState.State)
	if err != nil {
		logError(
			ctx,
			err,
			"failed to load task %v",
			taskInfo,
		)
		runnerMetrics.OnError(err)
		// This task might be corrupted.
		return err
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	runCtx = headers.Append(runCtx, taskState.Metadata.Vals())
	// All derived tasks should be pinned to the same storage folder.
	runCtx = setStorageFolder(runCtx, taskState.StorageFolder)
	runCtx = logging.WithCommonFields(runCtx)
	runCtx = tracing.GetTracingContext(runCtx)

	runCtx, span := tracing.StartSpanWithSampling(
		runCtx,
		fmt.Sprintf(taskInfo.TaskType),
		taskInfo.GenerationID <= maxSampledTaskGeneration, // sampled
		tracing.WithAttributes(
			tracing.AttributeString("task_id", taskInfo.ID),
			tracing.AttributeString("request_id", headers.GetRequestID(runCtx)),
			tracing.AttributeInt64(
				"generation_id",
				int64(taskInfo.GenerationID),
			),
			tracing.AttributeBool("regular", taskState.Regular),
		),
	)
	defer span.End()

	execCtx := newExecutionContext(
		task,
		taskStorage,
		taskState,
		inflightDurationHangTimeout,
		stallingDurationHangTimeout,
		totalDurationHangTimeout,
		missedEstimatesUntilTaskIsHanging,
	)

	pingCtx, cancelPing := context.WithCancel(ctx)
	go taskPinger(pingCtx, execCtx, pingPeriod, pingTimeout, cancelRun)
	defer cancelPing()

	runnerMetrics.OnExecutionStarted(execCtx)
	logging.Info(ctx, "started execution of task %v", taskInfo)

	runner.executeTask(runCtx, execCtx, task)

	runnerMetrics.OnExecutionStopped()
	logging.Info(ctx, "stopped execution of task %v", taskInfo)

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func runnerLoop(ctx context.Context, registry *Registry, runner runner) {
	logging.Info(ctx, "started runner loop")

	for {
		handle, err := runner.receiveTask(ctx)
		if err != nil {
			logging.Warn(ctx, "iteration stopped: %v", err)
			return
		}

		logging.Info(ctx, "iteration trying %v", handle.task)
		err = runner.lockAndExecuteTask(ctx, handle.task)
		if err == nil {
			logging.Info(ctx, "iteration completed successfully %v", handle.task)
		}
		handle.close()

		if ctx.Err() != nil {
			logging.Info(ctx, "iteration stopped: ctx cancelled")
			return
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func startRunner(
	ctx context.Context,
	taskStorage storage.Storage,
	registry *Registry,
	runnerMetricsRegistry metrics.Registry,
	channelForRun *channel,
	channelForCancel *channel,
	pingPeriod time.Duration,
	pingTimeout time.Duration,
	inflightDurationHangTimeout time.Duration,
	stallingDurationHangTimeout time.Duration,
	totalDurationHangTimeout time.Duration,
	missedEstimatesUntilTaskIsHanging uint64,
	exceptHangingTaskTypes []string,
	host string,
	idForRun string,
	idForCancel string,
	maxRetriableErrorCountDefault uint64,
	maxRetriableErrorCountByTaskType map[string]uint64,
	maxPanicCount uint64,
	maxSampledTaskGeneration uint64,
) error {

	// TODO: More granular control on runners and cancellers.

	runnerForRunMetrics := newRunnerMetrics(
		ctx,
		runnerMetricsRegistry,
		exceptHangingTaskTypes,
	)

	go runnerLoop(ctx, registry, &runnerForRun{
		storage:                          taskStorage,
		registry:                         registry,
		metrics:                          runnerForRunMetrics,
		channel:                          channelForRun,
		pingPeriod:                       pingPeriod,
		pingTimeout:                      pingTimeout,
		host:                             host,
		id:                               idForRun,
		maxRetriableErrorCountDefault:    maxRetriableErrorCountDefault,
		maxRetriableErrorCountByTaskType: maxRetriableErrorCountByTaskType,
		maxPanicCount:                    maxPanicCount,

		inflightDurationHangTimeout:       inflightDurationHangTimeout,
		stallingDurationHangTimeout:       stallingDurationHangTimeout,
		totalDurationHangTimeout:          totalDurationHangTimeout,
		missedEstimatesUntilTaskIsHanging: missedEstimatesUntilTaskIsHanging,
		maxSampledTaskGeneration:          maxSampledTaskGeneration,
	})

	runnerForCancelMetrics := newRunnerMetrics(
		ctx,
		runnerMetricsRegistry,
		exceptHangingTaskTypes,
	)

	go runnerLoop(ctx, registry, &runnerForCancel{
		storage:     taskStorage,
		registry:    registry,
		metrics:     runnerForCancelMetrics,
		channel:     channelForCancel,
		pingPeriod:  pingPeriod,
		pingTimeout: pingTimeout,
		host:        host,
		id:          idForCancel,

		inflightDurationHangTimeout:       inflightDurationHangTimeout,
		stallingDurationHangTimeout:       stallingDurationHangTimeout,
		totalDurationHangTimeout:          totalDurationHangTimeout,
		missedEstimatesUntilTaskIsHanging: missedEstimatesUntilTaskIsHanging,
		maxSampledTaskGeneration:          maxSampledTaskGeneration,
	})

	return nil
}

func startRunners(
	ctx context.Context,
	runnerCount uint64,
	taskStorage storage.Storage,
	registry *Registry,
	runnerMetricsRegistry metrics.Registry,
	channelsForRun []*channel,
	channelsForCancel []*channel,
	pingPeriod time.Duration,
	pingTimeout time.Duration,
	inflightDurationHangTimeout time.Duration,
	stallingDurationHangTimeout time.Duration,
	totalDurationHangTimeout time.Duration,
	missedEstimatesUntilTaskIsHanging uint64,
	exceptHangingTaskTypes []string,
	host string,
	maxRetriableErrorCountDefault uint64,
	maxRetriableErrorCountByTaskType map[string]uint64,
	maxPanicCount uint64,
	maxSampledTaskGeneration uint64,
) error {

	for i := uint64(0); i < runnerCount; i++ {
		err := startRunner(
			ctx,
			taskStorage,
			registry,
			runnerMetricsRegistry,
			channelsForRun[i],
			channelsForCancel[i],
			pingPeriod,
			pingTimeout,
			inflightDurationHangTimeout,
			stallingDurationHangTimeout,
			totalDurationHangTimeout,
			missedEstimatesUntilTaskIsHanging,
			exceptHangingTaskTypes,
			host,
			fmt.Sprintf("run_%v", i),
			fmt.Sprintf("cancel_%v", i),
			maxRetriableErrorCountDefault,
			maxRetriableErrorCountByTaskType,
			maxPanicCount,
			maxSampledTaskGeneration,
		)
		if err != nil {
			return fmt.Errorf("failed to start runner #%d: %w", i, err)
		}
	}

	return nil
}

func startStalkingRunners(
	ctx context.Context,
	runnerCount uint64,
	taskStorage storage.Storage,
	registry *Registry,
	runnerMetricsRegistry metrics.Registry,
	channelsForRun []*channel,
	channelsForCancel []*channel,
	pingPeriod time.Duration,
	pingTimeout time.Duration,
	inflightDurationHangTimeout time.Duration,
	stallingDurationHangTimeout time.Duration,
	totalDurationHangTimeout time.Duration,
	missedEstimatesUntilTaskIsHanging uint64,
	exceptHangingTaskTypes []string,
	host string,
	maxRetriableErrorCountDefault uint64,
	maxRetriableErrorCountByTaskType map[string]uint64,
	maxPanicCount uint64,
	maxSampledTaskGeneration uint64,
) error {

	for i := uint64(0); i < runnerCount; i++ {
		err := startRunner(
			ctx,
			taskStorage,
			registry,
			runnerMetricsRegistry,
			channelsForRun[i],
			channelsForCancel[i],
			pingPeriod,
			pingTimeout,
			inflightDurationHangTimeout,
			stallingDurationHangTimeout,
			totalDurationHangTimeout,
			missedEstimatesUntilTaskIsHanging,
			exceptHangingTaskTypes,
			host,
			fmt.Sprintf("stalker_run_%v", i),
			fmt.Sprintf("stalker_cancel_%v", i),
			maxRetriableErrorCountDefault,
			maxRetriableErrorCountByTaskType,
			maxPanicCount,
			maxSampledTaskGeneration,
		)
		if err != nil {
			return fmt.Errorf("failed to start stalking runner #%d: %w", i, err)
		}
	}

	return nil
}

// Registers the node and starts sending heartbeats.
func startHeartbeats(
	ctx context.Context,
	interval time.Duration,
	host string,
	storage storage.Storage,
	inflightTaskCountReporter func() uint32,
) {

	logging.Debug(
		ctx,
		"Start sending heartbeats",
	)

	ticker := time.NewTicker(interval)

	for {

		_ = storage.HeartbeatNode(ctx, host, time.Now(), inflightTaskCountReporter())

		select {
		case <-ticker.C:

		case <-ctx.Done():
			logging.Debug(
				ctx,
				"Heartbeats are stopped by the context cancellation",
			)
			return
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func StartRunners(
	ctx context.Context,
	taskStorage storage.Storage,
	registry *Registry,
	runnerMetricsRegistry metrics.Registry,
	config *tasks_config.TasksConfig,
	host string,
) error {

	pollForTasksPeriodMin, err := time.ParseDuration(config.GetPollForTasksPeriodMin())
	if err != nil {
		return err
	}

	pollForTasksPeriodMax, err := time.ParseDuration(config.GetPollForTasksPeriodMax())
	if err != nil {
		return err
	}

	if pollForTasksPeriodMin > pollForTasksPeriodMax {
		return fmt.Errorf(
			"pollForTasksPeriodMin should not be greater than pollForTasksPeriodMax",
		)
	}

	pollForStallingTasksPeriodMin, err := time.ParseDuration(config.GetPollForStallingTasksPeriodMin())
	if err != nil {
		return err
	}

	pollForStallingTasksPeriodMax, err := time.ParseDuration(config.GetPollForStallingTasksPeriodMax())
	if err != nil {
		return err
	}

	if pollForStallingTasksPeriodMin > pollForStallingTasksPeriodMax {
		return fmt.Errorf(
			"pollForStallingTasksPeriodMin should not be greater than pollForStallingTasksPeriodMax",
		)
	}

	pingPeriod, err := time.ParseDuration(config.GetTaskPingPeriod())
	if err != nil {
		return err
	}

	// Use TaskStallingTimeout as ping timeout, because there is no sense in
	// pinging stalling task.
	pingTimeout, err := time.ParseDuration(config.GetTaskStallingTimeout())
	if err != nil {
		return err
	}

	inflightDurationHangTimeout, err := time.ParseDuration(config.GetInflightDurationHangTimeout())
	if err != nil {
		return err
	}

	stallingDurationHangTimeout, err := time.ParseDuration(config.GetStallingDurationHangTimeout())
	if err != nil {
		return err
	}

	totalDurationHangTimeout, err := time.ParseDuration(config.GetTotalDurationHangTimeout())
	if err != nil {
		return err
	}

	inflightTaskLimits := config.GetInflightTaskPerNodeLimits()

	taskTypesForExecution := registry.TaskTypesForExecution()

	listerReadyToRun := newLister(
		ctx,
		func(ctx context.Context, limit uint64) ([]storage.TaskInfo, error) {
			return taskStorage.ListTasksReadyToRun(
				ctx,
				limit,
				taskTypesForExecution,
			)
		},
		config.GetRunnersCount(),
		config.GetTasksToListLimit(),
		pollForTasksPeriodMin,
		pollForTasksPeriodMax,
		inflightTaskLimits,
	)
	listerReadyToCancel := newLister(
		ctx,
		func(ctx context.Context, limit uint64) ([]storage.TaskInfo, error) {
			return taskStorage.ListTasksReadyToCancel(
				ctx,
				limit,
				taskTypesForExecution,
			)
		},
		config.GetRunnersCount(),
		config.GetTasksToListLimit(),
		pollForTasksPeriodMin,
		pollForTasksPeriodMax,
		inflightTaskLimits,
	)

	err = startRunners(
		ctx,
		config.GetRunnersCount(),
		taskStorage,
		registry,
		runnerMetricsRegistry,
		listerReadyToRun.channels,
		listerReadyToCancel.channels,
		pingPeriod,
		pingTimeout,
		inflightDurationHangTimeout,
		stallingDurationHangTimeout,
		totalDurationHangTimeout,
		config.GetMissedEstimatesUntilTaskIsHanging(),
		config.GetExceptHangingTaskTypes(),
		host,
		config.GetMaxRetriableErrorCount(),
		config.GetMaxRetriableErrorCountByTaskType(),
		config.GetMaxPanicCount(),
		config.GetMaxSampledTaskGeneration(),
	)
	if err != nil {
		return err
	}

	listerStallingWhileRunning := newLister(
		ctx,
		func(ctx context.Context, limit uint64) ([]storage.TaskInfo, error) {
			return taskStorage.ListTasksStallingWhileRunning(
				ctx,
				host,
				limit,
				taskTypesForExecution,
			)
		},
		config.GetStalkingRunnersCount(),
		config.GetTasksToListLimit(),
		pollForStallingTasksPeriodMin,
		pollForStallingTasksPeriodMax,
		inflightTaskLimits,
	)
	listerStallingWhileCancelling := newLister(
		ctx,
		func(ctx context.Context, limit uint64) ([]storage.TaskInfo, error) {
			return taskStorage.ListTasksStallingWhileCancelling(
				ctx,
				host,
				limit,
				taskTypesForExecution,
			)
		},
		config.GetStalkingRunnersCount(),
		config.GetTasksToListLimit(),
		pollForStallingTasksPeriodMin,
		pollForStallingTasksPeriodMax,
		inflightTaskLimits,
	)

	err = startStalkingRunners(
		ctx,
		config.GetStalkingRunnersCount(),
		taskStorage,
		registry,
		runnerMetricsRegistry,
		listerStallingWhileRunning.channels,
		listerStallingWhileCancelling.channels,
		pingPeriod,
		pingTimeout,
		inflightDurationHangTimeout,
		stallingDurationHangTimeout,
		totalDurationHangTimeout,
		config.GetMissedEstimatesUntilTaskIsHanging(),
		config.GetExceptHangingTaskTypes(),
		host,
		config.GetMaxRetriableErrorCount(),
		config.GetMaxRetriableErrorCountByTaskType(),
		config.GetMaxPanicCount(),
		config.GetMaxSampledTaskGeneration(),
	)
	if err != nil {
		return err
	}

	// Return the total number of inflight tasks.
	totalInflightTaskCountReporter := func() uint32 {
		return listerReadyToRun.getInflightTaskCount() +
			listerReadyToCancel.getInflightTaskCount() +
			listerStallingWhileRunning.getInflightTaskCount() +
			listerStallingWhileCancelling.getInflightTaskCount()
	}

	heartbeatInterval, err := time.ParseDuration(config.GetHearbeatInterval())
	if err != nil {
		return fmt.Errorf("could not parse heartbeat interval, reason: %w", err)
	}

	go startHeartbeats(
		ctx,
		heartbeatInterval,
		host,
		taskStorage,
		totalInflightTaskCountReporter,
	)
	return nil
}
