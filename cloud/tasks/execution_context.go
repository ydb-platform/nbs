package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
	grpc_codes "google.golang.org/grpc/codes"
)

////////////////////////////////////////////////////////////////////////////////

type ExecutionContext interface {
	SaveState(ctx context.Context) error

	SaveStateWithPreparation(
		ctx context.Context,
		preparation func(context.Context, *persistence.Transaction) error,
	) error

	GetTaskType() string

	GetTaskID() string

	// Dependencies are automatically added by Scheduler.WaitTask.
	AddTaskDependency(ctx context.Context, taskID string) error

	IsHanging() bool

	SetEstimatedInflightDuration(estimatedInflightDuration time.Duration)

	SetEstimatedStallingDuration(estimatedStallingDuration time.Duration)

	HasEvent(ctx context.Context, event int64) bool

	FinishWithPreparation(
		ctx context.Context,
		preparation func(context.Context, *persistence.Transaction) error,
	) error
}

////////////////////////////////////////////////////////////////////////////////

type executionContext struct {
	task           Task
	storage        storage.Storage
	taskState      storage.TaskState
	taskStateMutex sync.Mutex
	finished       bool

	hangingTaskTimeout                time.Duration
	inflightHangingTaskTimeout        time.Duration
	stallingHangingTaskTimeout        time.Duration
	missedEstimatesUntilTaskIsHanging uint64
}

// HACK from https://github.com/stretchr/testify/pull/694/files to avoid fake race detection
func (c *executionContext) String() string {
	return fmt.Sprintf("%[1]T<%[1]p>", c)
}

func (c *executionContext) SaveState(ctx context.Context) error {
	return c.SaveStateWithPreparation(ctx, nil /* preparation */)
}

func (c *executionContext) SaveStateWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateStateWithPreparation(
		ctx,
		func(taskState storage.TaskState) storage.TaskState {
			logging.Info(ctx, "saving state for task %v", taskState.ID)

			taskState.State = state
			return taskState
		},
		preparation,
	)
}

func (c *executionContext) GetTaskType() string {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()
	return c.taskState.TaskType
}

func (c *executionContext) GetTaskID() string {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()
	return c.taskState.ID
}

func (c *executionContext) AddTaskDependency(
	ctx context.Context,
	taskID string,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		logging.Info(ctx, "add task dependency of %v on %v", taskState.ID, taskID)

		taskState.State = state
		taskState.Dependencies.Add(taskID)
		return taskState
	})
}

func (c *executionContext) IsHanging() bool {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	inflightTimeout := max(
		c.taskState.EstimatedInflightDuration*time.Duration(c.missedEstimatesUntilTaskIsHanging),
		c.inflightHangingTaskTimeout,
	)

	stallingTimeout := max(
		c.taskState.EstimatedStallingDuration*time.Duration(c.missedEstimatesUntilTaskIsHanging),
		c.stallingHangingTaskTimeout,
	)

	return time.Since(c.taskState.CreatedAt) > c.hangingTaskTimeout ||
		c.taskState.InflightDuration > inflightTimeout ||
		c.taskState.StallingDuration > stallingTimeout
}

func (c *executionContext) SetEstimatedInflightDuration(estimatedInflightDuration time.Duration) {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	if c.taskState.EstimatedInflightDuration == 0 {
		c.taskState.EstimatedInflightDuration = estimatedInflightDuration
	}
}

func (c *executionContext) SetEstimatedStallingDuration(estimatedStallingDuration time.Duration) {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	if c.taskState.EstimatedStallingDuration == 0 {
		c.taskState.EstimatedStallingDuration = estimatedStallingDuration
	}
}

func (c *executionContext) HasEvent(ctx context.Context, event int64) bool {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	for _, elem := range c.taskState.Events {
		if event == elem {
			return true
		}
	}

	return false
}

func (c *executionContext) FinishWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	if c.finished {
		return nil
	}

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	err = c.updateStateWithPreparation(
		ctx,
		func(taskState storage.TaskState) storage.TaskState {
			taskState.State = state
			taskState.Status = storage.TaskStatusFinished
			return taskState
		},
		preparation,
	)
	if err != nil {
		return err
	}

	c.finished = true
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *executionContext) getRetriableErrorCount() uint64 {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()
	return c.taskState.RetriableErrorCount
}

func (c *executionContext) updateStateWithPreparation(
	ctx context.Context,
	transition func(storage.TaskState) storage.TaskState,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	taskState := transition(c.taskState)
	taskState = taskState.DeepCopy()

	now := time.Now()
	// Since executionContext exists only within the scope of lockAndExecuteTask,
	// the task is always inflight (is in running/cancelling status)
	// during any method call on executionContext.
	taskState.InflightDuration += now.Sub(taskState.ModifiedAt)
	taskState.ModifiedAt = now

	var newTaskState storage.TaskState
	var err error

	if preparation != nil {
		newTaskState, err = c.storage.UpdateTaskWithPreparation(ctx, taskState, preparation)
	} else {
		newTaskState, err = c.storage.UpdateTask(ctx, taskState)
	}
	if err != nil {
		return err
	}

	c.taskState = newTaskState
	return nil
}

func (c *executionContext) updateState(
	ctx context.Context,
	transition func(storage.TaskState) storage.TaskState,
) error {

	return c.updateStateWithPreparation(ctx, transition, nil /* preparation */)
}

func (c *executionContext) clearState(ctx context.Context) error {
	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		taskState.State = nil
		return taskState
	})
}

func (c *executionContext) incrementRetriableErrorCount(
	ctx context.Context,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		taskState.State = state
		taskState.RetriableErrorCount++
		return taskState
	})
}

func (c *executionContext) incrementPanicCount(
	ctx context.Context,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		taskState.State = state
		taskState.PanicCount++
		return taskState
	})
}

func (c *executionContext) setError(ctx context.Context, e error) error {
	state, err := c.task.Save()
	if err != nil {
		return err
	}

	err = c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		taskState.State = state
		taskState.Status = storage.TaskStatusReadyToCancel
		taskState.SetError(e)
		return taskState
	})
	if err != nil {
		logError(
			ctx,
			err,
			"failed to commit non retriable error for %v with task id %v",
			c.GetTaskType(),
			c.GetTaskID(),
		)
		return err
	}

	if !errors.IsSilent(e) {
		logError(
			ctx,
			e,
			"commited fatal error for %v with task id %v",
			c.GetTaskType(),
			c.GetTaskID(),
		)
	}
	return nil
}

func (c *executionContext) setNonCancellableError(
	ctx context.Context,
	e error,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		taskState.State = state
		taskState.Status = storage.TaskStatusCancelled
		taskState.SetError(e)
		return taskState
	})
}

func (c *executionContext) finish(ctx context.Context) error {
	return c.FinishWithPreparation(ctx, nil /* preparation */)
}

func (c *executionContext) setCancelled(ctx context.Context) error {
	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		taskState.State = state
		taskState.Status = storage.TaskStatusCancelled
		return taskState
	})
}

func (c *executionContext) ping(ctx context.Context) error {
	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		return taskState
	})
}

////////////////////////////////////////////////////////////////////////////////

func newExecutionContext(
	task Task,
	storage storage.Storage,
	taskState storage.TaskState,
	hangingTaskTimeout time.Duration,
	inflightHangingTaskTimeout time.Duration,
	stallingHangingTaskTimeout time.Duration,
	missedEstimatesUntilTaskIsHanging uint64,
) *executionContext {

	return &executionContext{
		task:      task,
		storage:   storage,
		taskState: taskState,

		hangingTaskTimeout:                hangingTaskTimeout,
		inflightHangingTaskTimeout:        inflightHangingTaskTimeout,
		stallingHangingTaskTimeout:        stallingHangingTaskTimeout,
		missedEstimatesUntilTaskIsHanging: missedEstimatesUntilTaskIsHanging,
	}
}

////////////////////////////////////////////////////////////////////////////////

func isCancelledError(err error) bool {
	switch {
	case
		errors.Is(err, context.Canceled),
		persistence.IsTransportError(err, grpc_codes.Canceled):
		return true
	default:
		return false
	}
}

func logError(
	ctx context.Context,
	err error,
	format string,
	args ...interface{},
) {

	description := fmt.Sprintf(format, args...)

	if errors.Is(err, errors.NewWrongGenerationError()) ||
		errors.Is(err, errors.NewInterruptExecutionError()) ||
		isCancelledError(err) {

		logging.Debug(logging.AddCallerSkip(ctx, 1), "%v: %v", description, err)
	} else {
		logging.Warn(logging.AddCallerSkip(ctx, 1), "%v: %v", description, err)
	}
}
