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

	SaveStateWithCallback(
		ctx context.Context,
		callback func(context.Context, *persistence.Transaction) error,
	) error

	GetTaskType() string

	GetTaskID() string

	// Dependencies are automatically added by Scheduler.WaitTask.
	AddTaskDependency(ctx context.Context, taskID string) error

	SetEstimate(estimatedDuration time.Duration)

	HasEvent(ctx context.Context, event int64) bool

	FinishWithCallback(
		ctx context.Context,
		callback func(context.Context, *persistence.Transaction) error,
	) error

	UpdateStateWithCallback(
		ctx context.Context,
		transition func(context.Context) error,
		callback func(context.Context, *persistence.Transaction) error,
	) error
}

////////////////////////////////////////////////////////////////////////////////

type executionContext struct {
	task           Task
	storage        storage.Storage
	taskState      storage.TaskState
	taskStateMutex sync.Mutex
	finished       bool
}

// HACK from https://github.com/stretchr/testify/pull/694/files to avoid fake race detection
func (c *executionContext) String() string {
	return fmt.Sprintf("%[1]T<%[1]p>", c)
}

func (c *executionContext) SaveState(ctx context.Context) error {
	return c.SaveStateWithCallback(ctx, nil /* callback */)
}

func (c *executionContext) SaveStateWithCallback(
	ctx context.Context,
	callback func(context.Context, *persistence.Transaction) error,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateStateWithCallback(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			logging.Info(ctx, "saving state for task %v", taskState.ID)

			taskState.State = state
			return taskState, nil
		},
		callback,
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

	return c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			logging.Info(ctx, "add task dependency of %v on %v", taskState.ID, taskID)

			taskState.State = state
			taskState.Dependencies.Add(taskID)
			return taskState, nil
		},
	)
}

func (c *executionContext) SetEstimate(estimatedDuration time.Duration) {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	if c.taskState.EstimatedTime.Before(c.taskState.CreatedAt) {
		c.taskState.EstimatedTime = c.taskState.CreatedAt.Add(estimatedDuration)
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

func (c *executionContext) FinishWithCallback(
	ctx context.Context,
	callback func(context.Context, *persistence.Transaction) error,
) error {

	if c.finished {
		return nil
	}

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	err = c.updateStateWithCallback(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			taskState.State = state
			taskState.Status = storage.TaskStatusFinished
			return taskState, nil
		},
		callback,
	)
	if err != nil {
		return err
	}

	c.finished = true
	return nil
}

func (c *executionContext) UpdateStateWithCallback(
	ctx context.Context,
	transition func(context.Context) error,
	callback func(context.Context, *persistence.Transaction) error,
) (err error) {

	return c.updateStateWithCallback(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			err = transition(ctx)
			if err != nil {
				return storage.TaskState{}, err
			}

			state, err := c.task.Save()
			if err != nil {
				logging.Warn(ctx, "Saving error, %v", err)
				return storage.TaskState{}, err
			}
			taskState.State = state
			return taskState, nil
		},
		callback,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (c *executionContext) getRetriableErrorCount() uint64 {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()
	return c.taskState.RetriableErrorCount
}

func (c *executionContext) updateStateWithCallback(
	ctx context.Context,
	transition func(storage.TaskState) (storage.TaskState, error),
	callback func(context.Context, *persistence.Transaction) error,
) error {

	logging.Debug(ctx, "USWC start")
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	taskState, err := transition(c.taskState)
	if err != nil {
		return err
	}

	logging.Debug(ctx, "USWC transition done")

	taskState = taskState.DeepCopy()
	taskState.ModifiedAt = time.Now()

	var newTaskState storage.TaskState

	if callback != nil {
		newTaskState, err = c.storage.UpdateTaskWithCallback(ctx, taskState, callback)
	} else {
		newTaskState, err = c.storage.UpdateTask(ctx, taskState)
	}
	logging.Debug(ctx, "USWC task updated")

	if err != nil {
		return err
	}

	c.taskState = newTaskState
	return nil
}

func (c *executionContext) updateState(
	ctx context.Context,
	transition func(storage.TaskState) (storage.TaskState, error),
) error {

	return c.updateStateWithCallback(ctx, transition, nil /* callback */)
}

func (c *executionContext) clearState(ctx context.Context) error {
	return c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			taskState.State = nil
			return taskState, nil
		},
	)
}

func (c *executionContext) incrementRetriableErrorCount(
	ctx context.Context,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			taskState.State = state
			taskState.RetriableErrorCount++
			return taskState, nil
		},
	)
}

func (c *executionContext) incrementPanicCount(
	ctx context.Context,
) error {

	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			taskState.State = state
			taskState.PanicCount++
			return taskState, nil
		},
	)
}

func (c *executionContext) setError(ctx context.Context, e error) error {
	state, err := c.task.Save()
	if err != nil {
		return err
	}

	err = c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			taskState.State = state
			taskState.Status = storage.TaskStatusReadyToCancel
			taskState.SetError(e)
			return taskState, nil
		},
	)
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

	return c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			taskState.State = state
			taskState.Status = storage.TaskStatusCancelled
			taskState.SetError(e)
			return taskState, nil
		},
	)
}

func (c *executionContext) finish(ctx context.Context) error {
	return c.FinishWithCallback(ctx, nil /* callback */)
}

func (c *executionContext) setCancelled(ctx context.Context) error {
	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			taskState.State = state
			taskState.Status = storage.TaskStatusCancelled
			return taskState, nil
		},
	)
}

func (c *executionContext) ping(ctx context.Context) error {
	return c.updateState(
		ctx,
		func(taskState storage.TaskState) (storage.TaskState, error) {
			return taskState, nil
		},
	)
}

////////////////////////////////////////////////////////////////////////////////

func newExecutionContext(
	task Task,
	storage storage.Storage,
	taskState storage.TaskState,
) *executionContext {

	return &executionContext{
		task:      task,
		storage:   storage,
		taskState: taskState,
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
