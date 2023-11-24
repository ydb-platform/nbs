package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type executionContext struct {
	task           Task
	storage        storage.Storage
	taskState      storage.TaskState
	taskStateMutex sync.Mutex
}

// HACK from https://github.com/stretchr/testify/pull/694/files to avoid fake race detection
func (c *executionContext) String() string {
	return fmt.Sprintf("%[1]T<%[1]p>", c)
}

func (c *executionContext) SaveState(ctx context.Context) error {
	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		logging.Debug(
			ctx,
			"saving state %v",
			taskState.ID,
		)

		taskState.State = state
		return taskState
	})
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
		logging.Debug(
			ctx,
			"add task dependency of %v on %v",
			taskState.ID,
			taskID,
		)

		taskState.State = state
		taskState.Dependencies.Add(taskID)
		return taskState
	})
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

////////////////////////////////////////////////////////////////////////////////

func (c *executionContext) getRetriableErrorCount() uint64 {
	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()
	return c.taskState.RetriableErrorCount
}

func (c *executionContext) updateState(
	ctx context.Context,
	f func(storage.TaskState) storage.TaskState,
) error {

	c.taskStateMutex.Lock()
	defer c.taskStateMutex.Unlock()

	taskState := f(c.taskState)
	taskState = taskState.DeepCopy()

	taskState.ModifiedAt = time.Now()

	newTaskState, err := c.storage.UpdateTask(ctx, taskState)
	if err != nil {
		return err
	}

	c.taskState = newTaskState
	return nil
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
		errors.LogError(
			ctx,
			err,
			"failed to commit non retriable error for %v with task id %v",
			c.GetTaskType(),
			c.GetTaskID(),
		)
		return err
	}

	if !errors.IsSilent(e) {
		errors.LogError(
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

func (c *executionContext) setFinished(ctx context.Context) error {
	state, err := c.task.Save()
	if err != nil {
		return err
	}

	return c.updateState(ctx, func(taskState storage.TaskState) storage.TaskState {
		taskState.State = state
		taskState.Status = storage.TaskStatusFinished
		return taskState
	})
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
) *executionContext {

	return &executionContext{
		task:      task,
		storage:   storage,
		taskState: taskState,
	}
}
