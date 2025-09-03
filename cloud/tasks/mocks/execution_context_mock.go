package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type ExecutionContextMock struct {
	mock.Mock
}

func (c *ExecutionContextMock) SaveState(ctx context.Context) error {
	args := c.Called(ctx)
	return args.Error(0)
}

func (c *ExecutionContextMock) SaveStateWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	args := c.Called(ctx, preparation)
	return args.Error(0)
}

func (c *ExecutionContextMock) GetTaskType() string {
	args := c.Called()
	return args.String(0)
}

func (c *ExecutionContextMock) GetTaskID() string {
	args := c.Called()
	return args.String(0)
}

func (c *ExecutionContextMock) AddTaskDependency(
	ctx context.Context,
	taskID string,
) error {

	args := c.Called(ctx, taskID)
	return args.Error(0)
}

func (c *ExecutionContextMock) IsHanging() bool {
	args := c.Called()
	return args.Bool(0)
}

func (c *ExecutionContextMock) SetEstimatedInflightDuration(estimatedInflightDuration time.Duration) {
	c.Called(estimatedInflightDuration)
}

func (c *ExecutionContextMock) HasEvent(ctx context.Context, event int64) bool {
	args := c.Called(ctx, event)
	return args.Bool(0)
}

func (c *ExecutionContextMock) FinishWithPreparation(
	ctx context.Context,
	preparation func(context.Context, *persistence.Transaction) error,
) error {

	args := c.Called(ctx, preparation)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewExecutionContextMock() *ExecutionContextMock {
	return &ExecutionContextMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ExecutionContextMock implements ExecutionContext.
func assertExecutionContextMockIsExecutionContext(
	arg *ExecutionContextMock,
) tasks.ExecutionContext {

	return arg
}
