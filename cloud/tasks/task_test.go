package tasks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
)

////////////////////////////////////////////////////////////////////////////////

type TaskMock struct {
	mock.Mock
}

func (t *TaskMock) Save() ([]byte, error) {
	args := t.Called()
	res, _ := args.Get(0).([]byte)
	return res, args.Error(1)
}

func (t *TaskMock) Load(request, state []byte) error {
	args := t.Called(request, state)
	return args.Error(0)
}

func (t *TaskMock) Run(ctx context.Context, execCtx ExecutionContext) error {
	args := t.Called(ctx, execCtx)
	return args.Error(0)
}

func (t *TaskMock) Cancel(ctx context.Context, execCtx ExecutionContext) error {
	args := t.Called(ctx, execCtx)
	return args.Error(0)
}

func (t *TaskMock) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	args := t.Called(ctx)
	res, _ := args.Get(0).(proto.Message)
	return res, args.Error(1)
}

func (t *TaskMock) GetResponse() proto.Message {
	args := t.Called()
	res, _ := args.Get(0).(proto.Message)
	return res
}

////////////////////////////////////////////////////////////////////////////////

func NewTaskMock() *TaskMock {
	return &TaskMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that TaskMock implements Task.
func assertTaskMockIsTask(arg *TaskMock) Task {
	return arg
}
