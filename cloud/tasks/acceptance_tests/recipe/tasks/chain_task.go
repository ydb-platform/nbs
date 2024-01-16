package tasks

import (
	"context"
	"math/rand"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/acceptance_tests/recipe/tasks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type ChainTask struct {
	scheduler tasks.Scheduler
	request   *protos.ChainTaskRequest
	state     *protos.ChainTaskState
}

func (t *ChainTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *ChainTask) Load(request, state []byte) error {
	t.request = &protos.ChainTaskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ChainTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *ChainTask) doSomeWork(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t.state.Counter++

	err := execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	rand.Seed(time.Now().UnixNano())
	<-time.After(common.RandomDuration(time.Millisecond, 5*time.Second))

	if rand.Intn(2) == 1 {
		return errors.NewInterruptExecutionError()
	}

	// TODO: should limit overall count of retriable errors.
	if rand.Intn(20) == 1 {
		return errors.NewRetriableErrorf("retriable error")
	}

	return nil
}

func (t *ChainTask) scheduleChild(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (string, error) {

	if t.request.Depth == 0 {
		return "", nil
	}

	return t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()),
		"ChainTask",
		"Chain task",
		&protos.ChainTaskRequest{
			Depth: t.request.Depth - 1,
		},
		"",
		"",
	)
}

func (t *ChainTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.doSomeWork(ctx, execCtx)
	if err != nil {
		return err
	}

	childTaskID, err := t.scheduleChild(ctx, execCtx)
	if err != nil {
		return err
	}

	if len(childTaskID) == 0 {
		return nil
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, childTaskID)
	return err
}

func (t *ChainTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.doSomeWork(ctx, execCtx)
	if err != nil {
		return err
	}

	var childTaskID string
	childTaskID, err = t.scheduleChild(ctx, execCtx)
	if err != nil {
		return err
	}

	if len(childTaskID) == 0 {
		return nil
	}

	_, err = t.scheduler.CancelTask(ctx, childTaskID)
	return err
}

func (t *ChainTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *ChainTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func NewChainTask(scheduler tasks.Scheduler) *ChainTask {
	return &ChainTask{
		scheduler: scheduler,
	}
}
