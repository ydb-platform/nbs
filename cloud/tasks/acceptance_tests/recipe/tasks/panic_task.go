package tasks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
)

////////////////////////////////////////////////////////////////////////////////

type PanicTask struct{}

func (t *PanicTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *PanicTask) Load(request, state []byte) error {
	return nil
}

func (t *PanicTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	<-time.After(common.RandomDuration(time.Millisecond, 5*time.Second))
	panic("test panic")
}

func (t *PanicTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *PanicTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *PanicTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func NewPanicTask() *PanicTask {
	return &PanicTask{}
}
