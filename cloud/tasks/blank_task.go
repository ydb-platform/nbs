package tasks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type blankTask struct {
}

func (t *blankTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *blankTask) Load(_, _ []byte) error {
	return nil
}

func (t *blankTask) Run(ctx context.Context, execCtx ExecutionContext) error {
	logging.Info(ctx, "running with id %v", execCtx.GetTaskID())
	return nil
}

func (t *blankTask) Cancel(
	ctx context.Context,
	execCtx ExecutionContext,
) error {

	logging.Info(ctx, "cancelling with id %v", execCtx.GetTaskID())
	return nil
}

func (t *blankTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *blankTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
