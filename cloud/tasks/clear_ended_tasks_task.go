package tasks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type clearEndedTasksTask struct {
	storage           storage.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearEndedTasksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearEndedTasksTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearEndedTasksTask) Run(
	ctx context.Context,
	execCtx ExecutionContext,
) error {

	endedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearEndedTasks(ctx, endedBefore, t.limit)
}

func (t *clearEndedTasksTask) Cancel(
	ctx context.Context,
	execCtx ExecutionContext,
) error {

	return nil
}

func (t *clearEndedTasksTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearEndedTasksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
