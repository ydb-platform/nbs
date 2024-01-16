package disks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearDeletedDisksTask struct {
	storage           resources.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearDeletedDisksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearDeletedDisksTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearDeletedDisksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	deletedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearDeletedDisks(ctx, deletedBefore, t.limit)
}

func (t *clearDeletedDisksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearDeletedDisksTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearDeletedDisksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
