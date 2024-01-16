package pools

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearDeletedBaseDisksTask struct {
	storage           storage.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearDeletedBaseDisksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearDeletedBaseDisksTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearDeletedBaseDisksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	deletedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearDeletedBaseDisks(ctx, deletedBefore, t.limit)
}

func (t *clearDeletedBaseDisksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearDeletedBaseDisksTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearDeletedBaseDisksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
