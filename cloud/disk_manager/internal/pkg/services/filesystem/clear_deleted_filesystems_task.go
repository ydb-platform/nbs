package filesystem

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearDeletedFilesystemsTask struct {
	storage           resources.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearDeletedFilesystemsTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearDeletedFilesystemsTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearDeletedFilesystemsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	deletedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearDeletedFilesystems(ctx, deletedBefore, t.limit)
}

func (t *clearDeletedFilesystemsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearDeletedFilesystemsTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearDeletedFilesystemsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
