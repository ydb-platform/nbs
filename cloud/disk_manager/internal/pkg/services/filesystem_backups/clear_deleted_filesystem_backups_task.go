package snapshots

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearDeletedFilesystemBackupsTask struct {
	storage           resources.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearDeletedFilesystemBackupsTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearDeletedFilesystemBackupsTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearDeletedFilesystemBackupsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	deletedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearDeletedFilesystemBackups(ctx, deletedBefore, t.limit)
}

func (t *clearDeletedFilesystemBackupsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearDeletedFilesystemBackupsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearDeletedFilesystemBackupsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
