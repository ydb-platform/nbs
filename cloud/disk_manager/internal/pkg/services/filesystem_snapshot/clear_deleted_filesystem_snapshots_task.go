package filesystem_snapshot

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearDeletedFilesystemSnapshotsTask struct {
	storage           resources.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearDeletedFilesystemSnapshotsTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearDeletedFilesystemSnapshotsTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearDeletedFilesystemSnapshotsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	deletedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearDeletedFilesystemSnapshots(ctx, deletedBefore, t.limit)
}

func (t *clearDeletedFilesystemSnapshotsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearDeletedFilesystemSnapshotsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearDeletedFilesystemSnapshotsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
