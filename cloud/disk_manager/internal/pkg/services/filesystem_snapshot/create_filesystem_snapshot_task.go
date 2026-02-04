package filesystem_snapshot

import (
	"context"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type createFilesystemSnapshotTask struct {
	scheduler    tasks.Scheduler
	cellSelector cells.CellSelector
}

func (t *createFilesystemSnapshotTask) Save() ([]byte, error) {
	return []byte{}, nil
}

func (t *createFilesystemSnapshotTask) Load(request, state []byte) error {
	return nil
}

func (t *createFilesystemSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *createFilesystemSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *createFilesystemSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.CreateFilesystemSnapshotMetadata{}

	return metadata, nil
}

func (t *createFilesystemSnapshotTask) GetResponse() proto.Message {
	return &disk_manager.CreateFilesystemSnapshotResponse{}
}
