package filesystem_snapshot

import (
	"context"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type deleteFilesystemSnapshotTask struct {
	scheduler tasks.Scheduler
}

func (t *deleteFilesystemSnapshotTask) Save() ([]byte, error) {
	return []byte{}, nil
}

func (t *deleteFilesystemSnapshotTask) Load(request, state []byte) error {
	return nil
}

func (t *deleteFilesystemSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteFilesystemSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteFilesystemSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.DeleteFilesystemSnapshotMetadata{}

	return metadata, nil
}

func (t *deleteFilesystemSnapshotTask) GetResponse() proto.Message {
	return &disk_manager.DeleteFilesystemSnapshotResponse{}
}
