package filesystem

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type resizeFilesystemTask struct {
	factory nfs.Factory
	request *protos.ResizeFilesystemRequest
	state   *protos.ResizeFilesystemTaskState
}

func (t *resizeFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *resizeFilesystemTask) Load(request, state []byte) error {
	t.request = &protos.ResizeFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ResizeFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *resizeFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.factory.NewClient(ctx, t.request.Filesystem.ZoneId)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Resize(ctx, t.request.Filesystem.FilesystemId, t.request.Size)
}

func (t *resizeFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *resizeFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *resizeFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
