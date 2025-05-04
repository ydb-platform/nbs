package filesystem

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deleteExternalFilesystemTask struct {
	storage resources.Storage
	factory nfs.Factory
	request *protos.DeleteFilesystemRequest
	state   *protos.DeleteFilesystemTaskState
}

func (t *deleteExternalFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteExternalFilesystemTask) Load(request, state []byte) error {
	t.request = &protos.DeleteFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteExternalFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return errors.NewInterruptExecutionError()
}

func (t *deleteExternalFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteExternalFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeleteFilesystemMetadata{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       t.request.Filesystem.ZoneId,
			FilesystemId: t.request.Filesystem.FilesystemId,
		},
	}, nil
}

func (t *deleteExternalFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
