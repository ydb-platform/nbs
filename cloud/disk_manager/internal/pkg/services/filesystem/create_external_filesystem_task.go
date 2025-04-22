package filesystem

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type createExternalFilesystemTask struct {
	storage resources.Storage
	factory nfs.Factory
	request *protos.CreateFilesystemRequest
	state   *protos.CreateFilesystemTaskState
}

func (t *createExternalFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createExternalFilesystemTask) Load(request, state []byte) error {
	t.request = &protos.CreateFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createExternalFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return errors.NewRetriableErrorWithIgnoreRetryLimit(fmt.Errorf("Waiting for manual task completion"))
}

func (t *createExternalFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {
	return nil
}

func (t *createExternalFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *createExternalFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
