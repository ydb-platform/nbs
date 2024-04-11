package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type imageDeletingTask struct {
	storage storage.Storage
	request *protos.ImageDeletingRequest
	state   *protos.ImageDeletingTaskState
}

func (t *imageDeletingTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *imageDeletingTask) Load(request, state []byte) error {
	t.request = &protos.ImageDeletingRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ImageDeletingTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *imageDeletingTask) imageDeleting(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.ImageDeleting(ctx, t.request.ImageId)
}

func (t *imageDeletingTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.imageDeleting(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *imageDeletingTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.imageDeleting(ctx, execCtx)
}

func (t *imageDeletingTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *imageDeletingTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
