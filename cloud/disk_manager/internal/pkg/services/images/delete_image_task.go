package images

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deleteImageTask struct {
	config      *config.ImagesConfig
	scheduler   tasks.Scheduler
	storage     resources.Storage
	poolService pools.Service
	request     *protos.DeleteImageRequest
	state       *protos.DeleteImageTaskState
}

func (t *deleteImageTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteImageTask) Load(request, state []byte) error {
	t.request = &protos.DeleteImageRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteImageTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteImageTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteImage(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteImageTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteImage(ctx, execCtx)
}

func (t *deleteImageTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeleteImageMetadata{
		ImageId: t.request.ImageId,
	}, nil
}

func (t *deleteImageTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *deleteImageTask) deleteImage(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return deleteImage(
		ctx,
		execCtx,
		t.config,
		t.scheduler,
		t.storage,
		t.poolService,
		t.request.ImageId,
	)
}
