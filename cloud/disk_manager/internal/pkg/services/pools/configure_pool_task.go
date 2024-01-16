package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type configurePoolTask struct {
	storage         storage.Storage
	resourceStorage resources.Storage
	request         *protos.ConfigurePoolRequest
	state           *protos.ConfigurePoolTaskState
}

func (t *configurePoolTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *configurePoolTask) Load(request, state []byte) error {
	t.request = &protos.ConfigurePoolRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ConfigurePoolTaskState{}
	return proto.Unmarshal(state, t.state)
}

// Returns image size.
func (t *configurePoolTask) checkImage(ctx context.Context) (uint64, error) {
	image, err := t.resourceStorage.GetImageMeta(ctx, t.request.ImageId)
	if err != nil {
		return 0, err
	}

	if image == nil {
		return 0, errors.NewSilentNonRetriableErrorf(
			"image with id %v is not found",
			t.request.ImageId,
		)
	}

	if !image.Ready {
		return 0, errors.NewSilentNonRetriableErrorf(
			"image with id %v is not ready",
			t.request.ImageId,
		)
	}

	return image.Size, nil
}

func (t *configurePoolTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	imageSize, err := t.checkImage(ctx)
	if err != nil {
		return err
	}

	if !t.request.UseImageSize {
		imageSize = 0
	}

	return t.storage.ConfigurePool(
		ctx,
		t.request.ImageId,
		t.request.ZoneId,
		t.request.Capacity,
		imageSize,
	)
}

func (t *configurePoolTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *configurePoolTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *configurePoolTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
