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

// Returns image meta. Returns nil if image is deleted or is being deleted;
// in that case pool config (if it has been written by this or by a previous
// attempt of this task) is deleted.
//
// Together with the order of operations in images service (image is marked as
// deleting *before* pools.ImageDeleting is scheduled) this closes the race
// between image deletion and pool configuration: pools.ImageDeleting can only
// delete pools that are already written, and a pool written after it has run
// is always followed by a re-check that observes the "deleting" mark.
func (t *configurePoolTask) checkImage(
	ctx context.Context,
) (*resources.ImageMeta, error) {

	image, err := t.resourceStorage.GetImageMeta(ctx, t.request.ImageId)
	if err != nil {
		return nil, err
	}

	if image != nil && len(image.DeleteTaskID) == 0 {
		return image, nil
	}

	err = t.storage.DeletePool(ctx, t.request.ImageId, t.request.ZoneId)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (t *configurePoolTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	image, err := t.checkImage(ctx)
	if err != nil {
		return err
	}

	if image == nil {
		return errors.NewSilentNonRetriableErrorf(
			"image with id %v is not found or is being deleted",
			t.request.ImageId,
		)
	}

	if !image.Ready {
		return errors.NewSilentNonRetriableErrorf(
			"image with id %v is not ready",
			t.request.ImageId,
		)
	}

	var imageSize uint64
	if t.request.UseImageSize {
		imageSize = image.Size
	}

	err = t.storage.ConfigurePool(
		ctx,
		t.request.ImageId,
		t.request.ZoneId,
		t.request.Capacity,
		imageSize,
	)
	if err != nil {
		return err
	}

	// Image could have been deleted concurrently, between the check above and
	// the config write. Roll the config back in that case (see |checkImage|).
	image, err = t.checkImage(ctx)
	if err != nil {
		return err
	}

	if image == nil {
		return errors.NewSilentNonRetriableErrorf(
			"image with id %v has been deleted during pool configuration",
			t.request.ImageId,
		)
	}

	return nil
}

func (t *configurePoolTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Config may have been written by an interrupted Run.
	_, err := t.checkImage(ctx)
	return err
}

func (t *configurePoolTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {
	return &empty.Empty{}, nil
}

func (t *configurePoolTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
