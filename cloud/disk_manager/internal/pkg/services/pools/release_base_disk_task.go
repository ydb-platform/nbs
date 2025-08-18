package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type releaseBaseDiskTask struct {
	storage storage.Storage
	request *protos.ReleaseBaseDiskRequest
	state   *protos.ReleaseBaseDiskTaskState
}

func (t *releaseBaseDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *releaseBaseDiskTask) Load(request, state []byte) error {
	t.request = &protos.ReleaseBaseDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ReleaseBaseDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *releaseBaseDiskTask) releaseBaseDisk(ctx context.Context) error {
	overlayDisk := t.request.OverlayDisk

	baseDisk, err := t.storage.ReleaseBaseDiskSlot(ctx, overlayDisk)
	if err == nil {
		logging.Info(
			ctx,
			"overlayDisk %v released slot on baseDisk %v",
			overlayDisk,
			baseDisk,
		)
	}

	return err
}

func (t *releaseBaseDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.releaseBaseDisk(ctx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *releaseBaseDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.releaseBaseDisk(ctx)
}

func (t *releaseBaseDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *releaseBaseDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
