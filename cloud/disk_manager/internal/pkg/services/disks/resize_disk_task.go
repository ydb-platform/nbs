package disks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	internal_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type resizeDiskTask struct {
	nbsFactory nbs.Factory
	request    *protos.ResizeDiskRequest
	state      *protos.ResizeDiskTaskState
}

func (t *resizeDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *resizeDiskTask) Load(request, state []byte) error {
	t.request = &protos.ResizeDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ResizeDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *resizeDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return err
	}

	params, err := client.Describe(ctx, t.request.Disk.DiskId)
	if err != nil {
		return err
	}

	if internal_common.IsLocalDiskKind(params.Kind) {
		return errors.NewNonCancellableErrorf(
			"cannot resize local disk %v",
			t.request.Disk.DiskId,
		)
	}

	return client.Resize(
		ctx,
		func() error {
			// Confirm that current generation is not obsolete (NBS-1292).
			return execCtx.SaveState(ctx)
		},
		t.request.Disk.DiskId,
		t.request.Size,
	)
}

func (t *resizeDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// TODO: should it be cancellable?
	return nil
}

func (t *resizeDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *resizeDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
