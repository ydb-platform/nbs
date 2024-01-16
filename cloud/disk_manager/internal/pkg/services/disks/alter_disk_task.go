package disks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type alterDiskTask struct {
	nbsFactory nbs.Factory
	request    *protos.AlterDiskRequest
	state      *protos.AlterDiskTaskState
}

func (t *alterDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *alterDiskTask) Load(request, state []byte) error {
	t.request = &protos.AlterDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.AlterDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *alterDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return err
	}

	return client.Alter(
		ctx,
		func() error {
			// Confirm that current generation is not obsolete (NBS-1292).
			return execCtx.SaveState(ctx)
		},
		t.request.Disk.DiskId,
		t.request.CloudId,
		t.request.FolderId,
	)
}

func (t *alterDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// TODO: should it be cancellable?
	return nil
}

func (t *alterDiskTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *alterDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
