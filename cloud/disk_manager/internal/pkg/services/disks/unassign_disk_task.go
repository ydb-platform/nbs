package disks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type unassignDiskTask struct {
	nbsFactory nbs.Factory
	request    *protos.UnassignDiskRequest
	state      *protos.UnassignDiskTaskState
}

func (t *unassignDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *unassignDiskTask) Load(request, state []byte) error {
	t.request = &protos.UnassignDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.UnassignDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *unassignDiskTask) unassign(
	ctx context.Context,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return err
	}

	return client.Unassign(ctx, t.request.Disk.DiskId)
}

func (t *unassignDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.unassign(ctx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *unassignDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.unassign(ctx)
}

func (t *unassignDiskTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *unassignDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
