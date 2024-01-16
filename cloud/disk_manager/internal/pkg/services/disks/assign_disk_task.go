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

type assignDiskTask struct {
	nbsFactory nbs.Factory
	request    *protos.AssignDiskRequest
	state      *protos.AssignDiskTaskState
}

func (t *assignDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *assignDiskTask) Load(request, state []byte) error {
	t.request = &protos.AssignDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.AssignDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *assignDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return err
	}

	return client.Assign(ctx, nbs.AssignDiskParams{
		ID:         t.request.Disk.DiskId,
		InstanceID: t.request.InstanceId,
		Host:       t.request.Host,
		Token:      t.request.Token,
	})
}

func (t *assignDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// TODO: should it be cancellable?
	// TODO: should we do unassign?
	return nil
}

func (t *assignDiskTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *assignDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
