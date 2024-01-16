package placementgroup

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type alterPlacementGroupMembershipTask struct {
	nbsFactory nbs.Factory
	request    *protos.AlterPlacementGroupMembershipRequest
	state      *protos.AlterPlacementGroupMembershipTaskState
}

func (t *alterPlacementGroupMembershipTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *alterPlacementGroupMembershipTask) Load(request, state []byte) error {
	t.request = &protos.AlterPlacementGroupMembershipRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.AlterPlacementGroupMembershipTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *alterPlacementGroupMembershipTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.ZoneId)
	if err != nil {
		return err
	}

	return client.AlterPlacementGroupMembership(
		ctx,
		func() error {
			// Confirm that current generation is not obsolete.
			return execCtx.SaveState(ctx)
		},
		t.request.GroupId,
		t.request.PlacementPartitionIndex,
		t.request.DisksToAdd,
		t.request.DisksToRemove,
	)
}

func (t *alterPlacementGroupMembershipTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// TODO: should it be cancellable?
	return nil
}

func (t *alterPlacementGroupMembershipTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *alterPlacementGroupMembershipTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
