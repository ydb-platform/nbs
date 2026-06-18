package placementgroup

import (
	"context"
	"slices"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type alterPlacementGroupMembershipTask struct {
	storage      resources.Storage
	nbsFactory   nbs.Factory
	cellSelector cells.CellSelector
	request      *protos.AlterPlacementGroupMembershipRequest
	state        *protos.AlterPlacementGroupMembershipTaskState
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

	pgMeta, err := t.storage.GetPlacementGroupMeta(ctx, t.request.GroupId)
	if err != nil {
		return err
	}

	zoneID := t.request.ZoneId
	if pgMeta != nil && len(pgMeta.ZoneID) > 0 {
		zoneCells, err := t.cellSelector.ResolveCells(t.request.ZoneId)
		if err != nil {
			return err
		}
		if !slices.Contains(zoneCells, pgMeta.ZoneID) {
			return errors.NewNonRetriableErrorf(
				"placement group %v is in zone %v, not in requested zone %v",
				t.request.GroupId,
				pgMeta.ZoneID,
				t.request.ZoneId,
			)
		}
		zoneID = pgMeta.ZoneID
	}

	client, err := t.nbsFactory.GetClient(ctx, zoneID)
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
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *alterPlacementGroupMembershipTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
