package placementgroup

import (
	"context"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type alterPlacementGroupMembershipTask struct {
	storage       resources.Storage
	nbsFactory    nbs.Factory
	cellSelector  cells.CellSelector
	disabledZones *common.DisabledZones
	request       *protos.AlterPlacementGroupMembershipRequest
	state         *protos.AlterPlacementGroupMembershipTaskState
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

	meta, err := t.storage.GetPlacementGroupMeta(
		ctx,
		t.request.GroupId,
	)
	if err != nil {
		return err
	}

	if zoneDisabled, err := t.handleRunDisabledZone(meta); zoneDisabled {
		return err
	}

	client, err := selectClientByGroup(
		ctx,
		t.storage,
		t.nbsFactory,
		t.cellSelector,
		t.request.GroupId,
		t.request.ZoneId,
		meta,
	)
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

func (t *alterPlacementGroupMembershipTask) handleRunDisabledZone(
	meta *resources.PlacementGroupMeta,
) (zoneDisabled bool, err error) {

	zoneID := t.request.ZoneId

	if meta != nil && len(meta.ZoneID) > 0 {
		zoneID = meta.ZoneID
	}

	// Disabled cells may already be absent from the cell mapping.
	if !t.disabledZones.IsDisabled(zoneID) {
		return false, nil
	}

	// Addition needs to result in error.
	if len(t.request.DisksToAdd) > 0 {
		return true, common.NewNonRetriableAbortedError(
			"can't alter placement group membership in disabled zone %q",
			zoneID,
		)
	}

	// Removal needs no DM metadata changes and can skip the unavailable NBS.
	return true, nil
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
