package placementgroup

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type deletePlacementGroupTask struct {
	storage       resources.Storage
	nbsFactory    nbs.Factory
	cellSelector  cells.CellSelector
	disabledZones *common.DisabledZones
	request       *protos.DeletePlacementGroupRequest
	state         *protos.DeletePlacementGroupTaskState
}

func (t *deletePlacementGroupTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deletePlacementGroupTask) Load(request, state []byte) error {
	t.request = &protos.DeletePlacementGroupRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeletePlacementGroupTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deletePlacementGroupTask) deletePlacementGroup(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	pgMeta, err := t.storage.GetPlacementGroupMeta(
		ctx,
		t.request.GroupId,
	)
	if err != nil {
		return err
	}

	zoneID := t.request.ZoneId
	if pgMeta != nil && len(pgMeta.ZoneID) > 0 {
		zoneID = pgMeta.ZoneID
	}

	// Disabled cells may already be absent from the cell mapping,
	// so we cannot rely on it to validate that the cell stored in metadata
	// belongs to the requested zone.
	if t.disabledZones.IsDisabled(zoneID) {
		return t.deletePlacementGroupInDisabledZone(ctx, execCtx, zoneID)
	}

	return t.deletePlacementGroupInEnabledZone(ctx, execCtx, pgMeta)
}

func (t *deletePlacementGroupTask) deletePlacementGroupInDisabledZone(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	zoneID string,
) error {

	err := t.markPlacementGroupDeleting(ctx, execCtx)
	if err != nil {
		return err
	}

	logging.Warn(
		ctx,
		"Skipping NBS deletion of placement group %v: zone %v is disabled",
		t.request.GroupId,
		zoneID,
	)

	return t.storage.PlacementGroupDeleted(ctx, t.request.GroupId, time.Now())
}

func (t *deletePlacementGroupTask) deletePlacementGroupInEnabledZone(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	pgMeta *resources.PlacementGroupMeta,
) error {

	zoneID, err := resolvePlacementGroupZoneID(
		t.cellSelector,
		t.request.GroupId,
		t.request.ZoneId,
		pgMeta,
	)
	if err != nil {
		return err
	}

	client, err := t.nbsFactory.GetClient(ctx, zoneID)
	if err != nil {
		return err
	}

	err = t.markPlacementGroupDeleting(ctx, execCtx)
	if err != nil {
		return err
	}

	err = client.DeletePlacementGroup(ctx, t.request.GroupId)
	if err != nil {
		return err
	}

	return t.storage.PlacementGroupDeleted(ctx, t.request.GroupId, time.Now())
}

func (t *deletePlacementGroupTask) markPlacementGroupDeleting(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	placementGroup, err := t.storage.DeletePlacementGroup(
		ctx,
		t.request.GroupId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if placementGroup == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.GroupId,
		)
	}

	return nil
}

func (t *deletePlacementGroupTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deletePlacementGroup(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deletePlacementGroupTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deletePlacementGroup(ctx, execCtx)
}

func (t *deletePlacementGroupTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeletePlacementGroupMetadata{
		GroupId: &disk_manager.GroupId{
			ZoneId:  t.request.ZoneId,
			GroupId: t.request.GroupId,
		},
	}, nil
}

func (t *deletePlacementGroupTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
