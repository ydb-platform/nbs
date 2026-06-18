package placementgroup

import (
	"context"
	"time"

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

type createPlacementGroupTask struct {
	storage      resources.Storage
	nbsFactory   nbs.Factory
	cellSelector cells.CellSelector
	request      *protos.CreatePlacementGroupRequest
	state        *protos.CreatePlacementGroupTaskState
}

func (t *createPlacementGroupTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createPlacementGroupTask) Load(request, state []byte) error {
	t.request = &protos.CreatePlacementGroupRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreatePlacementGroupTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createPlacementGroupTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	var client nbs.Client

	// On retry, check if PG was already persisted so we use its stored cell
	// instead of running cell selection that may fail with changed config.
	existingMeta, err := t.storage.GetPlacementGroupMeta(
		ctx,
		t.request.GroupId,
	)
	if err != nil {
		return err
	}

	if existingMeta != nil {
		client, err = t.nbsFactory.GetClient(ctx, existingMeta.ZoneID)
		if err != nil {
			return err
		}
	} else if len(t.state.GetSelectedCellId()) > 0 {
		client, err = t.nbsFactory.GetClient(ctx, t.state.GetSelectedCellId())
		if err != nil {
			return err
		}
	} else {
		client, err = t.cellSelector.SelectCellForPlacementGroup(
			ctx,
			t.request.ZoneId,
		)

		if err == nil {
			t.state.SelectedCellId = client.ZoneID()
			err = execCtx.SaveState(ctx)
		}

		if err != nil {
			return err
		}
	}

	selfTaskID := execCtx.GetTaskID()

	placementGroupMeta, err := t.storage.CreatePlacementGroup(ctx, resources.PlacementGroupMeta{
		ID:                      t.request.GroupId,
		ZoneID:                  client.ZoneID(),
		PlacementStrategy:       t.request.PlacementStrategy,
		PlacementPartitionCount: t.request.PlacementPartitionCount,

		CreateRequest: t.request,
		CreateTaskID:  selfTaskID,
		CreatingAt:    time.Now(),
		CreatedBy:     "", // TODO: Extract CreatedBy from execCtx
	})
	if err != nil {
		return err
	}

	if placementGroupMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.GroupId,
		)
	}

	err = client.CreatePlacementGroup(
		ctx,
		t.request.GroupId,
		t.request.PlacementStrategy,
		t.request.PlacementPartitionCount,
	)
	if err != nil {
		return err
	}

	placementGroupMeta.CreatedAt = time.Now()
	return t.storage.PlacementGroupCreated(ctx, *placementGroupMeta)
}

func (t *createPlacementGroupTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	placementGroupMeta, err := t.storage.DeletePlacementGroup(
		ctx,
		t.request.GroupId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if placementGroupMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.GroupId,
		)
	}

	zoneID := t.request.ZoneId
	if len(placementGroupMeta.ZoneID) > 0 {
		zoneID = placementGroupMeta.ZoneID
	}

	client, err := t.nbsFactory.GetClient(ctx, zoneID)
	if err != nil {
		return err
	}

	err = client.DeletePlacementGroup(ctx, t.request.GroupId)
	if err != nil {
		return err
	}

	return t.storage.PlacementGroupDeleted(ctx, t.request.GroupId, time.Now())
}

func (t *createPlacementGroupTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *createPlacementGroupTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
