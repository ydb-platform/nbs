package placementgroup

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deletePlacementGroupTask struct {
	storage    resources.Storage
	nbsFactory nbs.Factory
	request    *protos.DeletePlacementGroupRequest
	state      *protos.DeletePlacementGroupTaskState
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

	client, err := t.nbsFactory.GetClient(ctx, t.request.ZoneId)
	if err != nil {
		return err
	}

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

	err = client.DeletePlacementGroup(ctx, t.request.GroupId)
	if err != nil {
		return err
	}

	return t.storage.PlacementGroupDeleted(ctx, t.request.GroupId, time.Now())
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
	taskID string,
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
