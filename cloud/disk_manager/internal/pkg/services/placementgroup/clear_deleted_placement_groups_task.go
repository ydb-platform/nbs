package placementgroup

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearDeletedPlacementGroupsTask struct {
	storage           resources.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearDeletedPlacementGroupsTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearDeletedPlacementGroupsTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearDeletedPlacementGroupsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	deletedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearDeletedPlacementGroups(
		ctx,
		deletedBefore,
		t.limit,
	)
}

func (t *clearDeletedPlacementGroupsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearDeletedPlacementGroupsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearDeletedPlacementGroupsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
