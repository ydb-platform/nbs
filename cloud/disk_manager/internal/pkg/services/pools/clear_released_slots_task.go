package pools

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type clearReleasedSlotsTask struct {
	storage           storage.Storage
	expirationTimeout time.Duration
	limit             int
}

func (t *clearReleasedSlotsTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *clearReleasedSlotsTask) Load(_, _ []byte) error {
	return nil
}

func (t *clearReleasedSlotsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	releasedBefore := time.Now().Add(-t.expirationTimeout)
	return t.storage.ClearReleasedSlots(ctx, releasedBefore, t.limit)
}

func (t *clearReleasedSlotsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *clearReleasedSlotsTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *clearReleasedSlotsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
