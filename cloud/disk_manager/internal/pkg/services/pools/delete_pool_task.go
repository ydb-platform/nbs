package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type deletePoolTask struct {
	storage storage.Storage
	request *protos.DeletePoolRequest
	state   *protos.DeletePoolTaskState
}

func (t *deletePoolTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deletePoolTask) Load(request, state []byte) error {
	t.request = &protos.DeletePoolRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeletePoolTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deletePoolTask) deletePool(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.DeletePool(ctx, t.request.ImageId, t.request.ZoneId)
}

func (t *deletePoolTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deletePool(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deletePoolTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deletePool(ctx, execCtx)
}

func (t *deletePoolTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deletePoolTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
