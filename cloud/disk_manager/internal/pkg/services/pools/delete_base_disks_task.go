package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type deleteBaseDisksTask struct {
	storage    storage.Storage
	nbsFactory nbs.Factory
	limit      int
}

func (t *deleteBaseDisksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *deleteBaseDisksTask) Load(_, _ []byte) error {
	return nil
}

func (t *deleteBaseDisksTask) deleteBaseDisk(
	ctx context.Context,
	baseDisk storage.BaseDisk,
) error {

	client, err := t.nbsFactory.GetClient(ctx, baseDisk.ZoneID)
	if err != nil {
		return err
	}

	return client.Delete(ctx, baseDisk.ID)
}

func (t *deleteBaseDisksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	baseDisks, err := t.storage.GetBaseDisksToDelete(ctx, uint64(t.limit))
	if err != nil {
		return err
	}

	for _, disk := range baseDisks {
		err := t.deleteBaseDisk(ctx, disk)
		if err != nil {
			return err
		}
	}

	return t.storage.BaseDisksDeleted(ctx, baseDisks)
}

func (t *deleteBaseDisksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteBaseDisksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteBaseDisksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
