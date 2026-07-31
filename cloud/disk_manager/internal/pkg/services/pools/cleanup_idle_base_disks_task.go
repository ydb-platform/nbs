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

type cleanupIdleBaseDisksTask struct {
	storage         storage.Storage
	baseDiskIdleTTL time.Duration
	limit           uint64
}

func (t *cleanupIdleBaseDisksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *cleanupIdleBaseDisksTask) Load(_, _ []byte) error {
	return nil
}

func (t *cleanupIdleBaseDisksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.baseDiskIdleTTL <= 0 {
		return nil
	}

	idleBefore := time.Now().Add(-t.baseDiskIdleTTL)

	idleDisks, err := t.storage.GetIdleBaseDisks(ctx, t.baseDiskIdleTTL, t.limit)
	if err != nil {
		return err
	}

	if len(idleDisks) == 0 {
		return nil
	}

	var baseDiskIDs []string
	for _, d := range idleDisks {
		baseDiskIDs = append(baseDiskIDs, d.ID)
	}

	return t.storage.EjectIdleBaseDisksFromPool(ctx, baseDiskIDs, idleBefore)
}

func (t *cleanupIdleBaseDisksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *cleanupIdleBaseDisksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *cleanupIdleBaseDisksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
