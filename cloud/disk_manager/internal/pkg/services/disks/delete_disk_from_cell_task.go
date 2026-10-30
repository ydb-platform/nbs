package disks

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// Each instance retries cleanup in one cell; the parent waits for every cell.
type deleteDiskFromCellTask struct {
	deleteDiskTask
	cleanupID string
}

func (t *deleteDiskFromCellTask) Load(request, state []byte) error {
	var req protos.DeleteDiskFromCellRequest
	err := proto.Unmarshal(request, &req)
	if err != nil {
		return err
	}
	t.request = &protos.DeleteDiskRequest{Disk: req.Disk, Sync: req.Sync}
	t.cleanupID = req.CleanupId
	t.state = &protos.DeleteDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteDiskFromCellTask) cleanup(ctx context.Context, execCtx tasks.ExecutionContext) error {
	pending, err := t.storage.IsDiskCleanupPending(ctx, t.request.Disk.DiskId, t.cleanupID)
	if err != nil {
		return err
	}
	if !pending {
		// A retry after successful cleanup must not delete a newly created disk.
		return nil
	}
	err = t.deleteDiskInCell(ctx, execCtx, t.request.Disk.ZoneId, t.cleanupID+"_delete_disk_from_incremental")
	if err != nil {
		return err
	}
	// Release the ID reservation and finish this task in the same transaction.
	return execCtx.FinishWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		return t.storage.CompleteDiskCleanup(ctx, tx, t.request.Disk.DiskId, t.cleanupID)
	})
}

func (t *deleteDiskFromCellTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	err := t.cleanup(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}
	return nil
}

func (t *deleteDiskFromCellTask) Cancel(ctx context.Context, execCtx tasks.ExecutionContext) error {
	return t.cleanup(ctx, execCtx)
}

func (t *deleteDiskFromCellTask) GetMetadata(context.Context) (proto.Message, error) {
	return &empty.Empty{}, nil
}
