package disks

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

const legacyDiskCleanupPrefix = "legacy_disk_cleanup_"

func (t *deleteDiskTask) deleteDiskWithoutMetadata(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {
	if !t.request.DeleteIfNonExistent {
		return nil
	}
	diskID := t.request.Disk.DiskId
	zoneID := t.request.Disk.ZoneId
	if len(zoneID) == 0 {
		return t.storage.DiskDeleted(ctx, diskID, time.Now())
	}

	selfTaskID := execCtx.GetTaskID()
	cleanupID := legacyDiskCleanupPrefix + selfTaskID
	prepared, err := t.storage.PrepareLegacyDiskCleanup(ctx, diskID, cleanupID)
	if err != nil {
		return err
	}
	if !prepared {
		return nil
	}

	err = t.deleteDiskInCell(ctx, execCtx, zoneID, selfTaskID+"_delete_disk_from_incremental")
	if err != nil {
		return err
	}
	return execCtx.FinishWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		return t.storage.CompleteDiskCleanup(ctx, tx, diskID, cleanupID)
	})
}
