package disks

import (
	"context"
	"strings"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const legacyDiskCleanupPrefix = "legacy_disk_cleanup_"

func (t *deleteDiskTask) deleteDiskWithoutMetadata(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	backgroundCleanup bool,
) error {

	// The record may be missing, already deleted, or reserved for cell cleanup.
	if !t.request.DeleteIfNonExistent {
		return nil
	}

	selfTaskID := execCtx.GetTaskID()
	diskID := t.request.Disk.DiskId
	if backgroundCleanup {
		completed, err := t.storage.IsDiskDeletionCompleted(ctx, diskID)
		if err != nil {
			return err
		}
		if completed {
			return nil
		}
	}

	zoneID := t.request.Disk.ZoneId
	if len(zoneID) == 0 {
		// Zone was not specified in the request.
		return t.storage.DiskDeleted(ctx, diskID, time.Now())
	}

	if backgroundCleanup {
		cellIDs, err := t.cellSelector.ResolveCells(zoneID)
		if err != nil {
			return err
		}
		if len(cellIDs) == 0 {
			return errors.NewNonRetriableErrorf("no cells for zone %q", zoneID)
		}
		t.state.CleanupCellIds = cellIDs
		// Persist the target set before reserving the ID or scheduling any worker.
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
		return t.scheduleDiskCleanup(ctx, execCtx)
	}

	// Keep tracking legacy calls while background cleanup is disabled, including
	// requests saved before the preparatory release.
	legacyCleanupID := legacyDiskCleanupPrefix + selfTaskID
	prepared, err := t.storage.PrepareLegacyDiskCleanup(ctx, diskID, legacyCleanupID)
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
		return t.storage.CompleteDiskCleanup(ctx, tx, diskID, legacyCleanupID)
	})
}

func (t *deleteDiskTask) scheduleDiskCleanup(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := t.registerLegacyCleanup(ctx, execCtx); err != nil {
		return err
	}
	cleanupIDs := make([]string, 0, len(t.state.CleanupCellIds))
	for _, cellID := range t.state.CleanupCellIds {
		cleanupIDs = append(cleanupIDs, execCtx.GetTaskID()+"_delete_disk_from_cell_"+cellID)
	}
	if !t.state.CleanupPrepared {
		prepared, err := t.storage.PrepareDiskCleanup(ctx, t.request.Disk.DiskId, cleanupIDs)
		if err != nil {
			return err
		}
		if !prepared {
			return nil
		}
		t.state.CleanupPrepared = true
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}
	reservedIDs, err := t.storage.GetDiskCleanupIDs(ctx, t.request.Disk.DiskId)
	if err != nil {
		return err
	}
	for _, id := range reservedIDs {
		if !strings.HasPrefix(id, legacyDiskCleanupPrefix) {
			continue
		}
		legacyTaskID := strings.TrimPrefix(id, legacyDiskCleanupPrefix)
		if legacyTaskID == "" {
			return errors.NewNonRetriableErrorf("empty legacy task ID in disk cleanup reservation")
		}
		err = scheduleLegacyDiskCleanupObserver(ctx, t.scheduler, t.request.Disk.DiskId, legacyTaskID, id)
		if err != nil {
			return err
		}
	}
	for i, cellID := range t.state.CleanupCellIds {
		_, err := t.scheduler.ScheduleNonCancellableTask(
			diskCleanupTaskContext(ctx, cleanupIDs[i]),
			"disks.DeleteDiskFromCell", "", "",
			&protos.DeleteDiskFromCellRequest{
				Disk: &types.Disk{ZoneId: cellID, DiskId: t.request.Disk.DiskId},
				Sync: t.request.Sync, CleanupId: cleanupIDs[i],
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}
