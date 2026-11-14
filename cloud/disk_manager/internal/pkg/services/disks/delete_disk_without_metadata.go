package disks

import (
	"context"

	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

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
		// Zone was not specified in the request: nothing to clean up, the
		// tombstone keeps the disk ID reserved.
		return nil
	}

	selfTaskID := execCtx.GetTaskID()
	cleanupID := legacyDiskCleanupPrefix + selfTaskID

	prepared, err := t.storage.PrepareDiskCleanup(
		ctx,
		diskID,
		selfTaskID,
		cleanupID,
	)
	if err != nil {
		return err
	}

	if !prepared {
		return nil
	}

	err = t.deleteDiskInCell(ctx, execCtx, zoneID)
	if err != nil {
		return err
	}

	return execCtx.FinishWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			return t.storage.CompleteDiskCleanup(ctx, tx, diskID, cleanupID)
		},
	)
}

func (t *deleteDiskTask) deleteDiskInCell(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	zoneID string,
) error {

	diskID := t.request.Disk.DiskId

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			execCtx.GetTaskID()+"_delete_disk_from_incremental",
		),
		"dataplane.DeleteDiskFromIncremental",
		"",
		&dataplane_protos.DeleteDiskFromIncrementalRequest{
			Disk: &types.Disk{
				ZoneId: zoneID,
				DiskId: diskID,
			},
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	client, err := t.nbsFactory.GetClient(ctx, zoneID)
	if err != nil {
		return err
	}

	if t.request.Sync {
		return client.DeleteSync(ctx, diskID)
	}

	return client.Delete(ctx, diskID)
}
