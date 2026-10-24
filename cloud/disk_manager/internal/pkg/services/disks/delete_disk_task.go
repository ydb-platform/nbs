package disks

import (
	"context"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const legacyDiskCleanupPrefix = "legacy_disk_cleanup_"

type deleteDiskTask struct {
	performanceConfig *performance_config.PerformanceConfig
	storage           resources.Storage
	scheduler         tasks.Scheduler
	poolService       pools.Service
	nbsFactory        nbs.Factory
	cellSelector      cells.CellSelector
	request           *protos.DeleteDiskRequest
	state             *protos.DeleteDiskTaskState
}

func (t *deleteDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteDiskTask) Load(request, state []byte) error {
	t.request = &protos.DeleteDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteDiskTask) deleteDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.CleanupCellIds) != 0 {
		return t.scheduleDiskCleanup(ctx, execCtx)
	}

	selfTaskID := execCtx.GetTaskID()
	diskID := t.request.Disk.DiskId

	backgroundCleanup := t.request.BackgroundCleanup &&
		t.request.DeleteIfNonExistent && t.request.Disk.ZoneId != ""
	deleteDisk := t.storage.DeleteDisk
	if backgroundCleanup {
		deleteDisk = t.storage.DeleteDiskIfExists
	}
	diskMeta, err := deleteDisk(
		ctx,
		diskID,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	zoneID := ""
	if diskMeta == nil {
		// The record may be missing, already deleted, or reserved for cell cleanup.
		if !t.request.DeleteIfNonExistent {
			return nil
		}
		if backgroundCleanup {
			completed, err := t.storage.IsDiskDeletionCompleted(ctx, diskID)
			if err != nil {
				return err
			}
			if completed {
				return nil
			}
		}

		zoneID = t.request.Disk.ZoneId
		if len(zoneID) == 0 {
			// Zone was not specified in the request.
			return t.storage.DiskDeleted(ctx, diskID, time.Now())
		}
	} else {
		zoneID = diskMeta.ZoneID
	}

	if diskMeta == nil && backgroundCleanup {
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

	legacyCleanupID := ""
	if diskMeta == nil {
		legacyCleanupID = legacyDiskCleanupPrefix + selfTaskID
		prepared, err := t.storage.PrepareLegacyDiskCleanup(ctx, diskID, legacyCleanupID)
		if err != nil {
			return err
		}
		if !prepared {
			return nil
		}
	}

	if diskMeta != nil {
		err = t.setEstimate(ctx, execCtx)
		if err != nil {
			return err
		}
	}
	err = t.deleteDiskInCell(ctx, execCtx, zoneID, selfTaskID+"_delete_disk_from_incremental")
	if err != nil {
		return err
	}

	if legacyCleanupID != "" {
		return execCtx.FinishWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
			return t.storage.CompleteDiskCleanup(ctx, tx, diskID, legacyCleanupID)
		})
	}

	// Only overlay disks (created from image) should be released.
	if diskMeta != nil && len(diskMeta.SrcImageID) != 0 {
		taskID, err := t.poolService.ReleaseBaseDisk(
			headers.SetIncomingIdempotencyKey(
				ctx,
				selfTaskID+"_release_base_disk",
			),
			&pools_protos.ReleaseBaseDiskRequest{
				OverlayDisk: &types.Disk{
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
	}

	return t.storage.DiskDeleted(ctx, diskID, time.Now())
}

func (t *deleteDiskTask) scheduleDiskCleanup(ctx context.Context, execCtx tasks.ExecutionContext) error {
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
		_, err = t.scheduler.ScheduleNonCancellableTask(
			headers.SetIncomingIdempotencyKey(ctx, id+"_finish"), "disks.FinishLegacyDiskCleanup", "", "",
			&protos.FinishLegacyDiskCleanupRequest{DiskId: t.request.Disk.DiskId, LegacyTaskId: legacyTaskID, CleanupId: id},
		)
		if err != nil {
			return err
		}
	}
	for i, cellID := range t.state.CleanupCellIds {
		_, err := t.scheduler.ScheduleNonCancellableTask(
			headers.SetIncomingIdempotencyKey(ctx, cleanupIDs[i]),
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

func (t *deleteDiskTask) deleteDiskInCell(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	zoneID string,
	idempotencyKey string,
) error {

	diskID := t.request.Disk.DiskId

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			idempotencyKey,
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
		err = client.DeleteSync(ctx, diskID)
	} else {
		err = client.Delete(ctx, diskID)
	}
	if err != nil && !nbs.IsDiskNotFoundError(err) {
		return err
	}

	return nil
}

func (t *deleteDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteDisk(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteDisk(ctx, execCtx)
}

func (t *deleteDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeleteDiskMetadata{
		DiskId: &disk_manager.DiskId{
			ZoneId: t.request.Disk.ZoneId,
			DiskId: t.request.Disk.DiskId,
		},
	}, nil
}

func (t *deleteDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *deleteDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Only sync requests are blocking.
	if !t.request.Sync {
		return nil
	}

	diskID := t.request.Disk.DiskId

	diskMeta, err := t.storage.GetDiskMeta(ctx, diskID)
	if err != nil {
		return err
	}

	if diskMeta == nil || len(diskMeta.ZoneID) == 0 {
		// Should be idempotent.
		return nil
	}

	client, err := t.nbsFactory.GetClient(ctx, diskMeta.ZoneID)
	if err != nil {
		return err
	}

	diskParams, err := client.Describe(ctx, diskID)
	if err != nil {
		if nbs.IsDiskNotFoundError(err) {
			// Should be idempotent.
			return nil
		}

		return err
	}

	diskSize := diskParams.BlocksCount * uint64(diskParams.BlockSize)

	switch diskParams.Kind {
	case types.DiskKind_DISK_KIND_SSD_LOCAL:
		execCtx.SetEstimatedInflightDuration(performance.Estimate(
			diskSize,
			t.performanceConfig.GetSSDLocalDiskDeletingBandwidthMiBs(),
		))

	case types.DiskKind_DISK_KIND_HDD_LOCAL:
		execCtx.SetEstimatedInflightDuration(performance.Estimate(
			diskSize,
			t.performanceConfig.GetHDDLocalDiskDeletingBandwidthMiBs(),
		))
	}

	return nil
}
