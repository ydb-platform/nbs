package disks

import (
	"context"
	"slices"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
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
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type deleteDiskTask struct {
	performanceConfig *performance_config.PerformanceConfig
	storage           resources.Storage
	scheduler         tasks.Scheduler
	poolService       pools.Service
	nbsFactory        nbs.Factory
	cellSelector      cells.CellSelector
	disabledZones     *common.DisabledZones
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

	err := t.setEstimate(ctx, execCtx)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()
	diskID := t.request.Disk.DiskId
	sync := t.request.Sync

	diskMeta, err := t.storage.DeleteDisk(
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
		// DeleteDisk returns nil for a missing row and for a disk that is
		// already deleted but still present in meta (trash retention window).
		if !t.request.DeleteIfNonExistent {
			return nil
		}

		zoneID, err = t.resolveCleanupZoneID(ctx, diskID)
		if err != nil {
			return err
		}
		if len(zoneID) == 0 {
			logging.Error(
				ctx,
				"Cannot delete disk %v from NBS: zone is unknown both in meta and in the request",
				diskID,
			)
			return t.storage.DiskDeleted(ctx, diskID, time.Now())
		}
	} else {
		zoneID = diskMeta.ZoneID
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			selfTaskID+"_delete_disk_from_incremental",
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

	if t.disabledZones.IsDisabled(zoneID) {
		logging.Warn(
			ctx,
			"Skipping NBS deletion of disk %v: zone %v is disabled",
			diskID,
			zoneID,
		)
	} else {
		client, err := t.nbsFactory.GetClient(ctx, zoneID)
		if err != nil {
			return err
		}

		if sync {
			err = client.DeleteSync(ctx, diskID)
		} else {
			err = client.Delete(ctx, diskID)
		}
		if err != nil {
			return err
		}
	}

	// Only overlay disks (created from image) should be released.
	if diskMeta != nil && len(diskMeta.SrcImageID) != 0 {
		taskID, err = t.poolService.ReleaseBaseDisk(
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

// Resolves the zone for a disk that DeleteDisk did not return metadata for.
// Prefers the zone from meta (the disk may still be in the trash retention
// window) and falls back to the request zone. If meta has a zone, the request
// zone must be non-empty (otherwise a non-retriable error is returned) and
// must match the zone from meta or contain it as one of its cells (otherwise
// a retriable error is returned). The zone is not persisted and is resolved
// again on every retry.
func (t *deleteDiskTask) resolveCleanupZoneID(
	ctx context.Context,
	diskID string,
) (string, error) {

	meta, err := t.storage.GetDiskMeta(ctx, diskID)
	if err != nil {
		return "", err
	}

	requestZoneID := t.request.Disk.ZoneId
	if meta == nil || len(meta.ZoneID) == 0 {
		return requestZoneID, nil
	}

	if len(requestZoneID) == 0 {
		return "", errors.NewNonRetriableErrorf(
			"request zone is empty for disk %v with zone %q in meta",
			diskID,
			meta.ZoneID,
		)
	}

	if requestZoneID == meta.ZoneID {
		return meta.ZoneID, nil
	}

	requestCells, err := t.cellSelector.ResolveCells(requestZoneID)
	if err != nil {
		return "", err
	}

	if !slices.Contains(requestCells, meta.ZoneID) {
		return "", errors.NewRetriableErrorf(
			"zone %q of disk %v from meta does not match request zone %q (cells %v)",
			meta.ZoneID,
			diskID,
			requestZoneID,
			requestCells,
		)
	}

	return meta.ZoneID, nil
}

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

	if t.disabledZones.IsDisabled(diskMeta.ZoneID) {
		logging.Warn(
			ctx,
			"Skipping deletion time estimate for disk %v: zone %v is disabled",
			diskID,
			diskMeta.ZoneID,
		)
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
