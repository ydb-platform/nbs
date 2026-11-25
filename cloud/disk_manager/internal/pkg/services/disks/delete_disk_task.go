package disks

import (
	"context"
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

	selfTaskID := execCtx.GetTaskID()
	diskID := t.request.Disk.DiskId
	sync := t.request.Sync

	requestZoneID := t.request.Disk.ZoneId
	if requestZoneID == "" {
		return errors.NewNonCancellableErrorf("request zone is empty for disk %v", diskID)
	}

	meta, err := t.storage.GetDiskMeta(ctx, diskID)
	if err != nil {
		return err
	}

	var recordedZoneID string
	if meta != nil {
		recordedZoneID = meta.ZoneID
	}
	if recordedZoneID != "" && !t.disabledZones.IsDisabled(recordedZoneID) &&
		recordedZoneID != requestZoneID && !t.cellSelector.ZoneContainsCell(requestZoneID, recordedZoneID) {
		return errors.NewNonCancellableErrorf(
			"zone %q of disk %v from meta does not match request zone %q",
			recordedZoneID, diskID, requestZoneID,
		)
	}

	diskMeta, err := t.storage.DeleteDisk(
		ctx,
		diskID,
		selfTaskID,
		time.Now(),
		&recordedZoneID,
	)
	if err != nil {
		return err
	}
	if diskMeta == nil {
		return nil
	}
	zoneID := diskMeta.ZoneID
	if zoneID == "" {
		return errors.NewNonCancellableErrorf("disk %v has no zone in metadata", diskID)
	}
	disabled := t.disabledZones.IsDisabled(zoneID)
	if sync && !disabled {
		err = t.setEstimate(ctx, execCtx, diskID, zoneID)
		if err != nil {
			return err
		}
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

	if disabled {
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
	if len(diskMeta.SrcImageID) != 0 {
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

func (t *deleteDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	diskID string,
	zoneID string,
) error {

	client, err := t.nbsFactory.GetClient(ctx, zoneID)
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
