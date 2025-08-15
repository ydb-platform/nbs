package disks

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	disks_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const checkStatusPeriod = 200 * time.Millisecond

////////////////////////////////////////////////////////////////////////////////

type migrateDiskTask struct {
	disksConfig       *disks_config.DisksConfig
	performanceConfig *performance_config.PerformanceConfig
	scheduler         tasks.Scheduler
	poolService       pools.Service
	resourceStorage   resources.Storage
	poolStorage       storage.Storage
	nbsFactory        nbs.Factory
	request           *protos.MigrateDiskRequest
	state             *protos.MigrateDiskTaskState
}

func (t *migrateDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *migrateDiskTask) Load(request, state []byte) error {
	t.request = &protos.MigrateDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.MigrateDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *migrateDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for {

		t.logDebug(
			ctx,
			execCtx,
			"status is %v, executing",
			MigrationStatusToString(t.state.Status),
		)

		switch t.state.Status {
		case protos.MigrationStatus_Unspecified:
			err := t.start(ctx, execCtx)
			if err != nil {
				return err
			}

		case protos.MigrationStatus_Replicating:
			_, err := t.scheduler.WaitTask(ctx, execCtx, t.state.ReplicateTaskID)
			if err != nil {
				return err
			}

			t.state.Status = protos.MigrationStatus_ReplicationFinished
			err = execCtx.SaveState(ctx)
			if err != nil {
				return err
			}

		case protos.MigrationStatus_ReplicationFinished:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(checkStatusPeriod):
				t.logInfo(ctx, execCtx, "waiting for finishing status")

				if execCtx.HasEvent(
					ctx,
					int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION),
				) {
					t.state.Status = protos.MigrationStatus_Finishing
					err := execCtx.SaveState(ctx)
					if err != nil {
						// Can't fail while finishing migration - otherwise the system will
						// end up in a non-consistent state (NBS-4677).
						return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
					}
				}
			}

		case protos.MigrationStatus_Finishing:
			err := t.finishMigration(ctx, execCtx)
			if err != nil {
				// Can't fail while finishing migration - otherwise the system will end
				// up in a non-consistent state (NBS-4677).
				return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
			}

			return nil
		}
	}
}

func (t *migrateDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t.logInfo(ctx, execCtx, "cancelling task")

	// We do not expect task cancelling after FINISH_MIGRATION signal, but we still
	// avoid dst disk deletion.
	if !execCtx.HasEvent(
		ctx,
		int64(protos.MigrateDiskTaskEvents_FINISH_MIGRATION),
	) {
		err := t.deleteDstDisk(ctx, execCtx)
		if err != nil {
			return err
		}
		t.logInfo(ctx, execCtx, "deleted dst disk")
	}

	return nil
}

func (t *migrateDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	status, err := t.getStatusForAPI(ctx)
	if err != nil {
		return nil, err
	}

	metadata := &disk_manager.MigrateDiskMetadata{
		Status:           status,
		Progress:         0,
		SecondsRemaining: math.MaxInt64,
	}

	if len(t.state.ReplicateTaskID) > 0 {
		message, err := t.scheduler.GetTaskMetadata(ctx, t.state.ReplicateTaskID)
		if err != nil {
			return nil, err
		}

		replicateDiskTaskMetadata, ok := message.(*dataplane_protos.ReplicateDiskTaskMetadata)
		if !ok {
			return nil, errors.NewNonRetriableErrorf(
				"invalid dataplane.ReplicateDisk metadata type %T",
				message,
			)
		}

		metadata.Progress = replicateDiskTaskMetadata.Progress
		metadata.SecondsRemaining = replicateDiskTaskMetadata.SecondsRemaining
		metadata.UpdatedAt = replicateDiskTaskMetadata.UpdatedAt
	}

	return metadata, nil
}

func (t *migrateDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *migrateDiskTask) start(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t.logInfo(ctx, execCtx, "starting")

	err := t.ensureNonLocalDisk(ctx)
	if err != nil {
		return err
	}

	err = t.setEstimate(ctx, execCtx)
	if err != nil {
		return err
	}

	if t.state.FillGeneration == 0 {
		fillGeneration, err := t.incrementFillGeneration(ctx, execCtx)
		if err != nil {
			return err
		}

		t.state.FillGeneration = fillGeneration
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	if t.state.RelocateInfo == nil {
		err := execCtx.SaveStateWithPreparation(
			ctx,
			func(ctx context.Context, tx *persistence.Transaction) error {
				relocateInfo, err := t.poolStorage.RelocateOverlayDiskTx(
					ctx,
					tx,
					t.request.Disk,
					t.request.DstZoneId,
				)
				if err != nil {
					return err
				}

				t.state.RelocateInfo = &protos.RelocateInfo{
					BaseDiskID:       relocateInfo.BaseDiskID,
					TargetBaseDiskID: relocateInfo.TargetBaseDiskID,
					SlotGeneration:   relocateInfo.SlotGeneration,
				}
				return nil
			},
		)
		if err != nil {
			return err
		}

	} else if len(t.state.RelocateInfo.TargetBaseDiskID) != 0 {
		// Need to check that RelocateInfo is still actual.
		// OverlayDiskRebasing should be idempotent.
		err := t.poolStorage.OverlayDiskRebasing(ctx, storage.RebaseInfo{
			OverlayDisk:      t.request.Disk,
			BaseDiskID:       t.state.RelocateInfo.BaseDiskID,
			TargetZoneID:     t.request.DstZoneId,
			TargetBaseDiskID: t.state.RelocateInfo.TargetBaseDiskID,
			SlotGeneration:   t.state.RelocateInfo.SlotGeneration,
		})
		if err != nil {
			return err
		}
	}

	if !t.state.IsDiskCloned {
		multiZoneClient, err := t.nbsFactory.GetMultiZoneClient(
			t.request.Disk.ZoneId,
			t.request.DstZoneId,
		)
		if err != nil {
			return err
		}

		err = multiZoneClient.Clone(
			ctx,
			t.request.Disk.DiskId,
			t.request.DstPlacementGroupId,
			t.request.DstPlacementPartitionIndex,
			t.state.FillGeneration,
			t.state.RelocateInfo.TargetBaseDiskID,
		)
		if err != nil {
			return err
		}

		t.state.IsDiskCloned = true
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	if len(t.state.ReplicateTaskID) == 0 {
		err := t.scheduleReplicateTask(ctx, execCtx)
		if err != nil {
			return err
		}
	}

	t.logInfo(ctx, execCtx, "started")
	return nil
}

func (t *migrateDiskTask) ensureNonLocalDisk(ctx context.Context) error {
	client, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return err
	}

	params, err := client.Describe(ctx, t.request.Disk.DiskId)
	if err != nil {
		return err
	}

	if common.IsLocalDiskKind(params.Kind) {
		return errors.NewNonCancellableErrorf(
			"cannot migrate local disk %v",
			t.request.Disk.DiskId,
		)
	}

	return nil
}

func (t *migrateDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	client, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return err
	}

	stats, err := client.Stat(
		ctx,
		t.request.Disk.DiskId,
	)
	if err != nil {
		return err
	}

	execCtx.SetEstimate(performance.Estimate(
		stats.StorageSize,
		t.performanceConfig.GetReplicateDiskBandwidthMiBs(),
	))

	return nil
}

func (t *migrateDiskTask) scheduleReplicateTask(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	replicateTaskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			execCtx.GetTaskID()+"_replicate_disk",
		),
		"dataplane.ReplicateDisk",
		"",
		t.request.Disk.ZoneId,
		&dataplane_protos.ReplicateDiskTaskRequest{
			SrcDisk: t.request.Disk,
			DstDisk: &types.Disk{
				DiskId: t.request.Disk.DiskId,
				ZoneId: t.request.DstZoneId,
			},
			FillGeneration: t.state.FillGeneration,
			// Performs full copy of base disk if |IgnoreBaseDisk == false|.
			IgnoreBaseDisk: len(t.state.RelocateInfo.TargetBaseDiskID) != 0,
		},
	)
	if err != nil {
		return err
	}

	t.state.ReplicateTaskID = replicateTaskID
	t.state.Status = protos.MigrationStatus_Replicating
	return execCtx.SaveState(ctx)
}

func (t *migrateDiskTask) deleteDstDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t.logInfo(ctx, execCtx, "delete dst disk")

	dstZoneClient, err := t.nbsFactory.GetClient(ctx, t.request.DstZoneId)
	if err != nil {
		return err
	}

	if t.state.FillGeneration > 0 {
		err := dstZoneClient.DeleteWithFillGeneration(
			ctx,
			t.request.Disk.DiskId,
			t.state.FillGeneration,
		)
		if err != nil {
			return err
		}
	}

	t.logInfo(ctx, execCtx, "dst disk has been deleted")

	return nil
}

func (t *migrateDiskTask) incrementFillGeneration(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (uint64, error) {

	t.logInfo(ctx, execCtx, "incrementing fillGeneration")

	fillGeneration, err := t.resourceStorage.IncrementFillGeneration(
		ctx,
		t.request.Disk.DiskId,
	)
	if err != nil {
		return 0, err
	}

	t.logInfo(
		ctx,
		execCtx,
		"now fillGeneration is %v",
		fillGeneration,
	)

	return fillGeneration, nil
}

func (t *migrateDiskTask) finishFillDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t.logInfo(ctx, execCtx, "set IsFillFinished flag")

	client, err := t.nbsFactory.GetClient(ctx, t.request.DstZoneId)
	if err != nil {
		return err
	}

	err = client.FinishFillDisk(
		ctx,
		func() error {
			return execCtx.SaveState(ctx)
		},
		t.request.Disk.DiskId,
		t.state.FillGeneration,
	)
	if err != nil {
		return err
	}

	t.logInfo(ctx, execCtx, "IsFillFinished flag is set")
	return nil
}

func (t *migrateDiskTask) finishMigration(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	t.logInfo(ctx, execCtx, "finishing migration")

	err := t.finishFillDisk(ctx, execCtx)
	if err != nil {
		return err
	}

	if len(t.state.RelocateInfo.TargetBaseDiskID) != 0 {
		rebaseInfo := storage.RebaseInfo{
			OverlayDisk:      t.request.Disk,
			BaseDiskID:       t.state.RelocateInfo.BaseDiskID,
			TargetZoneID:     t.request.DstZoneId,
			TargetBaseDiskID: t.state.RelocateInfo.TargetBaseDiskID,
			SlotGeneration:   t.state.RelocateInfo.SlotGeneration,
		}

		err = execCtx.SaveStateWithPreparation(
			ctx,
			func(ctx context.Context, tx *persistence.Transaction) (err error) {
				err = t.poolStorage.OverlayDiskRebasedTx(
					ctx,
					tx,
					rebaseInfo,
				)
				if err != nil {
					return err
				}

				t.state.RelocateInfo.TargetBaseDiskID = ""
				return nil
			},
		)
		if err != nil {
			return err
		}
		logging.Info(ctx, "Overlay disk rebased for RebaseInfo %+v", rebaseInfo)
	}

	client, err := t.nbsFactory.GetClient(ctx, t.request.Disk.ZoneId)
	if err != nil {
		return err
	}

	err = client.Delete(ctx, t.request.Disk.DiskId)
	if err != nil {
		return err
	}
	t.logInfo(ctx, execCtx, "deleted src disk")

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			execCtx.GetTaskID()+"_delete_disk_from_incremental",
		),
		"dataplane.DeleteDiskFromIncremental",
		"",
		&dataplane_protos.DeleteDiskFromIncrementalRequest{
			Disk: t.request.Disk,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return execCtx.FinishWithPreparation(
		ctx,
		func(ctx context.Context, tx *persistence.Transaction) error {
			return t.resourceStorage.DiskRelocated(
				ctx,
				tx,
				t.request.Disk.DiskId,
				t.request.Disk.ZoneId,
				t.request.DstZoneId,
			)
		},
	)
}

func (t *migrateDiskTask) getStatusForAPI(ctx context.Context) (
	disk_manager.MigrateDiskMetadata_Status,
	error,
) {

	var apiStatus disk_manager.MigrateDiskMetadata_Status
	switch t.state.Status {
	case protos.MigrationStatus_Replicating:
		apiStatus = disk_manager.MigrateDiskMetadata_REPLICATING
	case protos.MigrationStatus_FinishingReplication:
		apiStatus = disk_manager.MigrateDiskMetadata_FINISHING_REPLICATION
	case protos.MigrationStatus_ReplicationFinished:
		apiStatus = disk_manager.MigrateDiskMetadata_REPLICATION_FINISHED
	case protos.MigrationStatus_Finishing:
		apiStatus = disk_manager.MigrateDiskMetadata_FINISHING
	case protos.MigrationStatus_Unspecified:
		fallthrough
	default:
		apiStatus = disk_manager.MigrateDiskMetadata_STATUS_UNSPECIFIED
	}

	return apiStatus, nil
}

func MigrationStatusToString(status protos.MigrationStatus) string {
	switch status {
	case protos.MigrationStatus_Unspecified:
		return "unspecified"
	case protos.MigrationStatus_Replicating:
		return "replicating"
	case protos.MigrationStatus_FinishingReplication:
		return "finishing_replication"
	case protos.MigrationStatus_ReplicationFinished:
		return "replication_finished"
	case protos.MigrationStatus_Finishing:
		return "finishing"
	}

	return fmt.Sprintf("unknown_%v", status)
}

////////////////////////////////////////////////////////////////////////////////

func (t *migrateDiskTask) logInfo(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	format string,
	args ...interface{},
) {

	format += t.getAdditionalLogInfo(execCtx)
	logging.Info(ctx, format, args...)
}

func (t *migrateDiskTask) logDebug(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	format string,
	args ...interface{},
) {

	format += t.getAdditionalLogInfo(execCtx)
	logging.Debug(ctx, format, args...)
}

func (t *migrateDiskTask) logError(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	format string,
	args ...interface{},
) {

	format += t.getAdditionalLogInfo(execCtx)
	logging.Error(ctx, format, args...)
}

func (t *migrateDiskTask) getAdditionalLogInfo(
	execCtx tasks.ExecutionContext,
) string {

	return fmt.Sprintf(". TaskID: %v", execCtx.GetTaskID())
}
