package disks

import (
	"context"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const TransferVolumeViaLinkTaskType = "disks.TransferVolumeViaLink"

const volumeLinkStatusPollPeriod = time.Second

// transferVolumeViaLinkTask transfers a standalone disk within one cell or between
// cells in the same public zone using an NBS volume link. NBS copies the data,
// transfers leadership, and deletes the source volume.
//
// The task is non-cancellable. Placement-group disks, overlay volumes with a
// BaseDiskId, local volumes, and direct-mirror group volumes are not supported.
// Disks created from an image are supported when they have no base disk.
type transferVolumeViaLinkTask struct {
	nbsFactory   nbs.Factory
	cellSelector cells.CellSelector
	storage      resources.Storage
	scheduler    tasks.Scheduler
	request      *protos.TransferVolumeViaLinkRequest
	state        *protos.TransferVolumeViaLinkTaskState
}

func (t *transferVolumeViaLinkTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *transferVolumeViaLinkTask) Load(request, state []byte) error {
	t.request = &protos.TransferVolumeViaLinkRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}
	t.state = &protos.TransferVolumeViaLinkTaskState{}
	return proto.Unmarshal(state, t.state)
}

func validateVolumeTransferRequest(request *protos.TransferVolumeViaLinkRequest) error {
	if request.GetLogicalDiskId() == "" {
		return errors.NewNonRetriableErrorf("logical disk ID must be specified")
	}
	return nil
}

func (t *transferVolumeViaLinkTask) volumeLink() nbs.VolumeLink {
	return nbs.VolumeLink{
		LeaderDiskID:   t.state.GetSrcDisk().GetDiskId(),
		LeaderCellID:   t.state.GetSrcDisk().GetZoneId(),
		FollowerDiskID: t.state.GetDstDisk().GetDiskId(),
		FollowerCellID: t.state.GetDstDisk().GetZoneId(),
	}
}

func (t *transferVolumeViaLinkTask) Run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if err := validateVolumeTransferRequest(t.request); err != nil {
		return errors.NewNonCancellableError(err)
	}
	if !execCtx.IsNonCancellable() {
		return errors.NewNonCancellableErrorf("%s must be scheduled as non-cancellable", TransferVolumeViaLinkTaskType)
	}
	if t.state.FailurePending {
		return t.cleanupAndFail(ctx, execCtx)
	}
	if t.state.ReservationReleased {
		if t.state.Status == string(nbs.VolumeLinkStateCompleted) {
			return nil
		}
		return errors.NewNonCancellableErrorf("disk reservation has already been released")
	}
	err := nbs.NormalizeVolumeLinkError(t.run(ctx, execCtx))
	if err == nil || ctx.Err() != nil {
		return err
	}
	if !isTerminalDiskOperationError(err) {
		return err
	}
	t.state.ErrorMessage = errors.ErrorForTracing(err)
	if t.state.ReservationAcquired && !t.state.RecoveryRequired &&
		t.state.Status != string(nbs.VolumeLinkStateCompleted) && !t.state.LinkCreationStarted && !t.state.LinkCreated {
		t.state.FailurePending = true
		if err := saveDiskOperationState(ctx, execCtx); err != nil {
			return err
		}
		return t.cleanupAndFail(ctx, execCtx)
	}
	if t.state.ReservationAcquired {
		return t.recoveryError(ctx, execCtx, "volume transfer", err)
	}
	return errors.NewNonCancellableError(err)
}

func (t *transferVolumeViaLinkTask) cleanupAndFail(ctx context.Context, execCtx tasks.ExecutionContext) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if t.state.ReservationReleased {
		return errors.NewNonCancellableErrorf("transfer failed before link creation: %s", t.state.ErrorMessage)
	}
	taskID := execCtx.GetTaskID()
	if !t.state.CleanupDone && t.state.SrcDisk != nil && t.state.DstDisk != nil {
		if err := t.nbsFactory.DeleteVolumeLinkTarget(ctx, t.volumeLink(), taskID); err != nil {
			return t.recoveryError(ctx, execCtx, "target cleanup", err)
		}
	}
	t.state.CleanupDone = true
	t.state.TargetCreated = false
	acquired, released := t.state.ReservationAcquired, t.state.ReservationReleased
	recovery := t.state.RecoveryRequired
	t.state.ReservationAcquired, t.state.ReservationReleased = false, true
	t.state.RecoveryRequired = false
	err := execCtx.SaveStateWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		return t.storage.ReleaseDiskOperation(ctx, tx, t.request.LogicalDiskId, taskID)
	})
	if err != nil {
		t.state.ReservationAcquired, t.state.ReservationReleased = acquired, released
		t.state.RecoveryRequired = recovery
		if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) {
			return err
		}
		return errors.NewRetriableErrorWithIgnoreRetryLimitf("failed to release disk reservation: %v", err)
	}
	return errors.NewNonCancellableErrorf("transfer failed before link creation: %s", t.state.ErrorMessage)
}

func (t *transferVolumeViaLinkTask) run(ctx context.Context, execCtx tasks.ExecutionContext) error {
	meta, err := t.reserve(ctx, execCtx)
	if err != nil {
		return err
	}
	if t.state.SrcDisk == nil || t.state.DstDisk == nil {
		if err := t.prepare(ctx, execCtx, meta); err != nil {
			return err
		}
	}
	link := t.volumeLink()
	if err := link.Validate(); err != nil {
		return err
	}
	if t.state.SourceCreateTaskId == "" {
		return errors.NewNonRetriableErrorf("source disk creation task ID is missing")
	}
	if t.state.Status == string(nbs.VolumeLinkStateCompleted) {
		return t.finish(ctx, execCtx)
	}
	source, err := t.nbsFactory.GetClient(ctx, link.LeaderCellID)
	if err != nil {
		return t.rpcError(err)
	}
	if err := t.ensureTarget(ctx, execCtx, link); err != nil {
		return err
	}
	for {
		status, err := source.GetVolumeLinkStatus(ctx, link)
		if err != nil {
			return t.rpcError(err)
		}
		if status.State == nbs.VolumeLinkStateNotFound {
			if err := t.createLink(ctx, execCtx, source, link); err != nil {
				return err
			}
			continue
		}
		if err := t.updateLinkStatus(ctx, execCtx, status); err != nil {
			return err
		}
		switch status.State {
		case nbs.VolumeLinkStateCompleted:
			return t.finish(ctx, execCtx)
		case nbs.VolumeLinkStateError:
			return errors.NewNonRetriableErrorf("volume link %q failed: %s", status.LinkUUID, status.ErrorMessage)
		case nbs.VolumeLinkStatePreparing, nbs.VolumeLinkStateLeadershipTransferring, nbs.VolumeLinkStateLeadershipTransferred:
		default:
			return errors.NewNonRetriableErrorf("unexpected volume link status %q", status.State)
		}
		timer := time.NewTimer(volumeLinkStatusPollPeriod)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (t *transferVolumeViaLinkTask) ensureTarget(ctx context.Context, execCtx tasks.ExecutionContext, link nbs.VolumeLink) error {
	if t.state.TargetCreated {
		return nil
	}
	if err := t.nbsFactory.CreateVolumeLinkTarget(ctx, link, execCtx.GetTaskID()); err != nil {
		return err
	}
	t.state.TargetCreated = true
	return saveDiskOperationState(ctx, execCtx)
}

func (t *transferVolumeViaLinkTask) createLink(ctx context.Context, execCtx tasks.ExecutionContext, source nbs.Client, link nbs.VolumeLink) error {
	if t.state.LinkCreated {
		return errors.NewNonRetriableErrorf("volume link %q disappeared", t.state.LinkUUID)
	}
	firstCreateAttempt := !t.state.LinkCreationStarted
	if firstCreateAttempt {
		// A persisted intent forbids cleanup after an ambiguous RPC result.
		t.state.LinkCreationStarted = true
		if err := saveDiskOperationState(ctx, execCtx); err != nil {
			// Create was not called; retry accounting must not persist an intent.
			t.state.LinkCreationStarted = false
			return err
		}
	}
	if err := source.CreateVolumeLink(ctx, link); err != nil {
		if firstCreateAttempt && nbs.IsVolumeLinkCreateRejected(err) {
			t.state.LinkCreationStarted = false
			return nbs.NormalizeVolumeLinkError(err)
		}
		return t.rpcError(err)
	}
	t.state.LinkCreated = true
	return saveDiskOperationState(ctx, execCtx)
}

func (t *transferVolumeViaLinkTask) updateLinkStatus(ctx context.Context, execCtx tasks.ExecutionContext, status nbs.VolumeLinkStatus) error {
	if !t.state.LinkCreationStarted && !t.state.LinkCreated && t.state.LinkUUID == "" {
		t.state.RecoveryRequired = true
		return errors.NewNonRetriableErrorf("received link status %q without this task's persisted create intent", status.State)
	}
	// An existing link may have been created before a lost reply or restart.
	t.state.LinkCreationStarted = true
	if status.LinkUUID == "" || (t.state.LinkUUID != "" && t.state.LinkUUID != status.LinkUUID) {
		return errors.NewNonRetriableErrorf("unexpected volume link UUID %q (expected %q)", status.LinkUUID, t.state.LinkUUID)
	}
	if !t.state.LinkCreated || t.state.LinkUUID != status.LinkUUID ||
		t.state.Status != string(status.State) {
		t.state.LinkCreated = true
		t.state.LinkUUID = status.LinkUUID
		t.state.Status = string(status.State)
		t.state.ErrorMessage = status.ErrorMessage
		if err := saveDiskOperationState(ctx, execCtx); err != nil {
			return err
		}
	}
	return nil
}

func (t *transferVolumeViaLinkTask) prepare(ctx context.Context, execCtx tasks.ExecutionContext, meta *resources.DiskMeta) error {
	logicalID := t.request.LogicalDiskId
	if meta == nil || meta.CreatedAt.IsZero() || meta.CreateTaskID == "" || meta.DeleteTaskID != "" {
		return errors.NewNonRetriableErrorf("disk %q is not ready for transfer", logicalID)
	}
	if meta.PlacementGroupID != "" {
		return errors.NewNonRetriableErrorf("placement-group disks are not supported by link transfers yet")
	}
	targetCell := t.request.GetDstCellId()
	if targetCell == "" {
		targetCell = meta.ZoneID
	}
	if !t.nbsFactory.HasClient(meta.ZoneID) || !t.nbsFactory.HasClient(targetCell) {
		return errors.NewNonRetriableErrorf("source or destination cell is not configured")
	}
	if t.cellSelector == nil {
		return errors.NewNonRetriableErrorf("cell selector is not configured")
	}
	if zoneID := t.request.GetZoneId(); zoneID != "" {
		if (meta.ZoneID != zoneID && !t.cellSelector.ZoneContainsCell(zoneID, meta.ZoneID)) ||
			(targetCell != zoneID && !t.cellSelector.ZoneContainsCell(zoneID, targetCell)) {
			return errors.NewNonRetriableErrorf("source and destination cells must belong to zone %q", zoneID)
		}
	}
	if !t.cellSelector.CellsShareZone(meta.ZoneID, targetCell) {
		return errors.NewNonRetriableErrorf("source and destination cells must belong to the same public zone")
	}
	source, err := t.nbsFactory.GetClient(ctx, meta.ZoneID)
	if err != nil {
		return err
	}
	physicalID, err := source.ResolveVolumeLinkSource(ctx, logicalID)
	if err != nil {
		return err
	}
	targetID := logicalID
	if meta.ZoneID == targetCell && physicalID == logicalID {
		targetID = logicalID + "-copy"
	}
	link := nbs.VolumeLink{LeaderDiskID: physicalID, LeaderCellID: meta.ZoneID,
		FollowerDiskID: targetID, FollowerCellID: targetCell}
	if err := link.Validate(); err != nil {
		return err
	}
	t.state.SrcDisk = &types.Disk{ZoneId: meta.ZoneID, DiskId: physicalID}
	t.state.DstDisk = &types.Disk{ZoneId: link.FollowerCellID, DiskId: targetID}
	t.state.SourceCreateTaskId = meta.CreateTaskID
	return saveDiskOperationState(ctx, execCtx)
}

func (t *transferVolumeViaLinkTask) rpcError(err error) error {
	err = nbs.NormalizeVolumeLinkError(err)
	if isTerminalDiskOperationError(err) {
		return err
	}
	if t.state.LinkCreationStarted || t.state.LinkCreated {
		// Keep observing after transport failures with an unknown outcome.
		return errors.NewRetriableErrorWithIgnoreRetryLimitf("NBS link request failed: %v", err)
	}
	return err
}

func (t *transferVolumeViaLinkTask) finish(ctx context.Context, execCtx tasks.ExecutionContext) error {
	link := t.volumeLink()
	taskID := execCtx.GetTaskID()
	if !t.state.TargetFinalized {
		if err := t.nbsFactory.FinalizeVolumeLinkTarget(ctx, link, taskID); err != nil {
			return t.recoveryError(ctx, execCtx, "target finalization", err)
		}
		t.state.TargetFinalized = true
		if err := saveDiskOperationState(ctx, execCtx); err != nil {
			return err
		}
	}
	if err := t.cleanupIncremental(ctx, execCtx, link); err != nil {
		return err
	}
	acquired, released := t.state.ReservationAcquired, t.state.ReservationReleased
	recovery, errorMessage := t.state.RecoveryRequired, t.state.ErrorMessage
	t.state.ReservationAcquired, t.state.ReservationReleased = false, true
	t.state.RecoveryRequired = false
	t.state.ErrorMessage = ""
	err := execCtx.FinishWithPreparation(ctx, func(ctx context.Context, tx *persistence.Transaction) error {
		return t.storage.FinishDiskTransfer(ctx, tx, t.request.LogicalDiskId,
			link.LeaderCellID, link.FollowerCellID, t.state.SourceCreateTaskId, taskID)
	})
	if err != nil {
		t.state.ReservationAcquired, t.state.ReservationReleased = acquired, released
		t.state.RecoveryRequired, t.state.ErrorMessage = recovery, errorMessage
		if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) {
			return err
		}
		return t.recoveryError(ctx, execCtx, "catalog finalization", err)
	}
	return nil
}

func (t *transferVolumeViaLinkTask) cleanupIncremental(ctx context.Context, execCtx tasks.ExecutionContext, link nbs.VolumeLink) error {
	if t.state.IncrementalCleanupTaskId == "" {
		key := execCtx.GetTaskID() + "_delete_disk_from_incremental"
		if t.state.IncrementalCleanupAttempt != 0 {
			key += "_" + strconv.FormatUint(t.state.IncrementalCleanupAttempt, 10)
		}
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, key),
			"dataplane.DeleteDiskFromIncremental",
			"",
			&dataplane_protos.DeleteDiskFromIncrementalRequest{
				Disk: &types.Disk{ZoneId: link.LeaderCellID, DiskId: t.request.LogicalDiskId},
			},
		)
		if err != nil {
			return errors.NewRetriableErrorWithIgnoreRetryLimitf("incremental cleanup scheduling failed: %v", err)
		}
		t.state.IncrementalCleanupTaskId = taskID
		if err := saveDiskOperationState(ctx, execCtx); err != nil {
			return err
		}
	}
	if _, err := t.scheduler.WaitTask(ctx, execCtx, t.state.IncrementalCleanupTaskId); err != nil {
		if ctx.Err() != nil || errors.Is(err, errors.NewInterruptExecutionError()) || errors.Is(err, errors.NewWrongGenerationError()) {
			return err
		}
		child, statusErr := t.scheduler.GetOperation(ctx, t.state.IncrementalCleanupTaskId)
		if statusErr != nil {
			if ctx.Err() != nil || errors.Is(statusErr, errors.NewInterruptExecutionError()) || errors.Is(statusErr, errors.NewWrongGenerationError()) {
				return statusErr
			}
			return errors.NewRetriableErrorWithIgnoreRetryLimitf("incremental cleanup status failed: %v", statusErr)
		}
		if child.GetDone() && child.GetError() != nil {
			if endErr := t.scheduler.WaitTaskEnded(ctx, t.state.IncrementalCleanupTaskId); endErr != nil {
				return endErr
			}
			t.state.IncrementalCleanupTaskId = ""
			t.state.IncrementalCleanupAttempt++
			t.state.RecoveryRequired = true
			t.state.ErrorMessage = child.GetError().GetMessage()
			// Persist a stable key for the next attempt before scheduling it.
			if saveErr := saveDiskOperationState(ctx, execCtx); saveErr != nil {
				return saveErr
			}
		}
		return errors.NewRetriableErrorWithIgnoreRetryLimitf("incremental cleanup failed: %v", err)
	}
	return nil
}

func (t *transferVolumeViaLinkTask) recoveryError(ctx context.Context, execCtx tasks.ExecutionContext, operation string, err error) error {
	if ctx.Err() != nil || errors.Is(err, errors.NewWrongGenerationError()) || errors.Is(err, errors.NewInterruptExecutionError()) {
		return err
	}
	err = nbs.NormalizeVolumeLinkError(err)
	if isTerminalDiskOperationError(err) {
		t.state.RecoveryRequired = true
		t.state.ErrorMessage = errors.ErrorForTracing(err)
		if saveErr := saveDiskOperationState(ctx, execCtx); saveErr != nil {
			return saveErr
		}
	}
	// Keep the owner executable: retry can observe completion or repeat cleanup.
	return errors.NewRetriableErrorWithIgnoreRetryLimitf("%s failed: %v", operation, err)
}

func (t *transferVolumeViaLinkTask) Cancel(context.Context, tasks.ExecutionContext) error {
	return errors.NewNonCancellableErrorf("%s does not support cancellation yet", TransferVolumeViaLinkTaskType)
}

func (t *transferVolumeViaLinkTask) GetMetadata(context.Context) (proto.Message, error) {
	return &disk_manager.TransferVolumeViaLinkMetadata{
		LinkUuid:         t.state.LinkUUID,
		Status:           t.state.Status,
		LogicalDiskId:    t.request.LogicalDiskId,
		SrcDisk:          volumeLinkDiskID(t.state.SrcDisk),
		DstDisk:          volumeLinkDiskID(t.state.DstDisk),
		ErrorMessage:     t.state.ErrorMessage,
		RecoveryRequired: t.state.RecoveryRequired,
	}, nil
}

func (t *transferVolumeViaLinkTask) GetResponse() proto.Message { return &empty.Empty{} }

func (t *transferVolumeViaLinkTask) reserve(ctx context.Context, execCtx tasks.ExecutionContext) (*resources.DiskMeta, error) {
	sourceCell := t.state.GetSrcDisk().GetZoneId()
	meta, err := acquireDiskOperation(ctx, execCtx, t.storage, resources.DiskOperation{
		DiskID: t.request.LogicalDiskId, Kind: resources.DiskOperationTransfer, CellID: sourceCell,
	}, &t.state.ReservationAcquired, &t.state.SourceCreateTaskId)
	if err != nil {
		if t.state.ReservationAcquired && errors.Is(err, errors.NewEmptyNonRetriableError()) {
			t.state.RecoveryRequired = true
		}
		return nil, err
	}
	if meta == nil {
		return nil, errors.NewNonRetriableErrorf("disk reservation returned no metadata")
	}
	return meta, nil
}

func volumeLinkDiskID(disk *types.Disk) *disk_manager.DiskId {
	if disk == nil {
		return nil
	}
	return &disk_manager.DiskId{ZoneId: disk.ZoneId, DiskId: disk.DiskId}
}
