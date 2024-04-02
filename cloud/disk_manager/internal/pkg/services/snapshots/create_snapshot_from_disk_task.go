package snapshots

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotFromDiskTask struct {
	performanceConfig *performance_config.PerformanceConfig
	scheduler         tasks.Scheduler
	storage           resources.Storage
	nbsFactory        nbs.Factory
	request           *protos.CreateSnapshotFromDiskRequest
	state             *protos.CreateSnapshotFromDiskTaskState
}

func (t *createSnapshotFromDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createSnapshotFromDiskTask) Load(request, state []byte) error {
	t.request = &protos.CreateSnapshotFromDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateSnapshotFromDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createSnapshotFromDiskTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nbsClient nbs.Client,
	checkpointID string,
) (*resources.SnapshotMeta, error) {

	disk := t.request.SrcDisk
	selfTaskID := execCtx.GetTaskID()

	diskParams, err := nbsClient.Describe(ctx, disk.DiskId)
	if err != nil {
		return nil, err
	}

	snapshotMeta, err := t.storage.CreateSnapshot(ctx, resources.SnapshotMeta{
		ID:                t.request.DstSnapshotId,
		FolderID:          t.request.FolderId,
		Disk:              disk,
		CheckpointID:      checkpointID,
		CreateRequest:     t.request,
		CreateTaskID:      selfTaskID,
		CreatingAt:        time.Now(),
		CreatedBy:         "",   // TODO: extract CreatedBy from execCtx.
		UseDataplaneTasks: true, // TODO: remove it.
		Encryption:        diskParams.EncryptionDesc,
	})
	if err != nil {
		return nil, err
	}

	if snapshotMeta == nil {
		return nil, errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.DstSnapshotId,
		)
	}

	if snapshotMeta.Ready {
		// Already created.
		return snapshotMeta, nil
	}

	err = nbsClient.CreateCheckpoint(
		ctx,
		nbs.CheckpointParams{
			DiskID:       disk.DiskId,
			CheckpointID: checkpointID,
		},
	)
	if err != nil {
		return nil, err
	}

	err = t.ensureCheckpointReady(ctx, nbsClient, disk.DiskId, checkpointID)
	if err != nil {
		return nil, err
	}

	baseSnapshotID := snapshotMeta.BaseSnapshotID
	baseCheckpointID := snapshotMeta.BaseCheckpointID

	if diskParams.IsDiskRegistryBasedDisk {
		// Should perform full snapshot of disk.
		// TODO: enable incremental snapshots for such disks.
		baseSnapshotID = ""
		baseCheckpointID = ""
	}

	if len(baseSnapshotID) != 0 {
		// Lock base snapshot to prevent deletion.
		locked, err := t.storage.LockSnapshot(
			ctx,
			snapshotMeta.BaseSnapshotID,
			selfTaskID,
		)
		if err != nil {
			return nil, err
		}

		if locked {
			logging.Debug(
				ctx,
				"Successfully locked snapshot with id %v",
				snapshotMeta.BaseSnapshotID,
			)
		} else {
			logging.Info(
				ctx,
				"Snapshot with id %v can't be locked",
				snapshotMeta.BaseSnapshotID,
			)

			// Should perform full snapshot of disk.
			baseSnapshotID = ""
			baseCheckpointID = ""
		}
	}

	taskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateSnapshotFromDisk",
		"",
		disk.ZoneId,
		&dataplane_protos.CreateSnapshotFromDiskRequest{
			SrcDisk:                 disk,
			SrcDiskBaseCheckpointId: baseCheckpointID,
			SrcDiskCheckpointId:     checkpointID,
			BaseSnapshotId:          baseSnapshotID,
			DstSnapshotId:           t.request.DstSnapshotId,
			UseS3:                   t.request.UseS3,
			UseProxyOverlayDisk:     t.request.UseProxyOverlayDisk,
		},
		t.request.OperationCloudId,
		t.request.OperationFolderId,
	)
	if err != nil {
		return nil, err
	}

	t.state.DataplaneTaskID = taskID

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return nil, err
	}

	typedResponse, ok := response.(*dataplane_protos.CreateSnapshotFromDiskResponse)
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"invalid create snapshot response type %T",
			response,
		)
	}

	// TODO: estimate should be applied before resource creation, not after.
	execCtx.SetEstimate(performance.Estimate(
		typedResponse.TransferredDataSize,
		t.performanceConfig.GetCreateSnapshotFromDiskBandwidthMiBs(),
	))

	t.state.SnapshotSize = int64(typedResponse.SnapshotSize)
	t.state.SnapshotStorageSize = int64(typedResponse.SnapshotStorageSize)

	err = execCtx.SaveState(ctx)
	if err != nil {
		return nil, err
	}

	err = t.storage.SnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		time.Now(),
		uint64(t.state.SnapshotSize),
		uint64(t.state.SnapshotStorageSize),
	)
	if err != nil {
		return nil, err
	}

	return snapshotMeta, nil
}

func (t *createSnapshotFromDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	disk := t.request.SrcDisk
	// NOTE: we use snapshot id as checkpoint id.
	checkpointID := t.request.DstSnapshotId
	selfTaskID := execCtx.GetTaskID()

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	snapshotMeta, err := t.run(ctx, execCtx, nbsClient, checkpointID)
	if err != nil {
		return err
	}

	if len(snapshotMeta.BaseSnapshotID) != 0 {
		err := t.storage.UnlockSnapshot(
			ctx,
			snapshotMeta.BaseSnapshotID,
			selfTaskID,
		)
		if err != nil {
			return err
		}

		logging.Debug(
			ctx,
			"Successfully unlocked snapshot with id %v",
			snapshotMeta.BaseSnapshotID,
		)
	}

	err = nbsClient.DeleteCheckpointData(ctx, disk.DiskId, checkpointID)
	if err != nil {
		return err
	}

	if len(snapshotMeta.BaseCheckpointID) == 0 {
		return nil
	}

	return nbsClient.DeleteCheckpoint(
		ctx,
		disk.DiskId,
		snapshotMeta.BaseCheckpointID,
	)
}

func (t *createSnapshotFromDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	disk := t.request.SrcDisk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	// NOTE: we use snapshot id as checkpoint id.
	checkpointID := t.request.DstSnapshotId

	// NBS-1873: should always delete checkpoint.
	err = nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.DeleteSnapshot(
		ctx,
		t.request.DstSnapshotId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if snapshotMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			t.request.DstSnapshotId,
		)
	}

	// NBS-3192.
	if len(snapshotMeta.BaseSnapshotID) != 0 {
		err := t.storage.UnlockSnapshot(
			ctx,
			snapshotMeta.BaseSnapshotID,
			selfTaskID,
		)
		if err != nil {
			return err
		}

		logging.Debug(
			ctx,
			"Successfully unlocked snapshot with id %v",
			snapshotMeta.BaseSnapshotID,
		)
	}

	// Hack for NBS-2225.
	if snapshotMeta.DeleteTaskID != selfTaskID {
		return t.scheduler.WaitTaskEnded(ctx, snapshotMeta.DeleteTaskID)
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_cancel"),
		"dataplane.DeleteSnapshot",
		"",
		&dataplane_protos.DeleteSnapshotRequest{
			SnapshotId: t.request.DstSnapshotId,
		},
		t.request.OperationCloudId,
		t.request.OperationFolderId,
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.storage.SnapshotDeleted(ctx, t.request.DstSnapshotId, time.Now())
}

func (t *createSnapshotFromDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.CreateSnapshotMetadata{}

	if len(t.state.DataplaneTaskID) != 0 {
		message, err := t.scheduler.GetTaskMetadata(
			ctx,
			t.state.DataplaneTaskID,
		)
		if err != nil {
			return nil, err
		}

		createMetadata, ok := message.(*dataplane_protos.CreateSnapshotFromDiskMetadata)
		if ok {
			metadata.Progress = createMetadata.Progress
		}
	} else {
		metadata.Progress = t.state.Progress
	}

	return metadata, nil
}

func (t *createSnapshotFromDiskTask) GetResponse() proto.Message {
	return &disk_manager.CreateSnapshotResponse{
		Size:        t.state.SnapshotSize,
		StorageSize: t.state.SnapshotStorageSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createSnapshotFromDiskTask) ensureCheckpointReady(
	ctx context.Context,
	nbsClient nbs.Client,
	diskID string,
	checkpointID string,
) error {

	status, err := nbsClient.GetCheckpointStatus(ctx, diskID, checkpointID)
	if err != nil {
		return err
	}

	logging.Debug(
		ctx,
		"Current CheckpointStatus: %v",
		status,
	)

	switch status {
	case nbs.CheckpointStatusNotReady:
		return errors.NewInterruptExecutionError()

	case nbs.CheckpointStatusError:
		_ = nbsClient.DeleteCheckpoint(ctx, diskID, checkpointID)
		return errors.NewRetriableErrorf("Filling the NRD disk replica ended with an error.")

	case nbs.CheckpointStatusReady:
		// Nothing to do.
	}

	return nil
}
