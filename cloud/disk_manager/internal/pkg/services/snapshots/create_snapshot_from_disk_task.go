package snapshots

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
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
) (string, error) {

	disk := t.request.SrcDisk
	selfTaskID := execCtx.GetTaskID()

	diskParams, err := nbsClient.Describe(ctx, disk.DiskId)
	if err != nil {
		return "", err
	}

	snapshotMeta, err := t.storage.CreateSnapshot(ctx, resources.SnapshotMeta{
		ID:                t.request.DstSnapshotId,
		FolderID:          t.request.FolderId,
		Disk:              disk,
		CreateRequest:     t.request,
		CreateTaskID:      selfTaskID,
		CreatingAt:        time.Now(),
		CreatedBy:         "",   // TODO: extract CreatedBy from execCtx.
		UseDataplaneTasks: true, // TODO: remove it.
		Encryption:        diskParams.EncryptionDesc,
	})
	if err != nil {
		return "", err
	}

	if snapshotMeta.Ready {
		// Already created.
		return snapshotMeta.CheckpointID, nil
	}

	checkpointID, err := t.createCheckpoint(
		ctx,
		execCtx,
		nbsClient,
		diskParams.IsDiskRegistryBasedDisk,
		selfTaskID,
	)
	if err != nil {
		return "", err
	}

	taskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateSnapshotFromDisk",
		"",
		disk.ZoneId,
		&dataplane_protos.CreateSnapshotFromDiskRequest{
			SrcDisk:             disk,
			SrcDiskCheckpointId: checkpointID,
			DstSnapshotId:       t.request.DstSnapshotId,
			UseS3:               t.request.UseS3,
			UseProxyOverlayDisk: t.request.UseProxyOverlayDisk,
		},
	)
	if err != nil {
		return "", err
	}

	t.state.DataplaneTaskID = taskID

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return "", err
	}

	typedResponse, ok := response.(*dataplane_protos.CreateSnapshotFromDiskResponse)
	if !ok {
		return "", errors.NewNonRetriableErrorf(
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
		return "", err
	}

	err = t.storage.SnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		checkpointID,
		time.Now(),
		uint64(t.state.SnapshotSize),
		uint64(t.state.SnapshotStorageSize),
	)
	if err != nil {
		return "", err
	}

	return checkpointID, nil
}

func (t *createSnapshotFromDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	disk := t.request.SrcDisk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	checkpointID, err := t.run(ctx, execCtx, nbsClient)
	if err != nil {
		return err
	}

	diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return err
	}

	if diskParams.IsDiskRegistryBasedDisk {
		return nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
	}

	return nbsClient.DeleteCheckpointData(ctx, disk.DiskId, checkpointID)
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

	selfTaskID := execCtx.GetTaskID()

	// TODO:_ do not describe in cancel ???
	diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return err
	}

	err = t.deleteCheckpoint(
		ctx,
		nbsClient,
		diskParams.IsDiskRegistryBasedDisk,
		selfTaskID,
	)
	if err != nil {
		return err
	}

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
		// Nothing to do.
		return nil
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

func (t *createSnapshotFromDiskTask) scheduleCreateShadowDiskBasedCheckpointTask(
	ctx context.Context,
	selfTaskID string,
) (string, error) {

	disk := t.request.SrcDisk

	return t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_create_checkpoint"), // TODO:_ idemp keys ok now?
		"dataplane.CreateShadowDiskBasedCheckpoint",
		"", // TODO:_ do we need description?
		&dataplane_protos.CreateShadowDiskBasedCheckpointRequest{
			Disk:               disk,
			CheckpointIdPrefix: t.request.DstSnapshotId,
		},
	)
}

func (t *createSnapshotFromDiskTask) createCheckpoint(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nbsClient nbs.Client,
	isDiskRegistryBasedDisk bool,
	selfTaskID string,
) (string, error) {

	disk := t.request.SrcDisk

	if !isDiskRegistryBasedDisk {
		checkpointID := t.request.DstSnapshotId

		err := nbsClient.CreateCheckpoint(
			ctx,
			nbs.CheckpointParams{
				DiskID:       disk.DiskId,
				CheckpointID: checkpointID,
			},
		)
		if err != nil {
			return "", err
		}

		return checkpointID, nil
	}

	taskID, err := t.scheduleCreateShadowDiskBasedCheckpointTask(
		ctx,
		selfTaskID,
	)
	if err != nil {
		return "", err
	}

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return "", err
	}

	typedResponse, ok := response.(*dataplane_protos.CreateShadowDiskBasedCheckpointResponse)
	if !ok {
		return "", errors.NewNonRetriableErrorf(
			"invalid create shadow disk based checkpoint response type %T",
			response,
		)
	}

	return typedResponse.CheckpointId, nil
}

func (t *createSnapshotFromDiskTask) deleteCheckpoint(
	ctx context.Context,
	nbsClient nbs.Client,
	isDiskRegistryBasedDisk bool,
	selfTaskID string,
) error {

	disk := t.request.SrcDisk

	if !isDiskRegistryBasedDisk {
		return nbsClient.DeleteCheckpoint(
			ctx,
			disk.DiskId,
			t.request.DstSnapshotId, // checkpointID
		)
	}

	checkpointTaskID, err := t.scheduleCreateShadowDiskBasedCheckpointTask(
		ctx,
		selfTaskID,
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.CancelTask(ctx, checkpointTaskID)
	if err != nil {
		return err
	}

	err = t.scheduler.WaitTaskEnded(ctx, checkpointTaskID)
	if err != nil {
		return err
	}

	metadata, err := t.scheduler.GetTaskMetadata(ctx, checkpointTaskID)
	if err != nil {
		return err
	}

	typedMetadata, ok := metadata.(*dataplane_protos.CreateShadowDiskBasedCheckpointMetadata)
	if !ok {
		return nil // TODO:_ return error?
	}
	checkpointID := typedMetadata.CheckpointId
	if checkpointID != "" {
		return nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
	}

	return nil
}
