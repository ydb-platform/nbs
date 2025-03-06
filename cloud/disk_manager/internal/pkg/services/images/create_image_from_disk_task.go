package images

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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createImageFromDiskTask struct {
	config            *config.ImagesConfig
	performanceConfig *performance_config.PerformanceConfig
	scheduler         tasks.Scheduler
	storage           resources.Storage
	nbsFactory        nbs.Factory
	poolService       pools.Service
	request           *protos.CreateImageFromDiskRequest
	state             *protos.CreateImageFromDiskTaskState
}

func (t *createImageFromDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createImageFromDiskTask) Load(request, state []byte) error {
	t.request = &protos.CreateImageFromDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateImageFromDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createImageFromDiskTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nbsClient nbs.Client,
) error {

	disk := t.request.SrcDisk
	selfTaskID := execCtx.GetTaskID()

	diskParams, err := nbsClient.Describe(ctx, disk.DiskId)
	if err != nil {
		return err
	}

	imageMeta, err := t.storage.CreateImage(ctx, resources.ImageMeta{
		ID:                t.request.DstImageId,
		FolderID:          t.request.FolderId,
		SrcDiskID:         t.request.SrcDisk.DiskId,
		CreateRequest:     t.request,
		CreateTaskID:      selfTaskID,
		CreatingAt:        time.Now(),
		CreatedBy:         "",   // TODO: extract CreatedBy from execCtx.
		UseDataplaneTasks: true, // TODO: remove it.
		Encryption:        diskParams.EncryptionDesc,
	})
	if err != nil {
		return err
	}

	if imageMeta.Ready {
		// Already created.
		return nil
	}

	checkpointID, err := t.createCheckpoint(
		ctx,
		execCtx,
		nbsClient,
		diskParams.IsDiskRegistryBasedDisk,
		selfTaskID,
	)
	if err != nil {
		return err
	}

	taskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateSnapshotFromDisk",
		"",
		disk.ZoneId,
		&dataplane_protos.CreateSnapshotFromDiskRequest{
			SrcDisk:             disk,
			SrcDiskCheckpointId: checkpointID,
			DstSnapshotId:       t.request.DstImageId,
			UseS3:               t.request.UseS3,
		},
	)
	if err != nil {
		return err
	}

	t.state.DataplaneTaskID = taskID

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	typedResponse, ok := response.(*dataplane_protos.CreateSnapshotFromDiskResponse)
	if !ok {
		return errors.NewNonRetriableErrorf(
			"invalid dataplane.CreateSnapshotFromDisk response type %T",
			response,
		)
	}

	// TODO: estimate should be applied before resource creation, not after.
	execCtx.SetEstimate(performance.Estimate(
		typedResponse.SnapshotStorageSize,
		t.performanceConfig.GetCreateImageFromDiskBandwidthMiBs(),
	))

	t.state.ImageSize = int64(typedResponse.SnapshotSize)
	t.state.ImageStorageSize = int64(typedResponse.SnapshotStorageSize)

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	return t.storage.ImageCreated(
		ctx,
		t.request.DstImageId,
		checkpointID,
		time.Now(),
		uint64(t.state.ImageSize),
		uint64(t.state.ImageStorageSize),
	)
}

func (t *createImageFromDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	disk := t.request.SrcDisk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	err = t.run(ctx, execCtx, nbsClient)
	if err != nil {
		return err
	}

	err = configureImagePools(
		ctx,
		execCtx,
		t.scheduler,
		t.poolService,
		t.request.DstImageId,
		t.request.DiskPools,
	)
	if err != nil {
		return err
	}

	diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	checkpointID, err := t.getCheckpointID(
		ctx,
		selfTaskID,
		diskParams.IsDiskRegistryBasedDisk,
	)
	if err != nil {
		return err
	}

	if diskParams.IsDiskRegistryBasedDisk {
		return nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
	}

	return nbsClient.DeleteCheckpointData(ctx, disk.DiskId, checkpointID)
}

func (t *createImageFromDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	disk := t.request.SrcDisk

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	checkpointTaskID, err := t.scheduler.GetTaskIDByIdempotencyKey(
		headers.SetIncomingIdempotencyKey(
			ctx,
			t.getIdempotencyKeyForCheckpointTask(selfTaskID),
		),
	)
	if err != nil {
		return err
	}

	if checkpointTaskID == "" {
		_, err = t.scheduler.CancelTask(ctx, checkpointTaskID)
		if err != nil {
			return err
		}
	}

	err = t.deleteCheckpoint(ctx, nbsClient, selfTaskID)
	if err != nil {
		return err
	}

	return deleteImage(
		ctx,
		execCtx,
		t.config,
		t.scheduler,
		t.storage,
		t.poolService,
		t.request.DstImageId,
	)
}

func (t *createImageFromDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.CreateImageMetadata{}

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

func (t *createImageFromDiskTask) GetResponse() proto.Message {
	return &disk_manager.CreateImageResponse{
		Size:        t.state.ImageSize,
		StorageSize: t.state.ImageStorageSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createImageFromDiskTask) getIdempotencyKeyForCheckpointTask(
	selfTaskID string,
) string {

	return selfTaskID + "_create_checkpoint"
}

func (t *createImageFromDiskTask) scheduleCreateShadowDiskBasedCheckpointTask(
	ctx context.Context,
	selfTaskID string,
) (string, error) {

	disk := t.request.SrcDisk

	return t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			t.getIdempotencyKeyForCheckpointTask(selfTaskID),
		),
		"dataplane.CreateShadowDiskBasedCheckpoint",
		"Create checkpoint for image "+t.request.DstImageId,
		&dataplane_protos.CreateShadowDiskBasedCheckpointRequest{
			Disk:               disk,
			CheckpointIdPrefix: t.request.DstImageId,
		},
	)
}

func (t *createImageFromDiskTask) createCheckpoint(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nbsClient nbs.Client,
	isDiskRegistryBasedDisk bool,
	selfTaskID string,
) (string, error) {

	disk := t.request.SrcDisk

	if !isDiskRegistryBasedDisk {
		checkpointID := t.request.DstImageId

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

func (t *createImageFromDiskTask) getCheckpointID(
	ctx context.Context,
	selfTaskID string,
	isDiskRegistryBasedDisk bool,
) (string, error) {

	if !isDiskRegistryBasedDisk {
		return t.request.DstImageId, nil
	}

	checkpointTaskID, err := t.scheduler.GetTaskIDByIdempotencyKey(
		headers.SetIncomingIdempotencyKey(
			ctx,
			t.getIdempotencyKeyForCheckpointTask(selfTaskID),
		),
	)
	if err != nil {
		return "", err
	}

	if checkpointTaskID == "" {
		return "", nil
	}

	err = t.scheduler.WaitTaskEnded(ctx, checkpointTaskID)
	if err != nil {
		return "", err
	}

	metadata, err := t.scheduler.GetTaskMetadata(ctx, checkpointTaskID)
	if err != nil {
		return "", err
	}

	typedMetadata, ok := metadata.(*dataplane_protos.CreateShadowDiskBasedCheckpointMetadata)
	if !ok {
		return "", errors.NewNonRetriableErrorf(
			"invalid create shadow disk based checkpoint metadata type %T",
			metadata,
		)
	}

	return typedMetadata.CheckpointId, nil
}

func (t *createImageFromDiskTask) deleteCheckpoint(
	ctx context.Context,
	nbsClient nbs.Client,
	selfTaskID string,
) error {

	disk := t.request.SrcDisk

	diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		if nbs.IsNotFoundError(err) {
			// No need to delete checkpoint if disk does not exist.
			return nil
		}
		return err
	}

	checkpointID, err := t.getCheckpointID(
		ctx,
		selfTaskID,
		diskParams.IsDiskRegistryBasedDisk,
	)
	if err != nil {
		return err
	}

	return nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
}
