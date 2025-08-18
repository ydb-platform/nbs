package images

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	internal_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/common"
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
) (string, error) {

	disk := t.request.SrcDisk

	selfTaskID := execCtx.GetTaskID()

	diskParams, err := nbsClient.Describe(ctx, disk.DiskId)
	if err != nil {
		return "", err
	}

	if internal_common.IsLocalDiskKind(diskParams.Kind) {
		return "", errors.NewNonCancellableErrorf(
			"creating image from local disk is forbidden",
		)
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
		return "", err
	}

	if imageMeta.Ready {
		// Already created.
		checkpointID := imageMeta.CheckpointID
		if checkpointID == "" {
			// Temporary solution for backwards compatibility. If checkpoint id
			// is empty, then the image was created by an older version of Disk
			// Manager. In that version checkpoint id is always the same as
			// image id.
			// TODO: remove it later.
			checkpointID = t.request.DstImageId
		}
		return checkpointID, nil
	}

	checkpointID, err := common.CreateCheckpoint(
		ctx,
		execCtx,
		t.scheduler,
		nbsClient,
		t.request.SrcDisk,
		t.request.DstImageId,
		selfTaskID,
		diskParams.IsDiskRegistryBasedDisk,
		t.request.RetryBrokenDRBasedDiskCheckpoint,
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
			DstSnapshotId:       t.request.DstImageId,
			UseS3:               t.request.UseS3,
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
		return "", err
	}

	err = t.storage.ImageCreated(
		ctx,
		t.request.DstImageId,
		checkpointID,
		time.Now(),
		uint64(t.state.ImageSize),
		uint64(t.state.ImageStorageSize),
	)
	if err != nil {
		return "", err
	}

	return checkpointID, nil
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

	checkpointID, err := t.run(ctx, execCtx, nbsClient)
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
	nbsClient, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return err
	}

	checkpointID, err := common.CancelCheckpointCreation(
		ctx,
		t.scheduler,
		nbsClient,
		disk,
		t.request.DstImageId,
		execCtx.GetTaskID(),
		t.request.RetryBrokenDRBasedDiskCheckpoint,
	)
	if err != nil {
		return err
	}

	if checkpointID != "" {
		err = nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
		if err != nil {
			return err
		}
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
