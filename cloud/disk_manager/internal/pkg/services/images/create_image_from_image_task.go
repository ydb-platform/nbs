package images

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createImageFromImageTask struct {
	config            *config.ImagesConfig
	performanceConfig *performance_config.PerformanceConfig
	scheduler         tasks.Scheduler
	storage           resources.Storage
	poolService       pools.Service
	request           *protos.CreateImageFromImageRequest
	state             *protos.CreateImageFromImageTaskState
}

func (t *createImageFromImageTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createImageFromImageTask) Load(request, state []byte) error {
	t.request = &protos.CreateImageFromImageRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateImageFromImageTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createImageFromImageTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	srcImageMeta, err := t.storage.GetImageMeta(ctx, t.request.SrcImageId)
	if err != nil {
		return err
	}

	var srcImageEncryption *types.EncryptionDesc
	if srcImageMeta != nil {
		srcImageEncryption = srcImageMeta.Encryption
	}

	_, err = t.storage.CreateImage(ctx, resources.ImageMeta{
		ID:                t.request.DstImageId,
		FolderID:          t.request.FolderId,
		SrcImageID:        t.request.SrcImageId,
		CreateRequest:     t.request,
		CreateTaskID:      selfTaskID,
		CreatingAt:        time.Now(),
		CreatedBy:         "",   // TODO: extract CreatedBy from execCtx.
		UseDataplaneTasks: true, // TODO: remove it.
		Encryption:        srcImageEncryption,
	})
	if err != nil {
		return err
	}

	srcIsDataplane := srcImageMeta != nil && srcImageMeta.UseDataplaneTasks

	if srcIsDataplane {
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
			"dataplane.CreateSnapshotFromSnapshot",
			"",
			&dataplane_protos.CreateSnapshotFromSnapshotRequest{
				SrcSnapshotId: t.request.SrcImageId,
				DstSnapshotId: t.request.DstImageId,
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

		typedResponse, ok := response.(*dataplane_protos.CreateSnapshotFromSnapshotResponse)
		if !ok {
			return errors.NewNonRetriableErrorf(
				"invalid dataplane.CreateSnapshotFromSnapshot response type %T",
				response,
			)
		}

		// TODO: estimate should be applied before resource creation, not after.
		execCtx.SetEstimate(performance.Estimate(
			typedResponse.SnapshotStorageSize,
			t.performanceConfig.GetCreateImageFromImageBandwidthMiBs(),
		))

		t.state.ImageSize = int64(typedResponse.SnapshotSize)
		t.state.ImageStorageSize = int64(typedResponse.SnapshotStorageSize)
	} else {
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
			"dataplane.CreateSnapshotFromLegacySnapshot",
			"",
			&dataplane_protos.CreateSnapshotFromLegacySnapshotRequest{
				SrcSnapshotId: t.request.SrcImageId,
				DstSnapshotId: t.request.DstImageId,
				UseS3:         t.request.UseS3,
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

		typedResponse, ok := response.(*dataplane_protos.CreateSnapshotFromLegacySnapshotResponse)
		if !ok {
			return errors.NewNonRetriableErrorf(
				"invalid dataplane.CreateSnapshotFromLegacySnapshot response type %T",
				response,
			)
		}

		// TODO: estimate should be applied before resource creation, not after.
		execCtx.SetEstimate(performance.Estimate(
			typedResponse.TransferredDataSize,
			t.performanceConfig.GetCreateImageFromImageBandwidthMiBs(),
		))

		t.state.ImageSize = int64(typedResponse.SnapshotSize)
		t.state.ImageStorageSize = int64(typedResponse.SnapshotStorageSize)
	}

	err = t.storage.ImageCreated(
		ctx,
		t.request.DstImageId,
		"", // checkpointID
		time.Now(),
		uint64(t.state.ImageSize),
		uint64(t.state.ImageStorageSize),
	)
	if err != nil {
		return err
	}

	return configureImagePools(
		ctx,
		execCtx,
		t.scheduler,
		t.poolService,
		t.request.DstImageId,
		t.request.DiskPools,
	)
}

func (t *createImageFromImageTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

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

func (t *createImageFromImageTask) GetMetadata(
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

		createMetadata, ok := message.(*dataplane_protos.CreateSnapshotFromSnapshotMetadata)
		if ok {
			metadata.Progress = createMetadata.Progress
		} else {
			createMetadata, ok := message.(*dataplane_protos.CreateSnapshotFromLegacySnapshotMetadata)
			if ok {
				metadata.Progress = createMetadata.Progress
			}
		}
	} else {
		metadata.Progress = t.state.Progress
	}

	return metadata, nil
}

func (t *createImageFromImageTask) GetResponse() proto.Message {
	return &disk_manager.CreateImageResponse{
		Size:        t.state.ImageSize,
		StorageSize: t.state.ImageStorageSize,
	}
}
