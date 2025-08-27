package images

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/accounting"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	url_package "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url"
	url_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
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

type createImageFromURLTask struct {
	config            *config.ImagesConfig
	performanceConfig *performance_config.PerformanceConfig
	scheduler         tasks.Scheduler
	storage           resources.Storage
	poolService       pools.Service
	request           *protos.CreateImageFromURLRequest
	state             *protos.CreateImageFromURLTaskState
}

func (t *createImageFromURLTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createImageFromURLTask) Load(request, state []byte) error {
	t.request = &protos.CreateImageFromURLRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateImageFromURLTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createImageFromURLTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	imageFormat, err := t.imageFormat(ctx, execCtx)
	if err != nil {
		return err
	}

	if !imageFormat.IsSupported() {
		return url_common.NewSourceInvalidError(
			"unsupported image format: %s",
			imageFormat,
		)
	}

	_, err = t.storage.CreateImage(ctx, resources.ImageMeta{
		ID:                t.request.DstImageId,
		FolderID:          t.request.FolderId,
		CreateRequest:     t.request,
		CreateTaskID:      selfTaskID,
		CreatingAt:        time.Now(),
		CreatedBy:         "",   // TODO: extract CreatedBy from execCtx.
		UseDataplaneTasks: true, // TODO: remove it.
	})
	if err != nil {
		return err
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateSnapshotFromURL",
		"",
		&dataplane_protos.CreateSnapshotFromURLRequest{
			SrcURL:        t.request.SrcURL,
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

	typedResponse, ok := response.(*dataplane_protos.CreateSnapshotFromURLResponse)
	if !ok {
		return errors.NewNonRetriableErrorf(
			"invalid dataplane.CreateSnapshotFromURL response type %T",
			response,
		)
	}

	// TODO: estimate should be applied before resource creation, not after.
	execCtx.SetInflightEstimate(performance.Estimate(
		typedResponse.TransferredDataSize,
		t.performanceConfig.GetCreateImageFromURLBandwidthMiBs(),
	))

	t.state.ImageSize = int64(typedResponse.SnapshotSize)
	t.state.ImageStorageSize = int64(typedResponse.SnapshotStorageSize)

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

	accounting.OnImageCreated(t.request.FolderId, imageFormat)

	return configureImagePools(
		ctx,
		execCtx,
		t.scheduler,
		t.poolService,
		t.request.DstImageId,
		t.request.DiskPools,
	)
}

func (t *createImageFromURLTask) Cancel(
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

func (t *createImageFromURLTask) GetMetadata(
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

		createMetadata, ok := message.(*dataplane_protos.CreateSnapshotFromURLMetadata)
		if ok {
			metadata.Progress = createMetadata.Progress
		}
	} else {
		metadata.Progress = t.state.Progress
	}

	return metadata, nil
}

func (t *createImageFromURLTask) GetResponse() proto.Message {
	return &disk_manager.CreateImageResponse{
		Size:        t.state.ImageSize,
		StorageSize: t.state.ImageStorageSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createImageFromURLTask) imageFormat(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (url_package.ImageFormat, error) {

	if t.state.ImageFormat != nil {
		return url_package.ImageFormat(*t.state.ImageFormat), nil
	}

	imageFormat, err := url_package.GetImageFormat(
		ctx,
		t.request.SrcURL,
	)
	if err != nil {
		return "", err
	}

	t.state.ImageFormat = (*string)(&imageFormat)

	err = execCtx.SaveState(ctx)
	if err != nil {
		return "", err
	}

	return url_package.ImageFormat(t.state.GetImageFormat()), nil
}
