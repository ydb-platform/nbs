package images

import (
	"context"

	"github.com/golang/protobuf/proto"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

type exportImageTask struct {
	scheduler tasks.Scheduler
	storage   resources.Storage
	config    *server_config.ExportConfig
	request   *api.ExportImageRequest
	state     *protos.ExportImageTaskState
}

func (t *exportImageTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *exportImageTask) Load(request, state []byte) error {
	t.request = &api.ExportImageRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}

	t.state = &protos.ExportImageTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *exportImageTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.Response != nil {
		return nil
	}

	if t.state.DataplaneTaskId == "" {
		// The scheduler persists x-task-id and restores it in the runner.
		// It identifies the Compute task; execCtx.GetTaskID is our task.
		computeTaskID := headers.GetTaskID(ctx)
		if computeTaskID == "" {
			return errors.NewNonRetriableErrorf(
				"Compute export task ID is missing from x-task-id",
			)
		}

		meta, err := t.storage.GetImageMeta(ctx, t.request.ImageId)
		if err != nil {
			return err
		}

		if meta == nil || !meta.Ready {
			return errors.NewNonRetriableErrorf(
				"image %v is absent or not ready",
				t.request.ImageId,
			)
		}

		if meta.FolderID != t.request.FolderId {
			return errors.NewNonRetriableErrorf(
				"image folder does not match export request",
			)
		}

		if common.Find(t.config.GetForbiddenFolderIds(), meta.FolderID) {
			return errors.NewNonRetriableErrorf(
				"image export is forbidden for folder %v",
				meta.FolderID,
			)
		}

		diskID := meta.SrcDiskID
		var blockSize uint32
		if diskID != "" {
			// The disk row can survive disk deletion until metadata cleanup.
			disk, err := t.storage.GetDiskMeta(ctx, diskID)
			if err != nil {
				return err
			}

			if disk != nil {
				blockSize = disk.BlockSize
			}
		}

		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				execCtx.GetTaskID()+"_export",
			),
			"dataplane.ExportImageToS3",
			"",
			&dataplane_protos.ExportSnapshotRequest{
				SnapshotId:     t.request.ImageId,
				ImageId:        t.request.ImageId,
				ComputeTaskId:  computeTaskID,
				FolderId:       meta.FolderID,
				SourceDiskId:   diskID,
				BlockSize:      blockSize,
				Destination:    t.request.Dst,
				DecryptionInfo: t.request.DecryptionInfo,
			},
		)
		if err != nil {
			return err
		}

		t.state.DataplaneTaskId = taskID
		if err := execCtx.SaveState(ctx); err != nil {
			return err
		}
	}

	response, err := t.scheduler.WaitTask(ctx, execCtx, t.state.DataplaneTaskId)
	if err != nil {
		return err
	}

	exportResponse, ok := response.(*api.ExportResponse)
	if !ok {
		return errors.NewNonRetriableErrorf(
			"unexpected image export response type",
		)
	}

	t.state.Response = exportResponse
	return execCtx.SaveState(ctx)
}

func (t *exportImageTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.DataplaneTaskId == "" {
		return nil
	}

	_, err := t.scheduler.CancelTask(ctx, t.state.DataplaneTaskId)
	if err != nil {
		return err
	}

	return t.scheduler.WaitTaskEnded(ctx, t.state.DataplaneTaskId)
}

func (t *exportImageTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	if t.state.Response != nil {
		return &api.ExportMetadata{Progress: 1}, nil
	}

	if t.state.DataplaneTaskId == "" {
		return &api.ExportMetadata{}, nil
	}

	return t.scheduler.GetTaskMetadata(ctx, t.state.DataplaneTaskId)
}

func (t *exportImageTask) GetResponse() proto.Message {
	if t.state.Response == nil {
		return &api.ExportResponse{}
	}

	return t.state.Response
}
