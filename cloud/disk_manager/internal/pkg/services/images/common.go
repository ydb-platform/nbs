package images

import (
	"context"
	"time"

	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

func deleteImage(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	scheduler tasks.Scheduler,
	storage resources.Storage,
	poolService pools.Service,
	imageID string,
	operationCloudID string,
	operationFolderID string,
) error {

	selfTaskID := execCtx.GetTaskID()

	taskID, err := poolService.ImageDeleting(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_image_deleting"),
		&pools_protos.ImageDeletingRequest{
			ImageId: imageID,
		},
	)
	if err != nil {
		return err
	}

	_, err = scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	imageMeta, err := storage.DeleteImage(
		ctx,
		imageID,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if imageMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			imageID,
		)
	}

	// Hack for NBS-2225.
	if imageMeta.DeleteTaskID != selfTaskID {
		return scheduler.WaitTaskEnded(ctx, imageMeta.DeleteTaskID)
	}

	taskID, err = scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_delete"),
		"dataplane.DeleteSnapshot",
		"",
		&dataplane_protos.DeleteSnapshotRequest{
			SnapshotId: imageID,
		},
		operationCloudID,
		operationFolderID,
	)
	if err != nil {
		return err
	}

	_, err = scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return storage.ImageDeleted(ctx, imageID, time.Now())
}

////////////////////////////////////////////////////////////////////////////////

func configureImagePools(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	scheduler tasks.Scheduler,
	poolService pools.Service,
	imageID string,
	diskPools []*types.DiskPool,
) error {

	configurePoolTasks := make([]string, 0)

	for _, c := range diskPools {
		idempotencyKey := execCtx.GetTaskID() + "_" + imageID + "_" + c.ZoneId

		taskID, err := poolService.ConfigurePool(
			headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
			&pools_protos.ConfigurePoolRequest{
				ImageId:  imageID,
				ZoneId:   c.ZoneId,
				Capacity: c.Capacity,
			},
		)
		if err != nil {
			return err
		}

		configurePoolTasks = append(configurePoolTasks, taskID)
	}

	for _, taskID := range configurePoolTasks {
		_, err := scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			return err
		}
	}

	return nil
}
