package common

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

func scheduleCreateDRBasedDiskCheckpointTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	disk *types.Disk,
	snapshotID string,
	selfTaskID string,
) (string, error) {

	return scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_create_checkpoint"),
		"dataplane.CreateDRBasedDiskCheckpoint",
		"Create checkpoint for snapshot/image "+snapshotID,
		&dataplane_protos.CreateDRBasedDiskCheckpointRequest{
			Disk:               disk,
			CheckpointIdPrefix: snapshotID,
		},
	)
}

func CreateCheckpoint(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	scheduler tasks.Scheduler,
	nbsClient nbs.Client,
	disk *types.Disk,
	snapshotID string,
	selfTaskID string,
	isDiskRegistryBasedDisk bool,
) (string, error) {

	if !isDiskRegistryBasedDisk {
		checkpointID := snapshotID

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

	taskID, err := scheduleCreateDRBasedDiskCheckpointTask(
		ctx,
		scheduler,
		disk,
		snapshotID,
		selfTaskID,
	)
	if err != nil {
		return "", err
	}

	response, err := scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return "", err
	}

	typedResponse, ok := response.(*dataplane_protos.CreateDRBasedDiskCheckpointResponse)
	if !ok {
		return "", errors.NewNonRetriableErrorf(
			"invalid dataplane.CreateDRBasedDiskCheckpoint response type %T",
			response,
		)
	}

	return typedResponse.CheckpointId, nil
}

func CancelCheckpointCreation(
	ctx context.Context,
	scheduler tasks.Scheduler,
	nbsClient nbs.Client,
	disk *types.Disk,
	snapshotID string,
	selfTaskID string,
) (string, error) {

	diskParams, err := nbsClient.Describe(ctx, snapshotID)
	if err != nil {
		if nbs.IsNotFoundError(err) {
			// Nothing to do.
			return "", nil
		}
		return "", err
	}

	if !diskParams.IsDiskRegistryBasedDisk {
		return snapshotID, nil
	}

	checkpointTaskID, err := scheduleCreateDRBasedDiskCheckpointTask(
		ctx,
		scheduler,
		disk,
		snapshotID,
		selfTaskID,
	)
	if err != nil {
		return "", err
	}

	_, err = scheduler.CancelTask(ctx, checkpointTaskID)
	if err != nil {
		return "", err
	}

	err = scheduler.WaitTaskEnded(ctx, checkpointTaskID)
	if err != nil {
		return "", err
	}

	metadata, err := scheduler.GetTaskMetadata(ctx, checkpointTaskID)
	if err != nil {
		return "", err
	}

	typedMetadata, ok := metadata.(*dataplane_protos.CreateDRBasedDiskCheckpointMetadata)
	if !ok {
		return "", errors.NewNonRetriableErrorf(
			"invalid dataplane.CreateDRBasedDiskCheckpoint metadata type %T",
			metadata,
		)
	}

	return typedMetadata.CheckpointId, nil
}
