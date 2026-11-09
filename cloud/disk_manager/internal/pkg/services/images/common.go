package images

import (
	"context"
	"time"

	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

func getDiskPoolConfigs(
	imageConfig *config.ImagesConfig,
	folderID string,
) []*config.DiskPoolConfig {

	defaults := imageConfig.GetDefaultDiskPoolConfigs()
	overrides := imageConfig.GetOverriddenDiskPoolConfigsByFolder()[folderID].GetConfigs()
	if len(overrides) == 0 {
		return defaults
	}

	configs := make([]*config.DiskPoolConfig, len(defaults), len(defaults)+len(overrides))
	copy(configs, defaults)
	zoneIndexes := make(map[string]int, len(configs))
	for i, c := range configs {
		zoneIndexes[c.GetZoneId()] = i
	}

	for _, c := range overrides {
		if i, ok := zoneIndexes[c.GetZoneId()]; ok {
			configs[i] = c
		} else {
			zoneIndexes[c.GetZoneId()] = len(configs)
			configs = append(configs, c)
		}
	}

	return configs
}

func deleteImage(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	config *config.ImagesConfig,
	scheduler tasks.Scheduler,
	storage resources.Storage,
	poolService pools.Service,
	imageID string,
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
		// Should be idempotent.
		return nil
	}

	err = scheduleRetireBaseDisks(
		ctx,
		execCtx,
		config,
		scheduler,
		*imageMeta,
	)
	if err != nil {
		return err
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

func scheduleRetireBaseDisks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	config *config.ImagesConfig,
	scheduler tasks.Scheduler,
	imageMeta resources.ImageMeta,
) error {

	for _, c := range getDiskPoolConfigs(config, imageMeta.FolderID) {
		_, err := scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				"retire_base_disks_"+execCtx.GetTaskID()+
					":"+c.GetZoneId()+":"+imageMeta.ID,
			),
			"pools.RetireBaseDisks",
			"",
			&pools_protos.RetireBaseDisksRequest{
				ImageId:          imageMeta.ID,
				ZoneId:           c.GetZoneId(),
				UseBaseDiskAsSrc: true,
				UseImageSize:     imageMeta.Size,
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
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
