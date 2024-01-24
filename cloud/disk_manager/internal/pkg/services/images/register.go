package images

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	images_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *images_config.ImagesConfig,
	performanceConfig *performance_config.PerformanceConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
	poolService pools.Service,
) error {

	deletedImageExpirationTimeout, err := time.ParseDuration(
		config.GetDeletedImageExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearDeletedImagesTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedImagesTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromURL", func() tasks.Task {
		return &createImageFromURLTask{
			performanceConfig: performanceConfig,
			scheduler:         taskScheduler,
			storage:           storage,
			poolService:       poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromImage", func() tasks.Task {
		return &createImageFromImageTask{
			performanceConfig: performanceConfig,
			scheduler:         taskScheduler,
			storage:           storage,
			poolService:       poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromSnapshot", func() tasks.Task {
		return &createImageFromSnapshotTask{
			performanceConfig: performanceConfig,
			scheduler:         taskScheduler,
			storage:           storage,
			poolService:       poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.CreateImageFromDisk", func() tasks.Task {
		return &createImageFromDiskTask{
			performanceConfig: performanceConfig,
			scheduler:         taskScheduler,
			storage:           storage,
			nbsFactory:        nbsFactory,
			poolService:       poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.DeleteImage", func() tasks.Task {
		return &deleteImageTask{
			scheduler:   taskScheduler,
			storage:     storage,
			poolService: poolService,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("images.ClearDeletedImages", func() tasks.Task {
		return &clearDeletedImagesTask{
			storage:           storage,
			expirationTimeout: deletedImageExpirationTimeout,
			limit:             int(config.GetClearDeletedImagesLimit()),
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"images.ClearDeletedImages",
		clearDeletedImagesTaskScheduleInterval,
		1,
	)

	return nil
}
