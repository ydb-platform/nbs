package pools

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *config.PoolsConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage storage.Storage,
	nbsFactory nbs.Factory,
	resourceStorage resources.Storage,
) error {

	scheduleBaseDisksTaskScheduleInterval, err := time.ParseDuration(
		config.GetScheduleBaseDisksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	optimizeBaseDisksTaskScheduleInterval, err := time.ParseDuration(
		config.GetOptimizeBaseDisksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	minOptimizedPoolAge, err := time.ParseDuration(
		config.GetMinOptimizedPoolAge(),
	)
	if err != nil {
		return err
	}

	deleteBaseDisksTaskScheduleInterval, err := time.ParseDuration(
		config.GetDeleteBaseDisksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	deletedBaseDiskExpirationTimeout, err := time.ParseDuration(
		config.GetDeletedBaseDiskExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearDeletedBaseDisksTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedBaseDisksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	releasedSlotExpirationTimeout, err := time.ParseDuration(
		config.GetReleasedSlotExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearReleasedSlotsTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearReleasedSlotsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.AcquireBaseDisk", func() tasks.Task {
		return &acquireBaseDiskTask{
			scheduler: taskScheduler,
			storage:   storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.CreateBaseDisk", func() tasks.Task {
		return &createBaseDiskTask{
			cloudID:  config.GetCloudId(),
			folderID: config.GetFolderId(),

			scheduler:       taskScheduler,
			storage:         storage,
			nbsFactory:      nbsFactory,
			resourceStorage: resourceStorage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.ReleaseBaseDisk", func() tasks.Task {
		return &releaseBaseDiskTask{
			storage: storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.RebaseOverlayDisk", func() tasks.Task {
		return &rebaseOverlayDiskTask{
			storage:    storage,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.ConfigurePool", func() tasks.Task {
		return &configurePoolTask{
			storage:         storage,
			resourceStorage: resourceStorage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.DeletePool", func() tasks.Task {
		return &deletePoolTask{
			storage: storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.ImageDeleting", func() tasks.Task {
		return &imageDeletingTask{
			storage: storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.RetireBaseDisk", func() tasks.Task {
		return &retireBaseDiskTask{
			scheduler:  taskScheduler,
			nbsFactory: nbsFactory,
			storage:    storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.RetireBaseDisks", func() tasks.Task {
		return &retireBaseDisksTask{
			scheduler: taskScheduler,
			storage:   storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.ScheduleBaseDisks", func() tasks.Task {
		return &scheduleBaseDisksTask{
			scheduler: taskScheduler,
			storage:   storage,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.OptimizeBaseDisks", func() tasks.Task {
		return &optimizeBaseDisksTask{
			scheduler:                               taskScheduler,
			storage:                                 storage,
			convertToImageSizedBaseDisksThreshold:   config.GetConvertToImageSizedBaseDiskThreshold(),
			convertToDefaultSizedBaseDisksThreshold: config.GetConvertToDefaultSizedBaseDiskThreshold(),
			minPoolAge:                              minOptimizedPoolAge,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.DeleteBaseDisks", func() tasks.Task {
		return &deleteBaseDisksTask{
			storage:    storage,
			nbsFactory: nbsFactory,
			limit:      int(config.GetDeleteBaseDisksLimit()),
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.ClearDeletedBaseDisks", func() tasks.Task {
		return &clearDeletedBaseDisksTask{
			storage:           storage,
			expirationTimeout: deletedBaseDiskExpirationTimeout,
			limit:             int(config.GetClearDeletedBaseDisksLimit()),
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("pools.ClearReleasedSlots", func() tasks.Task {
		return &clearReleasedSlotsTask{
			storage:           storage,
			expirationTimeout: releasedSlotExpirationTimeout,
			limit:             int(config.GetClearReleasedSlotsLimit()),
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"pools.ScheduleBaseDisks",
		tasks.TaskSchedule{
			ScheduleInterval: scheduleBaseDisksTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	if config.GetRegularBaseDiskOptimizationEnabled() {
		taskScheduler.ScheduleRegularTasks(
			ctx,
			"pools.OptimizeBaseDisks",
			tasks.TaskSchedule{
				ScheduleInterval: optimizeBaseDisksTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"pools.DeleteBaseDisks",
		tasks.TaskSchedule{
			ScheduleInterval: deleteBaseDisksTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"pools.ClearDeletedBaseDisks",
		tasks.TaskSchedule{
			ScheduleInterval: clearDeletedBaseDisksTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"pools.ClearReleasedSlots",
		tasks.TaskSchedule{
			ScheduleInterval: clearReleasedSlotsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
