package disks

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	disks_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *disks_config.DisksConfig,
	resourceStorage resources.Storage,
	poolStorage storage.Storage,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	poolService pools.Service,
	nbsFactory nbs.Factory,
) error {

	deletedDiskExpirationTimeout, err := time.ParseDuration(
		config.GetDeletedDiskExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearDeletedDisksTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedDisksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.CreateEmptyDisk", func() tasks.Task {
		return &createEmptyDiskTask{
			storage:    resourceStorage,
			scheduler:  taskScheduler,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.CreateOverlayDisk", func() tasks.Task {
		return &createOverlayDiskTask{
			storage:     resourceStorage,
			scheduler:   taskScheduler,
			poolService: poolService,
			nbsFactory:  nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.CreateDiskFromImage", func() tasks.Task {
		return &createDiskFromImageTask{
			storage:    resourceStorage,
			scheduler:  taskScheduler,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.CreateDiskFromSnapshot", func() tasks.Task {
		return &createDiskFromSnapshotTask{
			storage:    resourceStorage,
			scheduler:  taskScheduler,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.DeleteDisk", func() tasks.Task {
		return &deleteDiskTask{
			storage:     resourceStorage,
			scheduler:   taskScheduler,
			poolService: poolService,
			nbsFactory:  nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.ResizeDisk", func() tasks.Task {
		return &resizeDiskTask{
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.AlterDisk", func() tasks.Task {
		return &alterDiskTask{
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.AssignDisk", func() tasks.Task {
		return &assignDiskTask{
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.UnassignDisk", func() tasks.Task {
		return &unassignDiskTask{
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.ClearDeletedDisks", func() tasks.Task {
		return &clearDeletedDisksTask{
			storage:           resourceStorage,
			expirationTimeout: deletedDiskExpirationTimeout,
			limit:             int(config.GetClearDeletedDisksLimit()),
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("disks.MigrateDisk", func() tasks.Task {
		return &migrateDiskTask{
			disksConfig:     config,
			scheduler:       taskScheduler,
			poolService:     poolService,
			resourceStorage: resourceStorage,
			poolStorage:     poolStorage,
			nbsFactory:      nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"disks.ClearDeletedDisks",
		tasks.TaskSchedule{
			ScheduleInterval: clearDeletedDisksTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
