package placementgroup

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	placement_group_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/placementgroup/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *placement_group_config.Config,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage resources.Storage,
	nbsFactory nbs.Factory,
) error {

	err := taskRegistry.RegisterForExecution("placement_group.CreatePlacementGroup", func() tasks.Task {
		return &createPlacementGroupTask{
			storage:    storage,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("placement_group.DeletePlacementGroup", func() tasks.Task {
		return &deletePlacementGroupTask{
			storage:    storage,
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("placement_group.AlterPlacementGroupMembership", func() tasks.Task {
		return &alterPlacementGroupMembershipTask{
			nbsFactory: nbsFactory,
		}
	})
	if err != nil {
		return err
	}

	deletedPlacementGroupExpirationTimeout, err :=
		time.ParseDuration(config.GetDeletedPlacementGroupExpirationTimeout())
	if err != nil {
		return err
	}

	clearDeletedPlacementGroupsTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearDeletedPlacementGroupsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	const gcTaskType = "placement_groups.ClearDeletedPlacementGroups"

	err = taskRegistry.RegisterForExecution(
		gcTaskType,
		func() tasks.Task {
			return &clearDeletedPlacementGroupsTask{
				storage:           storage,
				expirationTimeout: deletedPlacementGroupExpirationTimeout,
				limit:             int(config.GetClearDeletedPlacementGroupsLimit()),
			}
		},
	)
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		gcTaskType,
		clearDeletedPlacementGroupsTaskScheduleInterval,
		1,
	)

	return nil
}
