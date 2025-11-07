package cells

import (
	"context"
	"time"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterForExecution(
	ctx context.Context,
	config *cells_config.CellsConfig,
	taskRegistry *tasks.Registry,
	taskScheduler tasks.Scheduler,
	storage storage.Storage,
	nbsFactory nbs.Factory,
	metricsRegistry metrics.Registry,
) error {

	collectClusterCapacityTaskScheduleInterval, err := time.ParseDuration(
		config.GetCollectClusterCapacityTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	clusterCapacityExpirationTimeout, err := time.ParseDuration(
		config.GetClusterCapacityExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution("cells.CollectClusterCapacity", func() tasks.Task {
		return &collectClusterCapacityTask{
			config:            config,
			storage:           storage,
			nbsFactory:        nbsFactory,
			expirationTimeout: clusterCapacityExpirationTimeout,
		}
	})
	if err != nil {
		return err
	}

	if config.GetScheduleCollectClusterCapacityTask() {
		taskScheduler.ScheduleRegularTasks(
			ctx,
			"cells.CollectClusterCapacity",
			tasks.TaskSchedule{
				ScheduleInterval: collectClusterCapacityTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)
	}

	collectCellsMetricsTaskScheduleInterval, err := time.ParseDuration(
		config.GetCollectCellsMetricsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	cellsMetricsCollectionInterval, err := time.ParseDuration(
		config.GetCellsMetricsCollectionInterval(),
	)
	if err != nil {
		return err
	}

	err = taskRegistry.RegisterForExecution(
		"cells.CollectCellsMetrics",
		func() tasks.Task {
			return &collectCellsMetricsTask{
				config:                    config,
				storage:                   storage,
				registry:                  metricsRegistry,
				metricsCollectionInterval: cellsMetricsCollectionInterval,
			}
		},
	)
	if err != nil {
		return err
	}

	if config.GetScheduleCollectClusterCapacityTask() {
		taskScheduler.ScheduleRegularTasks(
			ctx,
			"cells.CollectCellsMetrics",
			tasks.TaskSchedule{
				ScheduleInterval: collectCellsMetricsTaskScheduleInterval,
				MaxTasksInflight: 1,
			},
		)
	}

	return nil
}
