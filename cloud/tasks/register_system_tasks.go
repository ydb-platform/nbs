package tasks

import (
	"context"
	"time"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func RegisterSystemTasks(
	ctx context.Context,
	registry *Registry,
	storage tasks_storage.Storage,
	config *tasks_config.TasksConfig,
	tasksMetricsRegistry metrics.Registry,
	taskScheduler Scheduler,
) error {

	err := registry.RegisterForExecution("tasks.Blank", func() Task {
		return &blankTask{}
	})
	if err != nil {
		return err
	}

	endedTaskExpirationTimeout, err := time.ParseDuration(
		config.GetEndedTaskExpirationTimeout(),
	)
	if err != nil {
		return err
	}

	clearEndedTasksTaskScheduleInterval, err := time.ParseDuration(
		config.GetClearEndedTasksTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution("tasks.ClearEndedTasks", func() Task {
		return &clearEndedTasksTask{
			storage:           storage,
			expirationTimeout: endedTaskExpirationTimeout,
			limit:             int(config.GetClearEndedTasksLimit()),
		}
	})
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"tasks.ClearEndedTasks",
		TaskSchedule{
			ScheduleInterval: clearEndedTasksTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	listerMetricsCollectionInterval, err := time.ParseDuration(
		config.GetListerMetricsCollectionInterval(),
	)
	if err != nil {
		return err
	}

	collectListerMetricsTaskScheduleInterval, err := time.ParseDuration(
		config.GetCollectListerMetricsTaskScheduleInterval(),
	)
	if err != nil {
		return err
	}

	err = registry.RegisterForExecution(
		"tasks.CollectListerMetrics", func() Task {
			return &collectListerMetricsTask{
				registry:                  tasksMetricsRegistry,
				storage:                   storage,
				metricsCollectionInterval: listerMetricsCollectionInterval,

				taskTypes:                 registry.TaskTypes(),
				hangingTaskGaugesByID:     make(map[string]metrics.Gauge),
				maxHangingTaskIDsToReport: config.GetMaxHangingTaskIDsToReport(),
			}
		},
	)
	if err != nil {
		return err
	}

	taskScheduler.ScheduleRegularTasks(
		ctx,
		"tasks.CollectListerMetrics",
		TaskSchedule{
			ScheduleInterval: collectListerMetricsTaskScheduleInterval,
			MaxTasksInflight: 1,
		},
	)

	return nil
}
