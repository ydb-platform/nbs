package tasks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type collectListerMetricsTask struct {
	registry metrics.Registry
	storage  storage.Storage

	metricsCollectionInterval time.Duration

	hangingTaskGaugesByID     map[string]metrics.Gauge
	hangingTaskGaugesByType   map[string]metrics.Gauge
	hangingTaskTimeout        time.Duration
	exceptHangingTaskTypes    []string
	maxHangingTaskIDsToReport int64
}

func (c *collectListerMetricsTask) Save() ([]byte, error) {
	return nil, nil
}

func (c *collectListerMetricsTask) Load(request []byte, state []byte) error {
	return nil
}

func (c *collectListerMetricsTask) Run(
	ctx context.Context,
	execCtx ExecutionContext,
) error {

	logging.Info(ctx, "Collect lister metrics task started")
	ticker := time.NewTicker(c.metricsCollectionInterval)
	defer ticker.Stop()

	for range ticker.C {
		logging.Info(ctx, "Collect lister metrics iteration started")
		err := c.collectTasksMetrics(
			ctx,
			func(context.Context) ([]storage.TaskInfo, error) {
				return c.storage.ListTasksReadyToRun(
					ctx,
					^uint64(0), // limit
					nil,
				)
			},
			storage.TaskStatusToString(storage.TaskStatusReadyToRun),
		)
		if err != nil {
			return err
		}

		err = c.collectTasksMetrics(
			ctx,
			func(context.Context) ([]storage.TaskInfo, error) {
				return c.storage.ListTasksRunning(
					ctx,
					^uint64(0), // limit
				)
			},
			storage.TaskStatusToString(storage.TaskStatusRunning),
		)
		if err != nil {
			return err
		}

		err = c.collectTasksMetrics(
			ctx,
			func(context.Context) ([]storage.TaskInfo, error) {
				return c.storage.ListTasksReadyToCancel(
					ctx,
					^uint64(0), // limit
					nil,
				)
			},
			storage.TaskStatusToString(storage.TaskStatusReadyToCancel),
		)
		if err != nil {
			return err
		}

		err = c.collectTasksMetrics(
			ctx,
			func(context.Context) ([]storage.TaskInfo, error) {
				return c.storage.ListTasksCancelling(
					ctx,
					^uint64(0), // limit
				)
			},
			storage.TaskStatusToString(storage.TaskStatusCancelling),
		)
		if err != nil {
			return err
		}

		err = c.collectHangingTasksMetrics(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *collectListerMetricsTask) Cancel(
	ctx context.Context,
	execCtx ExecutionContext,
) error {

	return nil
}

func (c *collectListerMetricsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (c *collectListerMetricsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (c *collectListerMetricsTask) collectTasksMetrics(
	ctx context.Context,
	getTaskInfos func(context.Context) ([]storage.TaskInfo, error),
	sensor string,
) error {

	tasksByType := make(map[string]uint64)

	taskInfos, err := getTaskInfos(ctx)
	if err != nil {
		return err
	}

	for _, taskInfo := range taskInfos {
		tasksByType[taskInfo.TaskType]++
	}

	for taskType, count := range tasksByType {
		subRegistry := c.registry.WithTags(map[string]string{
			"type": taskType,
		})
		subRegistry.Gauge(sensor).Set(float64(count))
	}

	return nil
}

func (c *collectListerMetricsTask) collectHangingTasksMetrics(
	ctx context.Context,
) error {

	tasksByType := make(map[string]uint64)

	taskInfos, err := c.storage.ListTasksHanging(
		ctx,
		^uint64(0),
		c.exceptHangingTaskTypes,
		c.hangingTaskTimeout,
	)
	if err != nil {
		return err
	}

	reportedTaskIDCount := int64(0)
	newHangingsTaskGaugesByID := make(map[string]metrics.Gauge)
	newHangingTaskGaugesByType := make(map[string]metrics.Gauge)

	for _, taskInfo := range taskInfos {
		tasksByType[taskInfo.TaskType]++
		if reportedTaskIDCount < c.maxHangingTaskIDsToReport {
			logging.Info(ctx, "Task %s is hanging.", taskInfo.ID)
			subRegistry := c.registry.WithTags(
				map[string]string{
					"type": taskInfo.TaskType, "id": taskInfo.ID,
				},
			)
			gauge := subRegistry.Gauge("hangingTasks")
			gauge.Set(float64(1))
			newHangingsTaskGaugesByID[taskInfo.ID] = gauge
			reportedTaskIDCount++
		}
	}

	for id, gauge := range c.hangingTaskGaugesByID {
		_, ok := newHangingsTaskGaugesByID[id]
		if !ok {
			logging.Info(
				ctx,
				"Tasks with id %s is not hanging no more.",
				id,
			)
			gauge.Set(float64(0))
		}
	}

	c.hangingTaskGaugesByID = newHangingsTaskGaugesByID

	for taskType, count := range tasksByType {
		subRegistry := c.registry.WithTags(map[string]string{
			"type": taskType, "id": "all",
		})
		gauge := subRegistry.Gauge("hangingTasks")
		gauge.Set(float64(count))
		newHangingTaskGaugesByType[taskType] = gauge
	}

	for taskType, gauge := range c.hangingTaskGaugesByType {
		_, ok := newHangingTaskGaugesByType[taskType]
		if !ok {
			logging.Info(
				ctx,
				"Tasks with type %s is not hanging no more.",
				taskType,
			)
			gauge.Set(float64(0))
		}
	}

	c.hangingTaskGaugesByType = newHangingTaskGaugesByType
	return nil
}
