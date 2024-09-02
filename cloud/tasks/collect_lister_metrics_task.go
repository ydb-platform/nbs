package tasks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type collectListerMetricsTask struct {
	registry                    metrics.Registry
	storage                     storage.Storage
	hangingTasksGaugesByID      map[string]metrics.Gauge
	hangingTasksGaugesByType    map[string]metrics.Gauge
	metricsCollectionInterval   time.Duration
	hangingTasksDefaultDuration time.Duration
	exceptHangingTaskTypes      []string
	maxReportedHangingTaskIDs   int64
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

	ticker := time.NewTicker(c.metricsCollectionInterval)
	defer ticker.Stop()

	for range ticker.C {
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
		c.hangingTasksDefaultDuration,
	)
	if err != nil {
		return err
	}

	sensor := "hangingTasks"
	reportedTasksIDsCounter := int64(0)
	newHangingsTasksGaugesByID := make(map[string]metrics.Gauge)
	newHangingTasksGaugesByType := make(map[string]metrics.Gauge)

	for _, taskInfo := range taskInfos {
		tasksByType[taskInfo.TaskType]++
		if reportedTasksIDsCounter < c.maxReportedHangingTaskIDs {
			subRegistry := c.registry.WithTags(
				map[string]string{
					"type": taskInfo.TaskType, "task_id": taskInfo.ID,
				},
			)
			hangingTasksGauge := subRegistry.Gauge(sensor)
			hangingTasksGauge.Set(float64(1))
			newHangingsTasksGaugesByID[taskInfo.ID] = hangingTasksGauge
			reportedTasksIDsCounter++
		}
	}

	for id, gauge := range c.hangingTasksGaugesByID {
		_, ok := newHangingsTasksGaugesByID[id]
		if !ok {
			gauge.Set(float64(0))
		}
	}

	c.hangingTasksGaugesByID = newHangingsTasksGaugesByID

	for taskType, count := range tasksByType {
		subRegistry := c.registry.WithTags(map[string]string{
			"type": taskType, "task_id": "all",
		})
		gauge := subRegistry.Gauge(sensor)
		gauge.Set(float64(count))
		newHangingTasksGaugesByType[taskType] = gauge
	}

	for taskType, gauge := range c.hangingTasksGaugesByType {
		_, ok := newHangingTasksGaugesByType[taskType]
		if !ok {
			gauge.Set(float64(0))
		}
	}

	c.hangingTasksGaugesByType = newHangingTasksGaugesByType
	return nil
}
