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
	registry                  metrics.Registry
	storage                   storage.Storage
	metricsCollectionInterval time.Duration

	hangingTaskGaugesByID     map[string]metrics.Gauge
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

	taskInfos, err := c.storage.ListHangingTasks(
		ctx,
		^uint64(0),
		c.exceptHangingTaskTypes,
	)
	if err != nil {
		return err
	}

	taskInfoByID := make(map[string]storage.TaskInfo)
	for _, taskInfo := range taskInfos {
		taskInfoByID[taskInfo.ID] = taskInfo
	}

	for id, gauge := range c.hangingTaskGaugesByID {
		_, ok := taskInfoByID[id]
		if !ok {
			logging.Info(
				ctx,
				"Task with id %s is not hanging anymore",
				id,
			)
			gauge.Set(0)
			delete(c.hangingTaskGaugesByID, id)
		}
	}

	reportedTaskIDCount := int64(len(c.hangingTaskGaugesByID))
	sensorName := "hangingTasks"
	for _, taskInfo := range taskInfos {
		_, ok := c.hangingTaskGaugesByID[taskInfo.ID]
		if ok {
			continue
		}

		if reportedTaskIDCount < c.maxHangingTaskIDsToReport {
			logging.Info(
				ctx,
				"Task type %s, id %s is hanging",
				taskInfo.TaskType,
				taskInfo.ID,
			)
			subRegistry := c.registry.WithTags(
				map[string]string{
					"type": taskInfo.TaskType, "id": taskInfo.ID,
				},
			)
			gauge := subRegistry.Gauge(sensorName)
			gauge.Set(float64(1))
			c.hangingTaskGaugesByID[taskInfo.ID] = gauge
			reportedTaskIDCount++
		}
	}

	return nil
}
