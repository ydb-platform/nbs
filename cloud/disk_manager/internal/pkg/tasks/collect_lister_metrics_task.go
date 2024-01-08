package tasks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type collectListerMetricsTask struct {
	registry                  metrics.Registry
	storage                   storage.Storage
	metricsCollectionInterval time.Duration
}

func (c collectListerMetricsTask) Save() ([]byte, error) {
	return nil, nil
}

func (c collectListerMetricsTask) Load(request []byte, state []byte) error {
	return nil
}

func (c collectListerMetricsTask) Run(
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
				return c.storage.ListTasksReadyToCancel(
					ctx,        // excludingHostname
					^uint64(0), // limit
					nil,
				)
			},
			storage.TaskStatusToString(storage.TaskStatusReadyToCancel),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c collectListerMetricsTask) Cancel(
	ctx context.Context,
	execCtx ExecutionContext,
) error {

	return nil
}

func (c collectListerMetricsTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (c collectListerMetricsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (c collectListerMetricsTask) collectTasksMetrics(
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
