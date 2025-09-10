package storage

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type storageMetrics interface {
	OnTaskCreated(state TaskState, taskCount int)
	OnTaskUpdated(ctx context.Context, state TaskState)
}

////////////////////////////////////////////////////////////////////////////////

type taskMetrics struct {
	created              metrics.Counter
	timeTotal            metrics.Timer
	timeInflight         metrics.Timer
	timeStalling         metrics.Timer
	timeWaiting          metrics.Timer
	inflightEstimateMiss metrics.Timer
	stallingEstimateMiss metrics.Timer
}

type storageMetricsImpl struct {
	registry metrics.Registry

	// By taskType
	taskMetrics       map[string]*taskMetrics
	tasksMetricsMutex sync.Mutex
}

func taskDurationBuckets() metrics.DurationBuckets {
	return metrics.NewDurationBuckets(
		5*time.Second, 10*time.Second, 30*time.Second,
		1*time.Minute, 2*time.Minute, 5*time.Minute,
		10*time.Minute, 30*time.Minute,
		1*time.Hour, 2*time.Hour, 5*time.Hour, 10*time.Hour,
	)
}

func (m *storageMetricsImpl) getOrNewMetrics(taskType string) *taskMetrics {
	m.tasksMetricsMutex.Lock()
	defer m.tasksMetricsMutex.Unlock()

	t, ok := m.taskMetrics[taskType]
	if !ok {
		subRegistry := m.registry.WithTags(map[string]string{
			"type": taskType,
		})

		t = &taskMetrics{
			created:              subRegistry.Counter("created"),
			timeTotal:            subRegistry.DurationHistogram("time/total", taskDurationBuckets()),
			timeInflight:         subRegistry.DurationHistogram("time/inflight", taskDurationBuckets()),
			timeStalling:         subRegistry.DurationHistogram("time/stalling", taskDurationBuckets()),
			timeWaiting:          subRegistry.DurationHistogram("time/waiting", taskDurationBuckets()),
			inflightEstimateMiss: subRegistry.DurationHistogram("time/estimateMiss", taskDurationBuckets()),
			stallingEstimateMiss: subRegistry.DurationHistogram("time/stallingEstimateMiss", taskDurationBuckets()),
		}
		m.taskMetrics[taskType] = t
	}
	return t
}

////////////////////////////////////////////////////////////////////////////////

func (m *storageMetricsImpl) OnTaskCreated(state TaskState, taskCount int) {
	metrics := m.getOrNewMetrics(state.TaskType)
	metrics.created.Add(int64(taskCount))
}

func (m *storageMetricsImpl) OnTaskUpdated(
	ctx context.Context,
	state TaskState,
) {

	metrics := m.getOrNewMetrics(state.TaskType)
	if state.Status == TaskStatusFinished {
		metrics.timeTotal.RecordDuration(state.EndedAt.Sub(state.CreatedAt))
		metrics.timeInflight.RecordDuration(state.InflightDuration)

		if state.StallingDuration > 0 {
			metrics.timeStalling.RecordDuration(state.StallingDuration)
		}

		if state.WaitingDuration > 0 {
			metrics.timeWaiting.RecordDuration(state.WaitingDuration)
		}

		// Check whether the task exceeded any of its estimated durations.

		if state.EstimatedInflightDuration > 0 &&
			state.EstimatedInflightDuration < state.InflightDuration {

			estimateMiss := state.InflightDuration - state.EstimatedInflightDuration
			metrics.inflightEstimateMiss.RecordDuration(estimateMiss)
			logging.Info(
				ctx,
				"Task %q missed its inflight estimate: started: %q, ended: %q, estimate: %q, actual: %q, miss: %q",
				state.ID,
				state.CreatedAt,
				state.EndedAt,
				state.EstimatedInflightDuration,
				state.InflightDuration,
				estimateMiss,
			)
		}

		if state.EstimatedStallingDuration > 0 &&
			state.EstimatedStallingDuration < state.StallingDuration {

			estimateMiss := state.StallingDuration - state.EstimatedStallingDuration
			metrics.stallingEstimateMiss.RecordDuration(estimateMiss)
			logging.Info(
				ctx,
				"Task %q missed its stalling estimate: started: %q, ended: %q, estimate: %q, actual: %q, miss: %q",
				state.ID,
				state.CreatedAt,
				state.EndedAt,
				state.EstimatedStallingDuration,
				state.StallingDuration,
				estimateMiss,
			)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func newStorageMetrics(registry metrics.Registry) storageMetrics {
	return &storageMetricsImpl{
		registry:    registry,
		taskMetrics: make(map[string]*taskMetrics),
	}
}
