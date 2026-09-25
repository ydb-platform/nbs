package tasks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

type delayedGauge struct {
	value float64
}

func (g *delayedGauge) Set(v float64) {
	g.value = v
}

func (g *delayedGauge) Add(v float64) {
	g.value += v
}

////////////////////////////////////////////////////////////////////////////////

type delayedTimer struct {
	values []time.Duration
}

func (t *delayedTimer) RecordDuration(v time.Duration) {
	t.values = append(t.values, v)
}

////////////////////////////////////////////////////////////////////////////////

type delayedRegistry struct {
	metrics.Registry

	gauges map[string]*delayedGauge
	timers map[string]*delayedTimer
}

func (r *delayedRegistry) WithTags(map[string]string) metrics.Registry {
	return r
}

func (r *delayedRegistry) Gauge(name string) metrics.Gauge {
	if r.gauges[name] == nil {
		r.gauges[name] = &delayedGauge{}
	}

	return r.gauges[name]
}

func (r *delayedRegistry) DurationHistogram(
	name string,
	_ metrics.DurationBuckets,
) metrics.Timer {

	if r.timers[name] == nil {
		r.timers[name] = &delayedTimer{}
	}

	return r.timers[name]
}

////////////////////////////////////////////////////////////////////////////////

type delayedStatsStorage struct {
	storage.Storage
	stats    storage.DelayedTaskStats
	statsErr error
	listErr  error
}

func (s *delayedStatsStorage) GetDelayedTaskStats(
	context.Context,
	time.Time,
) (storage.DelayedTaskStats, error) {

	return s.stats, s.statsErr
}

func (s *delayedStatsStorage) ListTasksWithStatus(
	context.Context,
	string,
) ([]storage.TaskInfo, error) {
	return nil, s.listErr
}

////////////////////////////////////////////////////////////////////////////////

func TestDelayedQueueMetrics(t *testing.T) {
	r := &delayedRegistry{
		gauges: map[string]*delayedGauge{},
		timers: map[string]*delayedTimer{},
	}
	s := &delayedStatsStorage{
		stats: storage.DelayedTaskStats{
			Total:               10,
			Due:                 2,
			MaxOverdueSeconds:   5,
			TotalOverdueSeconds: 8,
		},
	}
	c := &collectListerMetricsTask{
		registry: r,
		storage:  s,
	}

	require.NoError(t, c.collectDelayedTasksMetrics(context.Background()))
	require.Equal(t, 10.0, r.gauges["delayedTasks"].value)
	require.Equal(t, 2.0, r.gauges["delayedTasksDue"].value)
	require.Equal(t, 5.0, r.gauges["delayedTaskMaxOverdueSeconds"].value)
	require.Equal(t, 4.0, r.gauges["delayedTaskAvgOverdueSeconds"].value)
	require.Equal(t, 1.0, r.gauges["delayedTaskStatsValid"].value)

	// An empty queue must clear the values from the previous collection.
	s.stats = storage.DelayedTaskStats{}
	require.NoError(t, c.collectDelayedTasksMetrics(context.Background()))
	for _, name := range []string{
		"delayedTasks", "delayedTasksDue",
		"delayedTaskMaxOverdueSeconds", "delayedTaskAvgOverdueSeconds",
	} {
		require.Zero(t, r.gauges[name].value)
	}
	require.Equal(t, 1.0, r.gauges["delayedTaskStatsValid"].value)
}

func TestDelayedQueueMetricsStayStaleOnCollectionError(t *testing.T) {
	r := &delayedRegistry{gauges: map[string]*delayedGauge{}}
	s := &delayedStatsStorage{stats: storage.DelayedTaskStats{Total: 10, Due: 2}}
	c := &collectListerMetricsTask{registry: r, storage: s}

	require.NoError(t, c.collectDelayedTasksMetrics(context.Background()))
	s.statsErr = errors.New("unavailable")
	require.Error(t, c.collectDelayedTasksMetrics(context.Background()))
	require.Equal(t, 10.0, r.gauges["delayedTasks"].value)
	require.Equal(t, 2.0, r.gauges["delayedTasksDue"].value)
	require.Zero(t, r.gauges["delayedTaskStatsValid"].value)
}

func TestDelayedQueueMetricsStayStaleOnEarlierListerError(t *testing.T) {
	r := &delayedRegistry{gauges: map[string]*delayedGauge{}}
	s := &delayedStatsStorage{
		stats:   storage.DelayedTaskStats{Total: 10, Due: 2},
		listErr: errors.New("legacy folder unavailable"),
	}
	c := &collectListerMetricsTask{
		registry:                  r,
		storage:                   s,
		metricsCollectionInterval: time.Millisecond,
	}
	require.NoError(t, c.collectDelayedTasksMetrics(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.ErrorIs(t, c.Run(ctx, nil), s.listErr)
	require.Equal(t, 10.0, r.gauges["delayedTasks"].value)
	require.Equal(t, 2.0, r.gauges["delayedTasksDue"].value)
	require.Zero(t, r.gauges["delayedTaskStatsValid"].value)
}

func TestInitialRunDelayMetrics(t *testing.T) {
	r := &delayedRegistry{
		gauges: map[string]*delayedGauge{},
		timers: map[string]*delayedTimer{},
	}
	m := &runnerMetricsImpl{registry: r}

	now := time.Now()
	m.OnInitialRunStarted(storage.TaskState{
		TaskType:    "snapshot",
		ReceivedAt:  now,
		AvailableAt: now.Add(3 * time.Second),
	}, now.Add(5*time.Second))

	require.Equal(
		t,
		[]time.Duration{3 * time.Second},
		r.timers["initialRun/plannedDelay"].values,
	)
	require.Equal(
		t,
		[]time.Duration{5 * time.Second},
		r.timers["initialRun/actualDelay"].values,
	)
	require.Equal(
		t,
		[]time.Duration{2 * time.Second},
		r.timers["initialRun/queueDelay"].values,
	)
}
