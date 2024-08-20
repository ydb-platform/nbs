package persistence

import (
	"context"
	"strings"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_metrics "github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	ydb_trace "github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

////////////////////////////////////////////////////////////////////////////////

type ydbMetrics struct {
	registry    metrics.Registry
	callTimeout time.Duration
}

func (m *ydbMetrics) StatCall(
	ctx context.Context,
	name string,
	query string,
) func(err *error) {

	start := time.Now()

	return func(err *error) {
		subRegistry := m.registry.WithTags(map[string]string{
			"call": name,
		})

		// Should initialize all counters before using them, to avoid 'no data'.
		hangingCounter := subRegistry.Counter("hanging")
		timeoutCounter := subRegistry.Counter("timeout")
		successCounter := subRegistry.Counter("success")

		if time.Since(start) >= m.callTimeout {
			logging.Error(ctx, "YDB call hanging, name %v, query %v", name, query)
			hangingCounter.Inc()
		}

		if *err != nil {
			var errorType string
			errorRegistry := subRegistry
			switch {
			case ydb.IsOperationErrorTransactionLocksInvalidated(*err):
				errorType = "tli"
			case ydb.IsOperationErrorSchemeError(*err):
				errorType = "scheme"
			case ydb.IsTransportError(*err):
				errorType = "transport"
			case ydb.IsOperationErrorOverloaded(*err):
				errorType = "overloaded"
			case ydb.IsOperationErrorUnavailable(*err):
				errorType = "unavailable"
			case ydb.IsRatelimiterAcquireError(*err):
				errorType = "ratelimiterAcquire"
			}

			if errorType != "" {
				errorRegistry = errorRegistry.WithTags(map[string]string{
					"type": errorType,
				})
			}

			// Should initialize all counters before using them, to avoid 'no data'.
			errorCounter := errorRegistry.Counter("errors")
			errorCounter.Inc()

			if errors.Is(*err, context.DeadlineExceeded) {
				logging.Error(ctx, "YDB call timed out, name %v, query %v", name, query)
				timeoutCounter.Inc()
			}
			return
		}

		successCounter.Inc()
	}
}

////////////////////////////////////////////////////////////////////////////////

func newYDBMetrics(
	registry metrics.Registry,
	callTimeout time.Duration,
) *ydbMetrics {

	return &ydbMetrics{
		registry:    registry,
		callTimeout: callTimeout,
	}
}

////////////////////////////////////////////////////////////////////////////////

type buckets []float64

func (b buckets) Size() int {
	return len(b)
}

func (b buckets) MapValue(v float64) int {
	idx := 0

	for _, bound := range b {
		if v < bound {
			break
		}

		idx++
	}

	return idx
}

func (b buckets) UpperBound(idx int) float64 {
	return b[idx]
}

////////////////////////////////////////////////////////////////////////////////

type metricsOption func(*metricsConfig)

type metricsConfig struct {
	details         ydb_trace.Details
	registry        metrics.Registry
	namespace       string
	durationBuckets metrics.DurationBuckets
}

func newMetricsConfig(
	registry metrics.Registry,
	opts ...metricsOption,
) *metricsConfig {

	durationBuckets := metrics.NewExponentialDurationBuckets(
		time.Millisecond,
		1.25,
		50,
	)
	c := &metricsConfig{
		registry:        registry,
		details:         ydb_trace.DetailsAll,
		namespace:       "ydb_go_sdk",
		durationBuckets: durationBuckets,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

////////////////////////////////////////////////////////////////////////////////

func (c *metricsConfig) CounterVec(
	name string,
	labelNames ...string,
) ydb_metrics.CounterVec {

	name = join(c.namespace, name)
	return &counterVec{
		v: c.registry.CounterVec(
			name,
			append([]string{}, labelNames...),
		),
	}
}

func (c *metricsConfig) GaugeVec(
	name string,
	labelNames ...string,
) ydb_metrics.GaugeVec {

	name = join(c.namespace, name)
	return &gaugeVec{
		v: c.registry.GaugeVec(
			name,
			append([]string{}, labelNames...),
		),
	}
}

func (c *metricsConfig) TimerVec(
	name string,
	labelNames ...string,
) ydb_metrics.TimerVec {

	name = join(c.namespace, name)
	return &timerVec{
		v: c.registry.DurationHistogramVec(
			name,
			c.durationBuckets,
			append([]string{}, labelNames...),
		),
	}
}

func (c *metricsConfig) HistogramVec(
	name string,
	bb []float64,
	labelNames ...string,
) ydb_metrics.HistogramVec {

	name = join(c.namespace, name)
	return &histogramVec{
		v: c.registry.HistogramVec(
			name,
			buckets(bb),
			append([]string{}, labelNames...),
		),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (c *metricsConfig) Details() ydb_trace.Details {
	return c.details
}

func (c *metricsConfig) WithSystem(subsystem string) ydb_metrics.Config {
	return &metricsConfig{
		details:         c.details,
		registry:        c.registry,
		namespace:       join(c.namespace, subsystem),
		durationBuckets: c.durationBuckets,
	}
}

////////////////////////////////////////////////////////////////////////////////

func WithTraceDetails(details ydb_trace.Details) metricsOption {
	return func(c *metricsConfig) {
		c.details |= details
	}
}

func WithTraces(
	registry metrics.Registry,
	opts ...metricsOption,
) ydb.Option {

	return ydb_metrics.WithTraces(newMetricsConfig(registry, opts...))
}

////////////////////////////////////////////////////////////////////////////////

type counterVec struct {
	v metrics.CounterVec
}

func (v *counterVec) With(kv map[string]string) ydb_metrics.Counter {
	return v.v.With(kv)
}

////////////////////////////////////////////////////////////////////////////////

type gaugeVec struct {
	v metrics.GaugeVec
}

func (v *gaugeVec) With(kv map[string]string) ydb_metrics.Gauge {
	return v.v.With(kv)
}

////////////////////////////////////////////////////////////////////////////////

type timerVec struct {
	v metrics.TimerVec
}

func (v *timerVec) With(kv map[string]string) ydb_metrics.Timer {
	return &timer{v.v.With(kv)}
}

////////////////////////////////////////////////////////////////////////////////

type histogramVec struct {
	v metrics.HistogramVec
}

func (v *histogramVec) With(kv map[string]string) ydb_metrics.Histogram {
	return &histogram{v.v.With(kv)}
}

////////////////////////////////////////////////////////////////////////////////

type histogram struct {
	metrics.Histogram
}

func (h *histogram) Record(v float64) {
	h.RecordValue(v)
}

////////////////////////////////////////////////////////////////////////////////

type timer struct {
	metrics.Timer
}

func (t *timer) Record(d time.Duration) {
	t.RecordDuration(d)
}

////////////////////////////////////////////////////////////////////////////////

func join(a, b string) string {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return ""
	}

	return strings.Join([]string{a, b}, "/")
}
