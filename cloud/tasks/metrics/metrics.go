package metrics

import (
	"time"

	"github.com/ydb-platform/nbs/library/go/core/metrics"
	"github.com/ydb-platform/nbs/library/go/core/metrics/nop"
)

////////////////////////////////////////////////////////////////////////////////

type Registry = metrics.Registry

type Counter = metrics.Counter

type CounterVec = metrics.CounterVec

type Gauge = metrics.Gauge

type GaugeVec = metrics.GaugeVec

type Histogram = metrics.Histogram

type HistogramVec = metrics.HistogramVec

type Timer = metrics.Timer

type TimerVec = metrics.TimerVec

type DurationBuckets = metrics.DurationBuckets

////////////////////////////////////////////////////////////////////////////////

func NewEmptyRegistry() Registry {
	return new(nop.Registry)
}

func NewDurationBuckets(args ...time.Duration) DurationBuckets {
	return metrics.NewDurationBuckets(args...)
}

func NewExponentialDurationBuckets(
	start time.Duration,
	factor float64,
	n int,
) DurationBuckets {

	return metrics.MakeExponentialDurationBuckets(start, factor, n)
}
