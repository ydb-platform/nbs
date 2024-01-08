package metrics

import (
	"time"

	"github.com/ydb-platform/nbs/library/go/core/metrics"
	"github.com/ydb-platform/nbs/library/go/core/metrics/nop"
)

////////////////////////////////////////////////////////////////////////////////

type Registry = metrics.Registry

type Counter = metrics.Counter

type Gauge = metrics.Gauge

type Histogram = metrics.Histogram

type Timer = metrics.Timer

type DurationBuckets = metrics.DurationBuckets

////////////////////////////////////////////////////////////////////////////////

func NewEmptyRegistry() Registry {
	return new(nop.Registry)
}

func NewDurationBuckets(args ...time.Duration) DurationBuckets {
	return metrics.NewDurationBuckets(args...)
}
