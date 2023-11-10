package metrics

import (
	"time"

	"github.com/ydb-platform/nbs/library/go/core/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Registry interface {
	metrics.Registry
}

type Counter interface {
	metrics.Counter
}

type CounterVec interface {
	metrics.CounterVec
}

type Gauge interface {
	metrics.Gauge
}

type GaugeVec interface {
	metrics.GaugeVec
}

type Histogram interface {
	metrics.Histogram
}

type HistogramVec interface {
	metrics.HistogramVec
}

type Timer interface {
	metrics.Timer
}

type TimerVec interface {
	metrics.TimerVec
}

type Buckets interface {
	metrics.Buckets
}

type DurationBuckets interface {
	metrics.DurationBuckets
}

type DelayedTimer struct {
	startTime time.Time
	timer     Timer
}

func StartDelayedTimer(timer Timer) DelayedTimer {
	return DelayedTimer{
		startTime: time.Now(),
		timer:     timer,
	}
}

func (t *DelayedTimer) Stop() {
	t.timer.RecordDuration(time.Since(t.startTime))
}
