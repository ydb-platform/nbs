package metrics

import (
	"net/http"
	"time"

	"github.com/ydb-platform/nbs/library/go/core/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type NewRegistryFunc = func(mux *http.ServeMux, path string) Registry

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

type Buckets = metrics.Buckets

type DurationBuckets = metrics.DurationBuckets

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
