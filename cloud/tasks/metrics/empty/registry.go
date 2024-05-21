package empty

import (
	"context"
	"io"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

var (
	_ metrics.Registry = (*Registry)(nil)
)

type Registry struct{}

func (Registry) ComposeName(parts ...string) string {
	return ""
}

func (Registry) WithTags(_ map[string]string) metrics.Registry {
	return Registry{}
}

func (Registry) WithPrefix(_ string) metrics.Registry {
	return Registry{}
}

func (Registry) Counter(_ string) metrics.Counter {
	return Counter{}
}

func (Registry) FuncCounter(_ string, function func() int64) metrics.FuncCounter {
	return FuncCounter{function: function}
}

func (Registry) Gauge(_ string) metrics.Gauge {
	return Gauge{}
}

func (Registry) FuncGauge(_ string, function func() float64) metrics.FuncGauge {
	return FuncGauge{function: function}
}

func (Registry) IntGauge(_ string) metrics.IntGauge {
	return IntGauge{}
}

func (Registry) FuncIntGauge(_ string, function func() int64) metrics.FuncIntGauge {
	return FuncIntGauge{function: function}
}

func (Registry) Timer(_ string) metrics.Timer {
	return Timer{}
}

func (Registry) Histogram(_ string, _ metrics.Buckets) metrics.Histogram {
	return Histogram{}
}

func (Registry) DurationHistogram(_ string, _ metrics.DurationBuckets) metrics.Timer {
	return Histogram{}
}

func (Registry) CounterVec(_ string, _ []string) metrics.CounterVec {
	return CounterVec{}
}

func (Registry) GaugeVec(_ string, _ []string) metrics.GaugeVec {
	return GaugeVec{}
}

func (Registry) IntGaugeVec(_ string, _ []string) metrics.IntGaugeVec {
	return IntGaugeVec{}
}

func (Registry) TimerVec(_ string, _ []string) metrics.TimerVec {
	return TimerVec{}
}

func (Registry) HistogramVec(_ string, _ metrics.Buckets, _ []string) metrics.HistogramVec {
	return HistogramVec{}
}

func (Registry) DurationHistogramVec(_ string, _ metrics.DurationBuckets, _ []string) metrics.TimerVec {
	return DurationHistogramVec{}
}

func (Registry) Stream(_ context.Context, _ io.Writer) (int, error) {
	return 0, nil
}

func NewRegistry() metrics.Registry {
	return Registry{}
}
