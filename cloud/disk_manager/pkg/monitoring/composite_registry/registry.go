package composite_registry

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	core_metrics "github.com/ydb-platform/nbs/library/go/core/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type compositeRegistry struct {
	registries []core_metrics.Registry
}

func WrapRegistry(registries []core_metrics.Registry) metrics.Registry {
	return &compositeRegistry{
		registries: registries,
	}
}

func (r *compositeRegistry) WithTags(tags map[string]string) metrics.Registry {
	newRegistries := make([]core_metrics.Registry, len(r.registries))
	for _, registry := range r.registries {
		newRegistries = append(newRegistries, registry.WithTags(tags))
	}
	return &compositeRegistry{
		registries: newRegistries,
	}
}

func (r *compositeRegistry) WithPrefix(prefix string) metrics.Registry {
	newRegistries := make([]core_metrics.Registry, len(r.registries))
	for _, registry := range r.registries {
		newRegistries = append(newRegistries, registry.WithPrefix(prefix))
	}
	return &compositeRegistry{
		registries: newRegistries,
	}
}

func (r *compositeRegistry) ComposeName(parts ...string) string {
	return r.registries[0].ComposeName(parts...)
}

func (r *compositeRegistry) Counter(name string) metrics.Counter {
	counters := make([]core_metrics.Counter, len(r.registries))
	for _, registry := range r.registries {
		counters = append(counters, registry.Counter(name))
	}
	return &composedCounter{
		counters: counters,
	}
}

func (r *compositeRegistry) CounterVec(
	name string,
	labels []string,
) metrics.CounterVec {
	counterVecs := make([]core_metrics.CounterVec, len(r.registries))
	for _, registry := range r.registries {
		counterVecs = append(counterVecs, registry.CounterVec(name, labels))
	}
	return &composedCounterVec{
		counterVecs: counterVecs,
	}
}

func (r *compositeRegistry) FuncCounter(
	name string,
	function func() int64,
) metrics.FuncCounter {
	funcCounters := make([]core_metrics.FuncCounter, len(r.registries))
	for _, registry := range r.registries {
		funcCounters = append(funcCounters, registry.FuncCounter(name, function))
	}
	return &composedFuncCounter{
		funcCounters: funcCounters,
	}
}

func (r *compositeRegistry) Gauge(name string) metrics.Gauge {
	gauges := make([]core_metrics.Gauge, len(r.registries))
	for _, registry := range r.registries {
		gauges = append(gauges, registry.Gauge(name))
	}
	return &composedGauge{
		gauges: gauges,
	}
}

func (r *compositeRegistry) GaugeVec(
	name string,
	labels []string,
) metrics.GaugeVec {
	gaugeVecsList := make([]core_metrics.GaugeVec, len(r.registries))
	for _, registry := range r.registries {
		gaugeVecsList = append(gaugeVecsList, registry.GaugeVec(name, labels))
	}
	return &composedGaugeVecs{
		gaugeVecs: gaugeVecsList,
	}
}

func (r *compositeRegistry) FuncGauge(
	name string,
	function func() float64,
) metrics.FuncGauge {
	funcGauges := make([]core_metrics.FuncGauge, len(r.registries))
	for _, registry := range r.registries {
		funcGauges = append(funcGauges, registry.FuncGauge(name, function))
	}
	return &composedFuncGauge{
		funcGauges: funcGauges,
	}
}

func (r *compositeRegistry) IntGauge(name string) metrics.IntGauge {
	intGauges := make([]core_metrics.IntGauge, len(r.registries))
	for _, registry := range r.registries {
		intGauges = append(intGauges, registry.IntGauge(name))
	}
	return &composedIntGauge{
		intGauges: intGauges,
	}
}

func (r *compositeRegistry) IntGaugeVec(
	name string,
	labels []string,
) metrics.IntGaugeVec {
	intGaugeVecsList := make([]core_metrics.IntGaugeVec, len(r.registries))
	for _, registry := range r.registries {
		intGaugeVecsList = append(intGaugeVecsList, registry.IntGaugeVec(name, labels))
	}
	return &composedIntGaugeVec{
		intGaugeVecs: intGaugeVecsList,
	}
}

func (r *compositeRegistry) FuncIntGauge(
	name string,
	function func() int64,
) metrics.FuncIntGauge {
	funcIntGauges := make([]core_metrics.FuncIntGauge, len(r.registries))
	for _, registry := range r.registries {
		funcIntGauges = append(funcIntGauges, registry.FuncIntGauge(name, function))
	}
	return &composedFuncIntGauge{
		funcIntGauges: funcIntGauges,
	}
}

func (r *compositeRegistry) Timer(name string) metrics.Timer {
	timers := make([]core_metrics.Timer, len(r.registries))
	for _, registry := range r.registries {
		timers = append(timers, registry.Timer(name))
	}
	return &composedTimer{
		timers: timers,
	}
}

func (r *compositeRegistry) TimerVec(
	name string,
	labels []string,
) metrics.TimerVec {
	timerVecsList := make([]core_metrics.TimerVec, len(r.registries))
	for _, registry := range r.registries {
		timerVecsList = append(timerVecsList, registry.TimerVec(name, labels))
	}
	return &composedTimerVec{
		timerVecs: timerVecsList,
	}
}

func (r *compositeRegistry) Histogram(
	name string,
	buckets metrics.Buckets,
) metrics.Histogram {
	histograms := make([]core_metrics.Histogram, len(r.registries))
	for _, registry := range r.registries {
		histograms = append(histograms, registry.Histogram(name, buckets))
	}
	return &composedHistogram{
		histogramList: histograms,
	}
}

func (r *compositeRegistry) HistogramVec(
	name string,
	buckets metrics.Buckets,
	labels []string,
) metrics.HistogramVec {
	histogramVecsList := make([]core_metrics.HistogramVec, len(r.registries))
	for _, registry := range r.registries {
		histogramVecsList = append(
			histogramVecsList,
			registry.HistogramVec(name, buckets, labels),
		)
	}
	return &histogramVec{
		histogramVecs: histogramVecsList,
	}
}

func (r *compositeRegistry) DurationHistogram(
	name string,
	buckets metrics.DurationBuckets,
) metrics.Timer {
	timers := make([]core_metrics.Timer, len(r.registries))
	for _, registry := range r.registries {
		timers = append(timers, registry.DurationHistogram(name, buckets))
	}
	return &composedTimer{
		timers: timers,
	}
}

func (r *compositeRegistry) DurationHistogramVec(
	name string,
	buckets metrics.DurationBuckets,
	labels []string,
) metrics.TimerVec {
	durationHistogramVecsList := make([]core_metrics.TimerVec, len(r.registries))
	for _, registry := range r.registries {
		durationHistogramVecsList = append(
			durationHistogramVecsList,
			registry.DurationHistogramVec(name, buckets, labels),
		)
	}
	return &composedTimerVec{
		timerVecs: durationHistogramVecsList,
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedGauge struct {
	gauges []core_metrics.Gauge
}

func (g *composedGauge) Set(value float64) {
	for _, gauge := range g.gauges {
		gauge.Set(value)
	}
}

func (g *composedGauge) Add(value float64) {
	for _, gauge := range g.gauges {
		gauge.Add(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedFuncGauge struct {
	funcGauges []core_metrics.FuncGauge
}

func (g *composedFuncGauge) Function() func() float64 {
	return g.funcGauges[0].Function()
}

////////////////////////////////////////////////////////////////////////////////

type composedIntGauge struct {
	intGauges []core_metrics.IntGauge
}

func (g *composedIntGauge) Set(value int64) {
	for _, gauge := range g.intGauges {
		gauge.Set(value)
	}
}

func (g *composedIntGauge) Add(value int64) {
	for _, gauge := range g.intGauges {
		gauge.Add(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedFuncIntGauge struct {
	funcIntGauges []core_metrics.FuncIntGauge
}

func (g *composedFuncIntGauge) Function() func() int64 {
	return g.funcIntGauges[0].Function()
}

////////////////////////////////////////////////////////////////////////////////

type composedCounter struct {
	counters []core_metrics.Counter
}

func (c *composedCounter) Inc() {
	for _, counter := range c.counters {
		counter.Inc()
	}
}

func (c *composedCounter) Add(delta int64) {
	for _, counter := range c.counters {
		counter.Add(delta)
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedFuncCounter struct {
	funcCounters []core_metrics.FuncCounter
}

func (c *composedFuncCounter) Function() func() int64 {
	return c.funcCounters[0].Function()
}

////////////////////////////////////////////////////////////////////////////////

type composedHistogram struct {
	histogramList []core_metrics.Histogram
}

func (h *composedHistogram) RecordValue(value float64) {
	for _, histogram := range h.histogramList {
		histogram.RecordValue(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedTimer struct {
	timers []core_metrics.Timer
}

func (t *composedTimer) RecordDuration(value time.Duration) {
	for _, timer := range t.timers {
		timer.RecordDuration(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedGaugeVecs struct {
	gaugeVecs []core_metrics.GaugeVec
}

func (v *composedGaugeVecs) With(kv map[string]string) metrics.Gauge {
	gaugeVecs := make([]core_metrics.Gauge, len(v.gaugeVecs))
	for _, gaugeVec := range v.gaugeVecs {
		gaugeVecs = append(gaugeVecs, gaugeVec.With(kv))
	}
	return &composedGauge{
		gauges: gaugeVecs,
	}
}

func (v *composedGaugeVecs) Reset() {
	for _, gaugeVec := range v.gaugeVecs {
		gaugeVec.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedIntGaugeVec struct {
	intGaugeVecs []core_metrics.IntGaugeVec
}

func (v *composedIntGaugeVec) With(kv map[string]string) metrics.IntGauge {
	intGaugeVecs := make([]core_metrics.IntGauge, len(v.intGaugeVecs))
	for _, intGaugeVec := range v.intGaugeVecs {
		intGaugeVecs = append(intGaugeVecs, intGaugeVec.With(kv))
	}
	return &composedIntGauge{
		intGauges: intGaugeVecs,
	}
}

func (v *composedIntGaugeVec) Reset() {
	for _, intGaugeVec := range v.intGaugeVecs {
		intGaugeVec.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedCounterVec struct {
	counterVecs []core_metrics.CounterVec
}

func (v *composedCounterVec) With(kv map[string]string) metrics.Counter {
	counters := make([]core_metrics.Counter, len(v.counterVecs))
	for _, counter := range v.counterVecs {
		counters = append(counters, counter.With(kv))
	}
	return &composedCounter{
		counters: counters,
	}
}

func (v *composedCounterVec) Reset() {
	for _, counter := range v.counterVecs {
		counter.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type composedTimerVec struct {
	timerVecs []core_metrics.TimerVec
}

func (v *composedTimerVec) With(kv map[string]string) metrics.Timer {
	timers := make([]core_metrics.Timer, len(v.timerVecs))
	for _, counter := range v.timerVecs {
		timers = append(timers, counter.With(kv))
	}
	return &composedTimer{
		timers: timers,
	}
}

func (v *composedTimerVec) Reset() {
	for _, timerVec := range v.timerVecs {
		timerVec.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type histogramVec struct {
	histogramVecs []core_metrics.HistogramVec
}

func (v *histogramVec) With(kv map[string]string) metrics.Histogram {
	histograms := make([]core_metrics.Histogram, len(v.histogramVecs))
	for _, counter := range v.histogramVecs {
		histograms = append(histograms, counter.With(kv))
	}
	return &composedHistogram{
		histogramList: histograms,
	}
}

func (v *histogramVec) Reset() {
	for _, histogramV := range v.histogramVecs {
		histogramV.Reset()
	}
}
