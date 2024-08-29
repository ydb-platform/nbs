package metrics

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type compoundRegistry struct {
	registries []metrics.Registry
}

func NewCompoundRegistry(registries []metrics.Registry) metrics.Registry {
	return &compoundRegistry{
		registries: registries,
	}
}

func (r *compoundRegistry) WithTags(tags map[string]string) metrics.Registry {
	var registries []metrics.Registry
	for _, registry := range r.registries {
		registries = append(registries, registry.WithTags(tags))
	}

	return &compoundRegistry{
		registries: registries,
	}
}

func (r *compoundRegistry) WithPrefix(prefix string) metrics.Registry {
	var registries []metrics.Registry
	for _, registry := range r.registries {
		registries = append(registries, registry.WithPrefix(prefix))
	}

	return &compoundRegistry{
		registries: registries,
	}
}

func (r *compoundRegistry) ComposeName(parts ...string) string {
	return r.registries[0].ComposeName(parts...)
}

func (r *compoundRegistry) Counter(name string) metrics.Counter {
	var counters []metrics.Counter
	for _, registry := range r.registries {
		counters = append(counters, registry.Counter(name))
	}

	return &compoundCounter{
		counters: counters,
	}
}

func (r *compoundRegistry) CounterVec(
	name string,
	labels []string,
) metrics.CounterVec {

	var counterVecs []metrics.CounterVec
	for _, registry := range r.registries {
		counterVecs = append(counterVecs, registry.CounterVec(name, labels))
	}

	return &compoundCounterVec{
		counterVecs: counterVecs,
	}
}

func (r *compoundRegistry) FuncCounter(
	name string,
	function func() int64,
) metrics.FuncCounter {

	var funcCounters []metrics.FuncCounter
	for _, registry := range r.registries {
		funcCounters = append(funcCounters, registry.FuncCounter(name, function))
	}

	return &compoundFuncCounter{
		funcCounters: funcCounters,
	}
}

func (r *compoundRegistry) Gauge(name string) metrics.Gauge {
	var gauges []metrics.Gauge
	for _, registry := range r.registries {
		gauges = append(gauges, registry.Gauge(name))
	}

	return &compoundGauge{
		gauges: gauges,
	}
}

func (r *compoundRegistry) GaugeVec(
	name string,
	labels []string,
) metrics.GaugeVec {

	var gaugeVecs []metrics.GaugeVec
	for _, registry := range r.registries {
		gaugeVecs = append(gaugeVecs, registry.GaugeVec(name, labels))
	}

	return &compoundGaugeVecs{
		gaugeVecs: gaugeVecs,
	}
}

func (r *compoundRegistry) FuncGauge(
	name string,
	function func() float64,
) metrics.FuncGauge {

	var funcGauges []metrics.FuncGauge
	for _, registry := range r.registries {
		funcGauges = append(funcGauges, registry.FuncGauge(name, function))
	}

	return &compoundFuncGauge{
		funcGauges: funcGauges,
	}
}

func (r *compoundRegistry) IntGauge(name string) metrics.IntGauge {
	var intGauges []metrics.IntGauge
	for _, registry := range r.registries {
		intGauges = append(intGauges, registry.IntGauge(name))
	}

	return &compoundIntGauge{
		intGauges: intGauges,
	}
}

func (r *compoundRegistry) IntGaugeVec(
	name string,
	labels []string,
) metrics.IntGaugeVec {

	var intGaugeVecs []metrics.IntGaugeVec
	for _, registry := range r.registries {
		intGaugeVecs = append(
			intGaugeVecs,
			registry.IntGaugeVec(name, labels),
		)
	}

	return &compoundIntGaugeVec{
		intGaugeVecs: intGaugeVecs,
	}
}

func (r *compoundRegistry) FuncIntGauge(
	name string,
	function func() int64,
) metrics.FuncIntGauge {

	var funcIntGauges []metrics.FuncIntGauge
	for _, registry := range r.registries {
		funcIntGauges = append(
			funcIntGauges,
			registry.FuncIntGauge(name, function),
		)
	}

	return &compoundFuncIntGauge{
		funcIntGauges: funcIntGauges,
	}
}

func (r *compoundRegistry) Timer(name string) metrics.Timer {
	var timers []metrics.Timer
	for _, registry := range r.registries {
		timers = append(timers, registry.Timer(name))
	}

	return &compoundTimer{
		timers: timers,
	}
}

func (r *compoundRegistry) TimerVec(
	name string,
	labels []string,
) metrics.TimerVec {

	var timerVecs []metrics.TimerVec
	for _, registry := range r.registries {
		timerVecs = append(timerVecs, registry.TimerVec(name, labels))
	}

	return &compoundTimerVec{
		timerVecs: timerVecs,
	}
}

func (r *compoundRegistry) Histogram(
	name string,
	buckets metrics.Buckets,
) metrics.Histogram {

	var histograms []metrics.Histogram
	for _, registry := range r.registries {
		histograms = append(histograms, registry.Histogram(name, buckets))
	}

	return &compoundHistogram{
		histograms: histograms,
	}
}

func (r *compoundRegistry) HistogramVec(
	name string,
	buckets metrics.Buckets,
	labels []string,
) metrics.HistogramVec {

	var histogramVecs []metrics.HistogramVec
	for _, registry := range r.registries {
		histogramVecs = append(
			histogramVecs,
			registry.HistogramVec(name, buckets, labels),
		)
	}

	return &compoundHistogramVec{
		histogramVecs: histogramVecs,
	}
}

func (r *compoundRegistry) DurationHistogram(
	name string,
	buckets metrics.DurationBuckets,
) metrics.Timer {

	var timers []metrics.Timer
	for _, registry := range r.registries {
		timers = append(timers, registry.DurationHistogram(name, buckets))
	}

	return &compoundTimer{
		timers: timers,
	}
}

func (r *compoundRegistry) DurationHistogramVec(
	name string,
	buckets metrics.DurationBuckets,
	labels []string,
) metrics.TimerVec {

	var durationHistogramVecs []metrics.TimerVec
	for _, registry := range r.registries {
		durationHistogramVecs = append(
			durationHistogramVecs,
			registry.DurationHistogramVec(name, buckets, labels),
		)
	}

	return &compoundTimerVec{
		timerVecs: durationHistogramVecs,
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundGauge struct {
	gauges []metrics.Gauge
}

func (g *compoundGauge) Set(value float64) {
	for _, gauge := range g.gauges {
		gauge.Set(value)
	}
}

func (g *compoundGauge) Add(value float64) {
	for _, gauge := range g.gauges {
		gauge.Add(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundFuncGauge struct {
	funcGauges []metrics.FuncGauge
}

func (g *compoundFuncGauge) Function() func() float64 {
	return g.funcGauges[0].Function()
}

////////////////////////////////////////////////////////////////////////////////

type compoundIntGauge struct {
	intGauges []metrics.IntGauge
}

func (g *compoundIntGauge) Set(value int64) {
	for _, gauge := range g.intGauges {
		gauge.Set(value)
	}
}

func (g *compoundIntGauge) Add(value int64) {
	for _, gauge := range g.intGauges {
		gauge.Add(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundFuncIntGauge struct {
	funcIntGauges []metrics.FuncIntGauge
}

func (g *compoundFuncIntGauge) Function() func() int64 {
	return g.funcIntGauges[0].Function()
}

////////////////////////////////////////////////////////////////////////////////

type compoundCounter struct {
	counters []metrics.Counter
}

func (c *compoundCounter) Inc() {
	for _, counter := range c.counters {
		counter.Inc()
	}
}

func (c *compoundCounter) Add(delta int64) {
	for _, counter := range c.counters {
		counter.Add(delta)
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundFuncCounter struct {
	funcCounters []metrics.FuncCounter
}

func (c *compoundFuncCounter) Function() func() int64 {
	return c.funcCounters[0].Function()
}

////////////////////////////////////////////////////////////////////////////////

type compoundHistogram struct {
	histograms []metrics.Histogram
}

func (h *compoundHistogram) RecordValue(value float64) {
	for _, histogram := range h.histograms {
		histogram.RecordValue(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundTimer struct {
	timers []metrics.Timer
}

func (t *compoundTimer) RecordDuration(value time.Duration) {
	for _, timer := range t.timers {
		timer.RecordDuration(value)
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundGaugeVecs struct {
	gaugeVecs []metrics.GaugeVec
}

func (v *compoundGaugeVecs) With(kv map[string]string) metrics.Gauge {
	var gaugeVecs []metrics.Gauge
	for _, gaugeVec := range v.gaugeVecs {
		gaugeVecs = append(gaugeVecs, gaugeVec.With(kv))
	}

	return &compoundGauge{
		gauges: gaugeVecs,
	}
}

func (v *compoundGaugeVecs) Reset() {
	for _, gaugeVec := range v.gaugeVecs {
		gaugeVec.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundIntGaugeVec struct {
	intGaugeVecs []metrics.IntGaugeVec
}

func (v *compoundIntGaugeVec) With(kv map[string]string) metrics.IntGauge {
	var intGaugeVecs []metrics.IntGauge
	for _, intGaugeVec := range v.intGaugeVecs {
		intGaugeVecs = append(intGaugeVecs, intGaugeVec.With(kv))
	}

	return &compoundIntGauge{
		intGauges: intGaugeVecs,
	}
}

func (v *compoundIntGaugeVec) Reset() {
	for _, intGaugeVec := range v.intGaugeVecs {
		intGaugeVec.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundCounterVec struct {
	counterVecs []metrics.CounterVec
}

func (v *compoundCounterVec) With(kv map[string]string) metrics.Counter {
	var counters []metrics.Counter
	for _, counter := range v.counterVecs {
		counters = append(counters, counter.With(kv))
	}

	return &compoundCounter{
		counters: counters,
	}
}

func (v *compoundCounterVec) Reset() {
	for _, counter := range v.counterVecs {
		counter.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundTimerVec struct {
	timerVecs []metrics.TimerVec
}

func (v *compoundTimerVec) With(kv map[string]string) metrics.Timer {
	var timers []metrics.Timer
	for _, counter := range v.timerVecs {
		timers = append(timers, counter.With(kv))
	}

	return &compoundTimer{
		timers: timers,
	}
}

func (v *compoundTimerVec) Reset() {
	for _, timerVec := range v.timerVecs {
		timerVec.Reset()
	}
}

////////////////////////////////////////////////////////////////////////////////

type compoundHistogramVec struct {
	histogramVecs []metrics.HistogramVec
}

func (v *compoundHistogramVec) With(kv map[string]string) metrics.Histogram {
	var histograms []metrics.Histogram
	for _, counter := range v.histogramVecs {
		histograms = append(histograms, counter.With(kv))
	}

	return &compoundHistogram{
		histograms: histograms,
	}
}

func (v *compoundHistogramVec) Reset() {
	for _, histogramV := range v.histogramVecs {
		histogramV.Reset()
	}
}
