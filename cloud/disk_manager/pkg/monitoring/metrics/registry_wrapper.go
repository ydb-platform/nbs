package metrics

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	core_metrics "github.com/ydb-platform/nbs/library/go/core/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Registry = metrics.Registry

// registryWrapper makes core_metrics.Registry conform with metrics.Registry.
// It is needed because Golang's duck typing is not recursive.
//
// Consider following example:
//
//	type A interface {
//	  Foo() int
//	}
//
//	type B interface {
//	  Foo() int
//	}
//
//	type C interface {
//	  Bar() A
//	}
//
//	type D interface {
//	  Bar() B
//	}
//
// Instance of A is equivalent to instance of B.
// But instance of C is not equivalent to instance of D.
type registryWrapper struct {
	registry core_metrics.Registry
}

func WrapRegistry(registry core_metrics.Registry) metrics.Registry {
	return &registryWrapper{
		registry: registry,
	}
}

func (r *registryWrapper) WithTags(tags map[string]string) metrics.Registry {
	return &registryWrapper{
		registry: r.registry.WithTags(tags),
	}
}

func (r *registryWrapper) WithPrefix(prefix string) metrics.Registry {
	return &registryWrapper{
		registry: r.registry.WithPrefix(prefix),
	}
}

func (r *registryWrapper) ComposeName(parts ...string) string {
	return r.registry.ComposeName(parts...)
}

func (r *registryWrapper) Counter(name string) metrics.Counter {
	return &counter{
		counter: r.registry.Counter(name),
	}
}

func (r *registryWrapper) CounterVec(
	name string,
	labels []string,
) metrics.CounterVec {

	return &counterVec{
		counterVec: r.registry.CounterVec(name, labels),
	}
}

func (r *registryWrapper) FuncCounter(
	name string,
	function func() int64,
) metrics.FuncCounter {

	return &funcCounter{
		funcCounter: r.registry.FuncCounter(name, function),
	}
}

func (r *registryWrapper) Gauge(name string) metrics.Gauge {
	return &gauge{
		gauge: r.registry.Gauge(name),
	}
}

func (r *registryWrapper) GaugeVec(
	name string,
	labels []string,
) metrics.GaugeVec {

	return &gaugeVec{
		gaugeVec: r.registry.GaugeVec(name, labels),
	}
}

func (r *registryWrapper) FuncGauge(
	name string,
	function func() float64,
) metrics.FuncGauge {

	return &funcGauge{
		funcGauge: r.registry.FuncGauge(name, function),
	}
}

func (r *registryWrapper) IntGauge(name string) metrics.IntGauge {
	return &intGauge{
		intGauge: r.registry.IntGauge(name),
	}
}

func (r *registryWrapper) IntGaugeVec(
	name string,
	labels []string,
) metrics.IntGaugeVec {

	return &intGaugeVec{
		intGaugeVec: r.registry.IntGaugeVec(name, labels),
	}
}

func (r *registryWrapper) FuncIntGauge(
	name string,
	function func() int64,
) metrics.FuncIntGauge {

	return &funcIntGauge{
		funcIntGauge: r.registry.FuncIntGauge(name, function),
	}
}

func (r *registryWrapper) Timer(name string) metrics.Timer {
	return &timer{
		timer: r.registry.Timer(name),
	}
}

func (r *registryWrapper) TimerVec(
	name string,
	labels []string,
) metrics.TimerVec {

	return &timerVec{
		timerVec: r.registry.TimerVec(name, labels),
	}
}

func (r *registryWrapper) Histogram(
	name string,
	buckets metrics.Buckets,
) metrics.Histogram {

	return &histogram{
		histogram: r.registry.Histogram(name, buckets),
	}
}

func (r *registryWrapper) HistogramVec(
	name string,
	buckets metrics.Buckets,
	labels []string,
) metrics.HistogramVec {

	return &histogramVec{
		histogramVec: r.registry.HistogramVec(name, buckets, labels),
	}
}

func (r *registryWrapper) DurationHistogram(
	name string,
	buckets metrics.DurationBuckets,
) metrics.Timer {

	return &timer{
		timer: r.registry.DurationHistogram(name, buckets),
	}
}

func (r *registryWrapper) DurationHistogramVec(
	name string,
	buckets metrics.DurationBuckets,
	labels []string,
) metrics.TimerVec {

	return &timerVec{
		timerVec: r.registry.DurationHistogramVec(name, buckets, labels),
	}
}

////////////////////////////////////////////////////////////////////////////////

type gauge struct {
	gauge core_metrics.Gauge
}

func (g *gauge) Set(value float64) {
	g.gauge.Set(value)
}

func (g *gauge) Add(value float64) {
	g.gauge.Add(value)
}

////////////////////////////////////////////////////////////////////////////////

type funcGauge struct {
	funcGauge core_metrics.FuncGauge
}

func (g *funcGauge) Function() func() float64 {
	return g.funcGauge.Function()
}

////////////////////////////////////////////////////////////////////////////////

type intGauge struct {
	intGauge core_metrics.IntGauge
}

func (g *intGauge) Set(value int64) {
	g.intGauge.Set(value)
}

func (g *intGauge) Add(value int64) {
	g.intGauge.Add(value)
}

////////////////////////////////////////////////////////////////////////////////

type funcIntGauge struct {
	funcIntGauge core_metrics.FuncIntGauge
}

func (g *funcIntGauge) Function() func() int64 {
	return g.funcIntGauge.Function()
}

////////////////////////////////////////////////////////////////////////////////

type counter struct {
	counter core_metrics.Counter
}

func (c *counter) Inc() {
	c.counter.Inc()
}

func (c *counter) Add(delta int64) {
	c.counter.Add(delta)
}

////////////////////////////////////////////////////////////////////////////////

type funcCounter struct {
	funcCounter core_metrics.FuncCounter
}

func (c *funcCounter) Function() func() int64 {
	return c.funcCounter.Function()
}

////////////////////////////////////////////////////////////////////////////////

type histogram struct {
	histogram core_metrics.Histogram
}

func (h *histogram) RecordValue(value float64) {
	h.histogram.RecordValue(value)
}

////////////////////////////////////////////////////////////////////////////////

type timer struct {
	timer metrics.Timer
}

func (t *timer) RecordDuration(value time.Duration) {
	t.timer.RecordDuration(value)
}

////////////////////////////////////////////////////////////////////////////////

type durationBuckets struct {
	durationBuckets core_metrics.DurationBuckets
}

func (b *durationBuckets) Size() int {
	return b.durationBuckets.Size()
}

func (b *durationBuckets) MapDuration(d time.Duration) int {
	return b.durationBuckets.MapDuration(d)
}

func (b *durationBuckets) UpperBound(bucketIndex int) time.Duration {
	return b.durationBuckets.UpperBound(bucketIndex)
}

////////////////////////////////////////////////////////////////////////////////

type buckets struct {
	buckets core_metrics.Buckets
}

func (b *buckets) Size() int {
	return b.buckets.Size()
}

func (b *buckets) MapValue(v float64) int {
	return b.buckets.MapValue(v)
}

func (b *buckets) UpperBound(bucketIndex int) float64 {
	return b.buckets.UpperBound(bucketIndex)
}

////////////////////////////////////////////////////////////////////////////////

type gaugeVec struct {
	gaugeVec core_metrics.GaugeVec
}

func (v *gaugeVec) With(kv map[string]string) metrics.Gauge {
	return &gauge{
		gauge: v.gaugeVec.With(kv),
	}
}

func (v *gaugeVec) Reset() {
	v.gaugeVec.Reset()
}

////////////////////////////////////////////////////////////////////////////////

type intGaugeVec struct {
	intGaugeVec core_metrics.IntGaugeVec
}

func (v *intGaugeVec) With(kv map[string]string) metrics.IntGauge {
	return &intGauge{
		intGauge: v.intGaugeVec.With(kv),
	}
}

func (v *intGaugeVec) Reset() {
	v.intGaugeVec.Reset()
}

////////////////////////////////////////////////////////////////////////////////

type counterVec struct {
	counterVec core_metrics.CounterVec
}

func (v *counterVec) With(kv map[string]string) metrics.Counter {
	return &counter{
		counter: v.counterVec.With(kv),
	}
}

func (v *counterVec) Reset() {
	v.counterVec.Reset()
}

////////////////////////////////////////////////////////////////////////////////

type timerVec struct {
	timerVec core_metrics.TimerVec
}

func (v *timerVec) With(kv map[string]string) metrics.Timer {
	return &timer{
		timer: v.timerVec.With(kv),
	}
}

func (v *timerVec) Reset() {
	v.timerVec.Reset()
}

////////////////////////////////////////////////////////////////////////////////

type histogramVec struct {
	histogramVec core_metrics.HistogramVec
}

func (v *histogramVec) With(kv map[string]string) metrics.Histogram {
	return &histogram{
		histogram: v.histogramVec.With(kv),
	}
}

func (v *histogramVec) Reset() {
	v.histogramVec.Reset()
}
