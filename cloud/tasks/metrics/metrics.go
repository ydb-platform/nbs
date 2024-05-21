package metrics

import (
	"time"
)

////////////////////////////////////////////////////////////////////////////////

// Tracks single float64 value.
type Gauge interface {
	Set(value float64)
	Add(value float64)
}

// FuncGauge is Gauge with value provided by callback function.
type FuncGauge interface {
	Function() func() float64
}

// Tracks single int64 value.
type IntGauge interface {
	Set(value int64)
	Add(value int64)
}

// FuncIntGauge is IntGauge with value provided by callback function.
type FuncIntGauge interface {
	Function() func() int64
}

// Tracks monotonically increasing value.
type Counter interface {
	// Increments counter by 1.
	Inc()

	// Adds delta to the counter. Delta must be >=0.
	Add(delta int64)
}

// FuncCounter is Counter with value provided by callback function.
type FuncCounter interface {
	Function() func() int64
}

// Tracks distribution of value.
type Histogram interface {
	RecordValue(value float64)
}

// Measures durations.
type Timer interface {
	RecordDuration(value time.Duration)
}

// Defines buckets of the duration histogram.
type DurationBuckets interface {
	// Returns number of buckets.
	Size() int

	// Returns index of the bucket.
	// index is integer in range [0, Size()).
	MapDuration(d time.Duration) int

	// UpperBound of the last bucket is always +Inf.
	// bucketIndex is integer in range [0, Size()-1).
	UpperBound(bucketIndex int) time.Duration
}

// Defines intervals of the regular histogram.
type Buckets interface {
	// Returns number of buckets.
	Size() int

	// Returns index of the bucket.
	// Index is integer in range [0, Size()).
	MapValue(v float64) int

	// UpperBound of the last bucket is always +Inf.
	// bucketIndex is integer in range [0, Size()-1).
	UpperBound(bucketIndex int) float64
}

// Stores multiple dynamically created gauges.
type GaugeVec interface {
	With(map[string]string) Gauge

	// Deletes all metrics in vector.
	Reset()
}

// Stores multiple dynamically created gauges.
type IntGaugeVec interface {
	With(map[string]string) IntGauge

	// Deletes all metrics in vector.
	Reset()
}

// Stores multiple dynamically created counters.
type CounterVec interface {
	With(map[string]string) Counter

	// Deletes all metrics in vector.
	Reset()
}

// Stores multiple dynamically created timers.
type TimerVec interface {
	With(map[string]string) Timer

	// Deletes all metrics in vector.
	Reset()
}

// Stores multiple dynamically created histograms.
type HistogramVec interface {
	With(map[string]string) Histogram

	// Deletes all metrics in vector.
	Reset()
}

// Creates profiling metrics.
type Registry interface {
	// Creates new sub-scope, where each metric has tags attached to it.
	WithTags(tags map[string]string) Registry
	// Creates new sub-scope, where each metric has prefix added to its name.
	WithPrefix(prefix string) Registry

	ComposeName(parts ...string) string

	Counter(name string) Counter
	CounterVec(name string, labels []string) CounterVec
	FuncCounter(name string, function func() int64) FuncCounter

	Gauge(name string) Gauge
	GaugeVec(name string, labels []string) GaugeVec
	FuncGauge(name string, function func() float64) FuncGauge

	IntGauge(name string) IntGauge
	IntGaugeVec(name string, labels []string) IntGaugeVec
	FuncIntGauge(name string, function func() int64) FuncIntGauge

	Timer(name string) Timer
	TimerVec(name string, labels []string) TimerVec

	Histogram(name string, buckets Buckets) Histogram
	HistogramVec(name string, buckets Buckets, labels []string) HistogramVec

	DurationHistogram(name string, buckets DurationBuckets) Timer
	DurationHistogramVec(name string, buckets DurationBuckets, labels []string) TimerVec
}
