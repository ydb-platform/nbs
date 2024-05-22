package metrics

import (
	"net/http"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type NewRegistryFunc = func(mux *http.ServeMux, path string) Registry

////////////////////////////////////////////////////////////////////////////////

type Gauge = metrics.Gauge
type FuncGauge = metrics.FuncGauge
type IntGauge = metrics.IntGauge
type FuncIntGauge = metrics.FuncIntGauge

type Counter = metrics.Counter
type FuncCounter = metrics.FuncCounter

type Histogram = metrics.Histogram
type Timer = metrics.Timer
type DurationBuckets = metrics.DurationBuckets
type Buckets = metrics.Buckets

type GaugeVec = metrics.GaugeVec
type IntGaugeVec = metrics.IntGaugeVec
type CounterVec = metrics.CounterVec
type TimerVec = metrics.TimerVec
type HistogramVec = metrics.HistogramVec

type Registry = metrics.Registry
