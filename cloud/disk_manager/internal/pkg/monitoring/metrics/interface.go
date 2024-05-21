package metrics

import (
	"net/http"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
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
