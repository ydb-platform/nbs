package empty

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

var (
	_ metrics.Histogram    = (*Histogram)(nil)
	_ metrics.Timer        = (*Histogram)(nil)
	_ metrics.HistogramVec = (*HistogramVec)(nil)
	_ metrics.TimerVec     = (*DurationHistogramVec)(nil)
)

type Histogram struct{}

func (Histogram) RecordValue(_ float64) {}

func (Histogram) RecordDuration(_ time.Duration) {}

type HistogramVec struct{}

func (HistogramVec) With(_ map[string]string) metrics.Histogram {
	return Histogram{}
}

func (HistogramVec) Reset() {}

type DurationHistogramVec struct{}

func (DurationHistogramVec) With(_ map[string]string) metrics.Timer {
	return Histogram{}
}

func (DurationHistogramVec) Reset() {}
