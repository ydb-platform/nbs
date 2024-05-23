package empty

import "github.com/ydb-platform/nbs/cloud/tasks/metrics"

////////////////////////////////////////////////////////////////////////////////

var (
	_ metrics.IntGauge     = (*IntGauge)(nil)
	_ metrics.IntGaugeVec  = (*IntGaugeVec)(nil)
	_ metrics.FuncIntGauge = (*FuncIntGauge)(nil)
)

type IntGauge struct{}

func (IntGauge) Set(_ int64) {}

func (IntGauge) Add(_ int64) {}

type IntGaugeVec struct{}

func (IntGaugeVec) With(_ map[string]string) metrics.IntGauge {
	return IntGauge{}
}

func (IntGaugeVec) Reset() {}

type FuncIntGauge struct {
	function func() int64
}

func (g FuncIntGauge) Function() func() int64 {
	return g.function
}
