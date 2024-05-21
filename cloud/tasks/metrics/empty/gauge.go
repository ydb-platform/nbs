package empty

import "github.com/ydb-platform/nbs/cloud/tasks/metrics"

////////////////////////////////////////////////////////////////////////////////

var (
	_ metrics.Gauge     = (*Gauge)(nil)
	_ metrics.GaugeVec  = (*GaugeVec)(nil)
	_ metrics.FuncGauge = (*FuncGauge)(nil)
)

type Gauge struct{}

func (Gauge) Set(_ float64) {}

func (Gauge) Add(_ float64) {}

type GaugeVec struct{}

func (GaugeVec) With(_ map[string]string) metrics.Gauge {
	return Gauge{}
}

func (GaugeVec) Reset() {}

type FuncGauge struct {
	function func() float64
}

func (g FuncGauge) Function() func() float64 {
	return g.function
}
