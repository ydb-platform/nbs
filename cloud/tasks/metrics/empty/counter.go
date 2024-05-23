package empty

import "github.com/ydb-platform/nbs/cloud/tasks/metrics"

////////////////////////////////////////////////////////////////////////////////

var (
	_ metrics.Counter     = (*Counter)(nil)
	_ metrics.CounterVec  = (*CounterVec)(nil)
	_ metrics.FuncCounter = (*FuncCounter)(nil)
)

type Counter struct{}

func (Counter) Inc() {}

func (Counter) Add(_ int64) {}

type CounterVec struct{}

func (CounterVec) With(_ map[string]string) metrics.Counter {
	return Counter{}
}

func (CounterVec) Reset() {}

type FuncCounter struct {
	function func() int64
}

func (c FuncCounter) Function() func() int64 {
	return c.function
}
