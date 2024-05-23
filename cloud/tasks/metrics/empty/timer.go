package empty

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

var (
	_ metrics.Timer    = (*Timer)(nil)
	_ metrics.TimerVec = (*TimerVec)(nil)
)

type Timer struct{}

func (Timer) RecordDuration(_ time.Duration) {}

type TimerVec struct{}

func (TimerVec) With(_ map[string]string) metrics.Timer {
	return Timer{}
}

func (TimerVec) Reset() {}
