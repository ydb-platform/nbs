package metrics

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

func NewLinearBuckets(start float64, width float64, n int) Buckets {
	return metrics.NewLinearBuckets(start, width, n)
}

func NewExponentialBuckets(start float64, factor float64, n int) Buckets {
	return metrics.NewExponentialBuckets(start, factor, n)
}

func NewDurationBuckets(args ...time.Duration) DurationBuckets {
	return metrics.NewDurationBuckets(args...)
}

func NewExponentialDurationBuckets(
	start time.Duration,
	factor float64,
	n int,
) DurationBuckets {

	return metrics.NewExponentialDurationBuckets(start, factor, n)
}
