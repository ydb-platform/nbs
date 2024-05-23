package metrics

import (
	"sort"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

var (
	_ DurationBuckets = (*durationBuckets)(nil)
	_ Buckets         = (*buckets)(nil)
)

type durationBuckets struct {
	buckets []time.Duration
}

func NewDurationBuckets(bk ...time.Duration) DurationBuckets {
	sort.Slice(bk, func(i, j int) bool {
		return bk[i] < bk[j]
	})
	return durationBuckets{buckets: bk}
}

func (d durationBuckets) Size() int {
	return len(d.buckets)
}

func (d durationBuckets) MapDuration(dv time.Duration) int {
	idx := 0
	for _, bound := range d.buckets {
		if dv < bound {
			break
		}
		idx++
	}
	return idx
}

func (d durationBuckets) UpperBound(idx int) time.Duration {
	return d.buckets[idx]
}

type buckets struct {
	buckets []float64
}

// Returns new Buckets implementation.
func NewBuckets(bk ...float64) Buckets {
	sort.Slice(bk, func(i, j int) bool {
		return bk[i] < bk[j]
	})
	return buckets{buckets: bk}
}

func (d buckets) Size() int {
	return len(d.buckets)
}

func (d buckets) MapValue(v float64) int {
	idx := 0
	for _, bound := range d.buckets {
		if v < bound {
			break
		}
		idx++
	}
	return idx
}

func (d buckets) UpperBound(idx int) float64 {
	return d.buckets[idx]
}

func NewLinearBuckets(start, width float64, n int) Buckets {
	bounds := make([]float64, n)
	for i := range bounds {
		bounds[i] = start + (float64(i) * width)
	}
	return NewBuckets(bounds...)
}

func NewLinearDurationBuckets(start, width time.Duration, n int) DurationBuckets {
	buckets := make([]time.Duration, n)
	for i := range buckets {
		buckets[i] = start + (time.Duration(i) * width)
	}
	return NewDurationBuckets(buckets...)
}

func NewExponentialBuckets(start, factor float64, n int) Buckets {
	buckets := make([]float64, n)
	curr := start
	for i := range buckets {
		buckets[i] = curr
		curr *= factor
	}
	return NewBuckets(buckets...)
}

func NewExponentialDurationBuckets(
	start time.Duration,
	factor float64,
	n int,
) DurationBuckets {

	buckets := make([]time.Duration, n)
	curr := start
	for i := range buckets {
		buckets[i] = curr
		curr = time.Duration(float64(curr) * factor)
	}
	return NewDurationBuckets(buckets...)
}
