package metrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestNewDurationBuckets(t *testing.T) {
	buckets := []time.Duration{
		1 * time.Second,
		3 * time.Second,
		5 * time.Second,
	}
	bk := NewDurationBuckets(buckets...)

	expect := durationBuckets{
		buckets: []time.Duration{
			1 * time.Second,
			3 * time.Second,
			5 * time.Second,
		},
	}
	require.Equal(t, expect, bk)
}

func TestDurationBucketsMapDuration(t *testing.T) {
	bk := NewDurationBuckets([]time.Duration{
		1 * time.Second,
		3 * time.Second,
		5 * time.Second,
	}...)

	for i := 0; i <= bk.Size(); i++ {
		require.Equal(t, i, bk.MapDuration(time.Duration(i*2)*time.Second))
	}
}

func TestDurationBucketsSize(t *testing.T) {
	var buckets []time.Duration
	for i := 1; i < 3; i++ {
		buckets = append(buckets, time.Duration(i)*time.Second)
		bk := NewDurationBuckets(buckets...)
		require.Equal(t, i, bk.Size())
	}
}

func TestDurationBucketsUpperBound(t *testing.T) {
	bk := NewDurationBuckets([]time.Duration{
		1 * time.Second,
		2 * time.Second,
		3 * time.Second,
	}...)

	for i := 0; i < bk.Size()-1; i++ {
		require.Equal(t, time.Duration(i+1)*time.Second, bk.UpperBound(i))
	}
}

func TestNewBuckets(t *testing.T) {
	bk := NewBuckets(1, 3, 5)

	expect := buckets{
		buckets: []float64{1, 3, 5},
	}
	require.Equal(t, expect, bk)
}

func TestBucketsMapValue(t *testing.T) {
	bk := NewBuckets(1, 3, 5)

	for i := 0; i <= bk.Size(); i++ {
		require.Equal(t, i, bk.MapValue(float64(i*2)))
	}
}

func TestBucketsSize(t *testing.T) {
	var buckets []float64
	for i := 1; i < 3; i++ {
		buckets = append(buckets, float64(i))
		bk := NewBuckets(buckets...)
		require.Equal(t, i, bk.Size())
	}
}

func TestBucketsUpperBound(t *testing.T) {
	bk := NewBuckets(1, 2, 3)

	for i := 0; i < bk.Size()-1; i++ {
		require.Equal(t, float64(i+1), bk.UpperBound(i))
	}
}

func TestNewExponentialDurationBuckets(t *testing.T) {
	require.Equal(
		t,
		NewDurationBuckets(2*time.Second, 4*time.Second, 8*time.Second),
		NewExponentialDurationBuckets(2*time.Second, 2, 3),
	)
}
