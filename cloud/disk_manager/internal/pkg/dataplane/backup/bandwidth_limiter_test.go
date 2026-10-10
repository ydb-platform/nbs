package backup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

const testChunkSize = 4 << 20

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestLimiter(bytesPerSecond uint64) (*BandwidthLimiter, *fakeClock) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	limiter := newBandwidthLimiter(bytesPerSecond, testChunkSize, clock.Now)
	return limiter, clock
}

////////////////////////////////////////////////////////////////////////////////

func TestBandwidthLimiterWithoutLimitDoesNotWait(t *testing.T) {
	var nilLimiter *BandwidthLimiter
	require.NoError(t, nilLimiter.Wait(context.Background(), 1<<30))

	limiter := NewBandwidthLimiter(0, testChunkSize)
	require.NoError(t, limiter.Wait(context.Background(), 1<<30))
}

func TestBandwidthLimiterStartsWithFullBucket(t *testing.T) {
	limiter, _ := newTestLimiter(10 << 20)

	// One second of bandwidth goes through at once.
	require.Zero(t, limiter.reserve(10<<20))
	// The next byte waits for the bucket to refill.
	require.Equal(t, time.Second/10, limiter.reserve(1<<20))
}

func TestBandwidthLimiterRefills(t *testing.T) {
	limiter, clock := newTestLimiter(10 << 20)

	require.Zero(t, limiter.reserve(10<<20))
	clock.Advance(500 * time.Millisecond)
	require.Zero(t, limiter.reserve(5<<20))
	require.Equal(t, time.Second/10, limiter.reserve(1<<20))
}

func TestBandwidthLimiterDoesNotAccumulateIdleTime(t *testing.T) {
	limiter, clock := newTestLimiter(10 << 20)

	clock.Advance(time.Hour)
	require.Zero(t, limiter.reserve(10<<20))
	require.Equal(t, time.Second, limiter.reserve(10<<20))
}

func TestBandwidthLimiterQueuesReservations(t *testing.T) {
	limiter, _ := newTestLimiter(10 << 20)

	require.Zero(t, limiter.reserve(10<<20))
	require.Equal(t, time.Second, limiter.reserve(10<<20))
	require.Equal(t, 2*time.Second, limiter.reserve(10<<20))
}

func TestBandwidthLimiterBucketHoldsAtLeastOneChunk(t *testing.T) {
	// 1 MiB/s is less than a chunk: the bucket is still a chunk deep, so a
	// chunk passes and the next one waits a chunk's worth of time.
	limiter, _ := newTestLimiter(1 << 20)

	require.Zero(t, limiter.reserve(testChunkSize))
	require.Equal(t, 4*time.Second, limiter.reserve(testChunkSize))
}

func TestBandwidthLimiterWaitHonoursCancellation(t *testing.T) {
	limiter, _ := newTestLimiter(1 << 20)
	require.Zero(t, limiter.reserve(testChunkSize))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := limiter.Wait(ctx, testChunkSize)
	require.Equal(t, context.Canceled, err)
}

func TestBandwidthLimiterWaitReturnsWhenBytesAreAvailable(t *testing.T) {
	limiter := NewBandwidthLimiter(1<<30, testChunkSize)

	start := time.Now()
	require.NoError(t, limiter.Wait(context.Background(), testChunkSize))
	require.Less(t, time.Since(start), time.Second)
}
