package backup

import (
	"context"
	"math"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

// The bucket is never smaller than a chunk, otherwise a chunk larger than a
// second of bandwidth would never pass.
const minBurstBytes = 4 << 20

////////////////////////////////////////////////////////////////////////////////

// Token bucket over bytes. One instance per process caps what the node sends
// to the follower whatever the number of copy tasks on it. A nil limiter and
// a zero rate let everything through.
type BandwidthLimiter struct {
	bytesPerSecond float64
	burst          float64
	now            func() time.Time

	mutex  sync.Mutex
	tokens float64
	last   time.Time
}

func NewBandwidthLimiter(bytesPerSecond uint64) *BandwidthLimiter {
	return newBandwidthLimiter(bytesPerSecond, time.Now)
}

func newBandwidthLimiter(
	bytesPerSecond uint64,
	now func() time.Time,
) *BandwidthLimiter {

	if bytesPerSecond == 0 {
		return nil
	}

	burst := math.Max(float64(bytesPerSecond), minBurstBytes)
	return &BandwidthLimiter{
		bytesPerSecond: float64(bytesPerSecond),
		burst:          burst,
		now:            now,
		tokens:         burst,
		last:           now(),
	}
}

// Blocks until the bytes may be sent or the context is done. The bytes stay
// reserved on cancellation: the caller retries the same chunk soon.
func (l *BandwidthLimiter) Wait(ctx context.Context, bytes int) error {
	if l == nil {
		return nil
	}

	delay := l.reserve(bytes)
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Takes the bytes from the bucket and returns how long the caller waits for
// them. A negative balance is a reservation: later callers queue behind it.
func (l *BandwidthLimiter) reserve(bytes int) time.Duration {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := l.now()
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens = math.Min(l.burst, l.tokens+elapsed*l.bytesPerSecond)
		l.last = now
	}

	l.tokens -= float64(bytes)
	if l.tokens >= 0 {
		return 0
	}

	return time.Duration(-l.tokens / l.bytesPerSecond * float64(time.Second))
}
