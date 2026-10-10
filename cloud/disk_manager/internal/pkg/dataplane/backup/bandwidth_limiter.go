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
const minCapacityBytes = 4 << 20

////////////////////////////////////////////////////////////////////////////////

// Token bucket over bytes. One instance per process caps what the node sends
// to the follower whatever the number of copy tasks on it. A nil limiter and
// a zero rate let everything through.
//
// availableBytes grows at bytesPerSecond up to capacityBytes, so an idle node
// may send at most a second of bandwidth at once. It goes negative when
// callers reserve more than is available: later callers wait behind them.
type BandwidthLimiter struct {
	bytesPerSecond float64
	capacityBytes  float64
	now            func() time.Time

	// Shared by every copy goroutine of the process.
	mutex          sync.Mutex
	availableBytes float64
	refilledAt     time.Time
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

	capacityBytes := math.Max(float64(bytesPerSecond), minCapacityBytes)
	return &BandwidthLimiter{
		bytesPerSecond: float64(bytesPerSecond),
		capacityBytes:  capacityBytes,
		now:            now,
		availableBytes: capacityBytes,
		refilledAt:     now(),
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
// them.
func (l *BandwidthLimiter) reserve(bytes int) time.Duration {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := l.now()
	elapsed := now.Sub(l.refilledAt).Seconds()
	if elapsed > 0 {
		l.availableBytes = math.Min(
			l.capacityBytes,
			l.availableBytes+elapsed*l.bytesPerSecond,
		)
		l.refilledAt = now
	}

	l.availableBytes -= float64(bytes)
	if l.availableBytes >= 0 {
		return 0
	}

	return time.Duration(
		-l.availableBytes / l.bytesPerSecond * float64(time.Second),
	)
}
