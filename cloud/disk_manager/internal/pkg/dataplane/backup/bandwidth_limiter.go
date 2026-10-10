package backup

import (
	"context"
	"math"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

// Token bucket over bytes. One instance per process caps what the node sends
// to the follower whatever the number of copy tasks on it. A nil limiter and
// a zero rate let everything through.
//
// availableBytes grows at addAvailableBytesPerSecond up to maxAvailableBytes,
// a second of bandwidth: an idle node may send at most that much at once. It
// goes negative when callers reserve more than is available: later callers
// wait behind them. A request larger than the bucket waits in proportion.
type BandwidthLimiter struct {
	addAvailableBytesPerSecond float64
	maxAvailableBytes          float64
	now                        func() time.Time

	// Shared by every copy goroutine of the process.
	mutex          sync.Mutex
	availableBytes float64
	refilledAt     time.Time
}

func NewBandwidthLimiter(addAvailableBytesPerSecond uint64) *BandwidthLimiter {
	return newBandwidthLimiter(addAvailableBytesPerSecond, time.Now)
}

func newBandwidthLimiter(
	addAvailableBytesPerSecond uint64,
	now func() time.Time,
) *BandwidthLimiter {

	if addAvailableBytesPerSecond == 0 {
		return nil
	}

	return &BandwidthLimiter{
		addAvailableBytesPerSecond: float64(addAvailableBytesPerSecond),
		maxAvailableBytes:          float64(addAvailableBytesPerSecond),
		now:                        now,
		availableBytes:             float64(addAvailableBytesPerSecond),
		refilledAt:                 now(),
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
			l.maxAvailableBytes,
			l.availableBytes+elapsed*l.addAvailableBytesPerSecond,
		)
		l.refilledAt = now
	}

	l.availableBytes -= float64(bytes)
	if l.availableBytes >= 0 {
		return 0
	}

	return time.Duration(
		-l.availableBytes / l.addAvailableBytesPerSecond * float64(time.Second),
	)
}
