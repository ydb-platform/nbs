package persistence

import (
	"context"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

// How the rate of an adaptive limiter moves, once per window: down by the
// factor after a window with a throttled attempt, up by the step after a
// window without one. Additive increase and multiplicative decrease, as in
// TCP: the hosts sharing a quota converge to equal shares of it.
const (
	attemptRateWindow            = time.Second
	attemptRateDecreaseFactor    = 0.8
	attemptRateIncreasePerWindow = 2.0
)

// Spaces attempts evenly, at most rate of them per second. Attempts wait in
// one line: the attempt at its head sleeps until the time of the next
// attempt, takes it, and lets the next one in. A cancelled attempt leaves
// the line and takes no time, so the attempts behind it move up.
//
// With minRate below maxRate the rate adapts to the throttling of the
// storage (429, SlowDown) between the two; otherwise it stays at maxRate.
type s3AttemptLimiter struct {
	// Held by the attempt at the head of the line.
	turn chan struct{}
	// Time of the next attempt; only the holder of the turn reads or moves it.
	next time.Time

	mutex      sync.Mutex
	rate       float64 // attempts per second
	minRate    float64
	maxRate    float64
	throttled  bool // an attempt was throttled since the last adjustment
	adjustedAt time.Time
	window     time.Duration
	onRate     func(rate float64)
}

// Returns nil, which does not limit, for zero maxAttemptsPerSecond. A zero
// minAttemptsPerSecond keeps the rate fixed at the maximum. An adaptive
// limiter starts at the minimum and climbs while the storage does not
// throttle.
func newS3AttemptLimiter(
	minAttemptsPerSecond uint64,
	maxAttemptsPerSecond uint64,
	onRate func(rate float64),
) *s3AttemptLimiter {

	if maxAttemptsPerSecond == 0 {
		return nil
	}

	if minAttemptsPerSecond == 0 || minAttemptsPerSecond > maxAttemptsPerSecond {
		minAttemptsPerSecond = maxAttemptsPerSecond
	}

	if onRate == nil {
		onRate = func(float64) {}
	}

	limiter := &s3AttemptLimiter{
		turn:       make(chan struct{}, 1),
		rate:       float64(minAttemptsPerSecond),
		minRate:    float64(minAttemptsPerSecond),
		maxRate:    float64(maxAttemptsPerSecond),
		adjustedAt: time.Now(),
		window:     attemptRateWindow,
		onRate:     onRate,
	}
	onRate(limiter.rate)
	return limiter
}

// Blocks until the attempt may be sent or the context is done.
func (l *s3AttemptLimiter) wait(ctx context.Context) error {
	select {
	case l.turn <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-l.turn }()

	if delay := time.Until(l.next); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-ctx.Done():
			// The time stays for the next attempt in line.
			return ctx.Err()
		}
	}

	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	l.next = l.next.Add(l.interval(now))
	return nil
}

// Records a throttled attempt: the rate goes down at the next adjustment.
func (l *s3AttemptLimiter) onThrottle() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.throttled = true
}

// Returns the current spacing of attempts, adjusting the rate once per
// window.
func (l *s3AttemptLimiter) interval(now time.Time) time.Duration {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.minRate < l.maxRate && now.Sub(l.adjustedAt) >= l.window {
		if l.throttled {
			l.rate *= attemptRateDecreaseFactor
		} else {
			l.rate += attemptRateIncreasePerWindow
		}

		l.rate = min(max(l.rate, l.minRate), l.maxRate)
		l.throttled = false
		l.adjustedAt = now
		l.onRate(l.rate)
	}

	return time.Duration(float64(time.Second) / l.rate)
}

func (l *s3AttemptLimiter) currentRate() float64 {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.rate
}
