package persistence

import (
	"context"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

// Spaces attempts evenly, at most attemptsPerSecond of them. Attempts wait
// in one line: the attempt at its head sleeps until the time of the next
// attempt, takes it, and lets the next one in. A cancelled attempt leaves
// the line and takes no time, so the attempts behind it move up.
type s3AttemptLimiter struct {
	interval time.Duration

	// Held by the attempt at the head of the line.
	turn chan struct{}
	// Time of the next attempt; only the holder of the turn reads or moves it.
	next time.Time
}

// Returns nil, which does not limit, for zero attemptsPerSecond.
func newS3AttemptLimiter(attemptsPerSecond uint64) *s3AttemptLimiter {
	if attemptsPerSecond == 0 {
		return nil
	}

	return &s3AttemptLimiter{
		interval: time.Second / time.Duration(attemptsPerSecond),
		turn:     make(chan struct{}, 1),
	}
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
	l.next = l.next.Add(l.interval)
	return nil
}
