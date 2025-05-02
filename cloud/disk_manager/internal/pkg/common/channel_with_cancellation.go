package common

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type ChannelWithCancellation[T any] struct {
	channel         chan T
	cancellationCtx context.Context
	cancel          func()
}

func (c *ChannelWithCancellation[T]) Empty() bool {
	return c.channel == nil
}

// Used by the sender.
// Returns true if sending was successful, otherwise returns false.
func (c *ChannelWithCancellation[T]) Send(
	ctx context.Context,
	value T,
) (bool, error) {

	select {
	case c.channel <- value:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.cancellationCtx.Done():
		return false, nil
	}
}

// Used by the receiver.
// Should not be called after Cancel.
func (c *ChannelWithCancellation[T]) Receive(
	ctx context.Context,
) (T, bool, error) {

	select {
	case value, more := <-c.channel:
		return value, more, nil
	case <-ctx.Done():
		var zero T
		return zero, false, ctx.Err()
	}
}

// Used by the receiver to indicate that sending is no longer needed,
// afterwards Send will always return false.
func (c *ChannelWithCancellation[T]) Cancel() {
	if c.cancel != nil {
		c.cancel()
	}
}

// Used by the sender to indicate that sending is finished.
func (c *ChannelWithCancellation[T]) Close() {
	if c.channel != nil {
		close(c.channel)
	}
}

////////////////////////////////////////////////////////////////////////////////

func NewChannelWithCancellation[T any](capacity int) ChannelWithCancellation[T] {
	c := ChannelWithCancellation[T]{
		channel: make(chan T, capacity),
	}
	c.cancellationCtx, c.cancel = context.WithCancel(context.Background())
	return c
}
