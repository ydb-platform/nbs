package common

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type ChannelWithCancellation struct {
	channel         chan uint32
	cancellationCtx context.Context
	cancel          func()
}

func (c *ChannelWithCancellation) Empty() bool {
	return c.channel == nil
}

// Used by the sender.
// Returns true if sending was successful, otherwise returns false.
func (c *ChannelWithCancellation) Send(
	ctx context.Context,
	value uint32,
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
func (c *ChannelWithCancellation) Receive(
	ctx context.Context,
) (uint32, bool, error) {

	select {
	case value, more := <-c.channel:
		return value, more, nil
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

// Used by the receiver to indicate that sending is no longer needed,
// afterwards Send will always return false.
func (c *ChannelWithCancellation) Cancel() {
	if c.cancel != nil {
		c.cancel()
	}
}

// Used by the sender to indicate that sending is finished.
func (c *ChannelWithCancellation) Close() {
	if c.channel != nil {
		close(c.channel)
	}
}

////////////////////////////////////////////////////////////////////////////////

func NewChannelWithCancellation(capacity int) ChannelWithCancellation {
	c := ChannelWithCancellation{
		channel: make(chan uint32, capacity),
	}
	c.cancellationCtx, c.cancel = context.WithCancel(context.Background())
	return c
}
