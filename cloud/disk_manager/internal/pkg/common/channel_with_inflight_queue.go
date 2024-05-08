package common

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type ChannelWithInflightQueue struct {
	inflightQueue *InflightQueue
	channel       chan uint32
}

func (c *ChannelWithInflightQueue) Empty() bool {
	return c.channel == nil
}

// Not thread-safe.
func (c *ChannelWithInflightQueue) Send(
	ctx context.Context,
	value uint32,
) (bool, error) {

	ok, err := c.inflightQueue.Add(ctx, value)
	if err != nil {
		return false, err
	}

	if ok {
		select {
		case c.channel <- value:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	return ok, nil
}

func (c *ChannelWithInflightQueue) Channel() <-chan uint32 {
	return c.channel
}

func (c *ChannelWithInflightQueue) Milestone() Milestone {
	return c.inflightQueue.Milestone()
}

func (c *ChannelWithInflightQueue) UpdateMilestoneHint(value uint32) {
	c.inflightQueue.UpdateMilestoneHint(value)
}

func (c *ChannelWithInflightQueue) Close() {
	c.inflightQueue.Close()
	if c.channel != nil {
		close(c.channel)
	}
}

////////////////////////////////////////////////////////////////////////////////

func NewChannelWithInflightQueue(
	milestone Milestone,
	processedValues <-chan uint32,
	holeValues ChannelWithCancellation,
	inflightLimit int,
) ChannelWithInflightQueue {

	inflightQueue := NewInflightQueue(
		milestone,
		processedValues,
		holeValues,
		inflightLimit,
	)
	return ChannelWithInflightQueue{
		inflightQueue: inflightQueue,
		channel:       make(chan uint32, inflightLimit),
	}
}
