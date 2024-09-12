package common

import (
	"context"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////

type Cond struct {
	cond *sync.Cond
}

func NewCond(l sync.Locker) Cond {
	return Cond{
		cond: sync.NewCond(l),
	}
}

func (c *Cond) Signal() {
	c.cond.Signal()
}

func (c *Cond) Broadcast() {
	c.cond.Broadcast()
}

// Waits for internal condvar event or for ctx cancellation.
func (c *Cond) Wait(ctx context.Context) error {
	waitFinishedCtx, waitFinished := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case <-ctx.Done():
			c.cond.Broadcast()
		case <-waitFinishedCtx.Done():
			return
		}

		for {
			select {
			case <-waitFinishedCtx.Done():
				return
			default:
			}

			// Signal until c.cond.Wait() has finished for sure.
			c.cond.Signal()
		}
	}()

	c.cond.Wait()
	waitFinished()
	wg.Wait()

	return ctx.Err()
}
