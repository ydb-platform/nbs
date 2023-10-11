package common

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

const (
	saveProgressPeriod = time.Second
)

////////////////////////////////////////////////////////////////////////////////

func ProgressSaver(
	ctx context.Context,
	saveProgress func(context.Context) error,
) (func() error, <-chan error) {

	originalCtx := ctx
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)

	var resultErr error
	errors := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	var done atomic.Bool

	go func() {
		defer wg.Done()
		defer close(errors)
		defer func() {
			done.Store(true)
		}()

		for {
			select {
			case <-time.After(saveProgressPeriod):
			case <-ctx.Done():
				resultErr = ctx.Err()
				errors <- resultErr
				return
			}

			err := saveProgress(ctx)
			if err != nil {
				resultErr = err
				errors <- resultErr
				return
			}
		}
	}()

	return func() error {
		alreadyDone := done.Load()
		if alreadyDone {
			return resultErr
		}

		cancel()
		wg.Wait()
		return saveProgress(originalCtx)
	}, errors
}
