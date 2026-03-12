package util

import (
	"context"
	"errors"
	"math/rand"
	"time"

	tasks_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func WithNemesis(
	ctx context.Context,
	runFunc func(ctx context.Context) error,
	minDuration time.Duration,
	maxDuration time.Duration,
) error {

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	durationRange := maxDuration - minDuration

	for {
		runCtx, cancel := context.WithCancel(ctx)
		delay := minDuration + time.Duration(rng.Int63n(int64(durationRange)))
		logging.Info(
			runCtx,
			"task will be cancelled after %v",
			delay,
		)
		timer := time.AfterFunc(delay, cancel)

		err := runFunc(runCtx)

		timer.Stop()
		cancel()

		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			continue
		}

		if tasks_errors.Is(err, tasks_errors.NewEmptyRetriableError()) {
			continue
		}

		return err
	}
}
