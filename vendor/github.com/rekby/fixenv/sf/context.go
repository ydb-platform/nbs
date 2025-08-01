package sf

import (
	"context"
	"github.com/rekby/fixenv"
)

func Context(e fixenv.Env) context.Context {
	f := func() (*fixenv.Result, error) {
		ctx, ctxCancel := context.WithCancel(context.Background())
		return fixenv.NewResultWithCleanup(ctx, fixenv.FixtureCleanupFunc(ctxCancel)), nil
	}
	return e.CacheResult(f).(context.Context)
}
