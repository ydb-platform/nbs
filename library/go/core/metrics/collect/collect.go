package collect

import (
	"context"

	"github.com/ydb-platform/nbs/library/go/core/metrics"
)

type Func func(ctx context.Context, r metrics.Registry, c metrics.CollectPolicy)
