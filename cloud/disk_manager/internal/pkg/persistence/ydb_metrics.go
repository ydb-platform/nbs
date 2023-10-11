package persistence

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type ydbMetrics struct {
	registry    metrics.Registry
	callTimeout time.Duration
}

func (m *ydbMetrics) StatCall(
	ctx context.Context,
	name string,
	query string,
) func(err *error) {

	start := time.Now()

	return func(err *error) {
		subRegistry := m.registry.WithTags(map[string]string{
			"call": name,
		})

		if time.Since(start) >= m.callTimeout {
			logging.Error(ctx, "YDB call hanging, name %v, query %v", name, query)
			subRegistry.Counter("hanging").Inc()
		}

		if *err != nil {
			subRegistry.Counter("errors").Inc()

			if errors.Is(*err, context.DeadlineExceeded) {
				logging.Error(ctx, "YDB call timed out, name %v, query %v", name, query)
				subRegistry.Counter("timeout").Inc()
			}
			return
		}

		subRegistry.Counter("success").Inc()
	}
}

////////////////////////////////////////////////////////////////////////////////

func newYDBMetrics(
	registry metrics.Registry,
	callTimeout time.Duration,
) *ydbMetrics {

	return &ydbMetrics{
		registry:    registry,
		callTimeout: callTimeout,
	}
}
