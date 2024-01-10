package persistence

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func s3CallDurationBuckets() metrics.DurationBuckets {
	return metrics.NewExponentialDurationBuckets(
		time.Millisecond,
		1.25,
		50,
	)
}

////////////////////////////////////////////////////////////////////////////////

type s3Metrics struct {
	registry    metrics.Registry
	callTimeout time.Duration
}

func (m *s3Metrics) StatCall(
	ctx context.Context,
	name string,
) func(err *error) {

	start := time.Now()

	return func(err *error) {
		subRegistry := m.registry.WithTags(map[string]string{
			"call": name,
		})

		// Should initialize all counters before using them, to avoid 'no data'.
		errorCounter := subRegistry.Counter("errors")
		successCounter := subRegistry.Counter("success")
		hangingCounter := subRegistry.Counter("hanging")
		timeoutCounter := subRegistry.Counter("timeout")
		timeHistogram := subRegistry.DurationHistogram("time", s3CallDurationBuckets())

		if time.Since(start) >= m.callTimeout {
			logging.Error(ctx, "S3 call hanging, name %v", name)
			hangingCounter.Inc()
		}

		if *err != nil {
			errorCounter.Inc()

			if errors.Is(*err, context.DeadlineExceeded) {
				logging.Error(ctx, "S3 call timed out, name %v", name)
				timeoutCounter.Inc()
			}
			return
		}

		timeHistogram.RecordDuration(time.Since(start))
		successCounter.Inc()
	}
}

func newS3Metrics(registry metrics.Registry, callTimeout time.Duration) *s3Metrics {
	return &s3Metrics{
		registry:    registry,
		callTimeout: callTimeout,
	}
}
