package persistence

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/metrics"
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
	bucket string,
	key string,
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
			logging.Error(
				ctx,
				"S3 call with name %v hanging, bucket %v, key %v",
				name,
				bucket,
				key,
			)

			hangingCounter.Inc()
		}

		if *err != nil {
			logging.Error(
				ctx,
				"S3 call with name %v ended with error %v, bucket %v, key %v",
				name,
				*err,
				bucket,
				key,
			)

			errorCounter.Inc()

			if errors.Is(*err, context.DeadlineExceeded) {
				logging.Error(
					ctx,
					"S3 call with name %v timed out, bucket %v, key %v",
					name,
					bucket,
					key,
				)

				timeoutCounter.Inc()
			}
			return
		}

		logging.Debug(
			ctx,
			"S3 call with name %v ended, bucket %v, key %v",
			name,
			bucket,
			key,
		)

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
