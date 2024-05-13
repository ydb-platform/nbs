package persistence

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
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
		timeoutCounter := subRegistry.Counter("errors/timeout")
		canceledCounter := subRegistry.Counter("errors/cancelled")
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

			var awsError awserr.Error
			if errors.As(*err, &awsError) &&
				awsError.Code() == request.CanceledErrorCode {

				if ctx.Err() == context.DeadlineExceeded {
					timeoutCounter.Inc()
				} else {
					canceledCounter.Inc()
				}
			} else {
				logging.Debug(ctx, "failed to process aws go sdk error %v", err)
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
