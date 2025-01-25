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
	callTimeout            time.Duration
	registry               metrics.Registry
	availabilityMonitoring *AvailabilityMonitoring
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
		canceledCounter := subRegistry.Counter("errors/canceled")
		timeHistogram := subRegistry.DurationHistogram("time", s3CallDurationBuckets())

		if m.availabilityMonitoring != nil {
			m.availabilityMonitoring.AccountQuery(*err)
		}

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
				logging.Warn(ctx, "failed to process aws go sdk error %v", *err)
			}

			return
		}

		logging.Info(
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

func (m *s3Metrics) OnRetry(req *request.Request) {
	logging.Info(
		req.Context(),
		"retrying request %v for a %v time",
		req.Operation.Name,
		req.RetryCount+1,
	)

	subRegistry := m.registry.WithTags(map[string]string{
		"call": req.Operation.Name,
	})

	// Should initialize all counters before using them, to avoid 'no data'.
	retryCounter := subRegistry.Counter("retry")
	retryCounter.Inc()
}

func newS3Metrics(
	callTimeout time.Duration,
	registry metrics.Registry,
	availabilityMonitoring *AvailabilityMonitoring,
) *s3Metrics {

	return &s3Metrics{
		callTimeout:            callTimeout,
		registry:               registry,
		availabilityMonitoring: availabilityMonitoring,
	}
}
