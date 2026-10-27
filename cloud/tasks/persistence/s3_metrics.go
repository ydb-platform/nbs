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
	endpoint               string
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

	ctx = logging.WithFields(ctx, logging.String("S3_ENDPOINT", m.endpoint))
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
		subRegistry.Counter("errors/quotaExceeded")
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
				"S3 call with name %v ended with error %v, "+
					"bucket %v, key %v, duration %v",
				name,
				*err,
				bucket,
				key,
				time.Since(start),
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
		logging.WithFields(
			req.Context(), logging.String("S3_ENDPOINT", m.endpoint),
		),
		"retrying request %v for a %v time, error %v",
		req.Operation.Name,
		req.RetryCount+1,
		req.Error,
	)

	subRegistry := m.registry.WithTags(map[string]string{
		"call": req.Operation.Name,
	})

	// Should initialize all counters before using them, to avoid 'no data'.
	retryCounter := subRegistry.Counter("retry")
	retryCounter.Inc()
}

// Records how long an attempt of the call waited for the attempt limiter.
func (m *s3Metrics) OnAttemptWait(name string, wait time.Duration) {
	subRegistry := m.registry.WithTags(map[string]string{
		"call": name,
	})

	subRegistry.Counter("attemptWaitMs").Add(wait.Milliseconds())
}

// Records the current rate of the attempt limiter of the call.
func (m *s3Metrics) OnAttemptRate(name string, rate float64) {
	subRegistry := m.registry.WithTags(map[string]string{
		"call": name,
	})

	subRegistry.Gauge("attemptRate").Set(rate)
}

func (m *s3Metrics) OnQuotaExceeded(ctx context.Context, name string) {
	subRegistry := m.registry.WithTags(map[string]string{
		"call": name,
	})

	quotaExceededCounter := subRegistry.Counter("errors/quotaExceeded")
	quotaExceededCounter.Inc()
}

func newS3Metrics(
	endpoint string,
	callTimeout time.Duration,
	registry metrics.Registry,
	availabilityMonitoring *AvailabilityMonitoring,
) *s3Metrics {

	return &s3Metrics{
		endpoint:               endpoint,
		callTimeout:            callTimeout,
		registry:               registry,
		availabilityMonitoring: availabilityMonitoring,
	}
}
