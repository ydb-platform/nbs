package exporter

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// Retry whole operations so a per-call timeout does not limit the export's
// lifetime. Keep part buffers and upload bytes unchanged between attempts.
// maxRetries counts retries after the initial attempt; -1 means unlimited.
func retryS3Operation[T any](
	ctx context.Context,
	maxRetries int,
	operation func() (T, error),
) (T, error) {

	if maxRetries < -1 {
		var zero T
		return zero, task_errors.NewNonRetriableErrorf("S3 max retries must be -1 or nonnegative")
	}

	retriesLeft := maxRetries
	delay := 100 * time.Millisecond
	retrying := false
	for {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}

		if retrying {
			recordS3OperationRetry(ctx)
		}

		value, err := operation()
		if err == nil {
			return value, nil
		}
		// A child call's deadline can be retried with a fresh context. A
		// canceled export/group context must stop before another attempt.
		if parentErr := ctx.Err(); parentErr != nil {
			var zero T
			return zero, parentErr
		}
		if retriesLeft == 0 || !isRetryableS3OperationError(err) {
			return value, err
		}
		if retriesLeft > 0 {
			retriesLeft--
		}

		logging.Warn(ctx, "retrying S3 export operation in %v: %v", delay, err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			var zero T
			return zero, ctx.Err()
		case <-timer.C:
		}

		retrying = true
		delay *= 2
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}
}

func isRetryableS3OperationError(err error) bool {
	if task_errors.Is(err, task_errors.NewEmptyNonRetriableError()) ||
		task_errors.Is(err, task_errors.NewEmptyNonCancellableError()) {
		return false
	}
	return task_errors.CanRetry(err) || isS3OperationTimeout(err)
}

func isS3OperationTimeout(err error) bool {
	if task_errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout net.Error
	if task_errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}
	// AWS SDK v1 uses OrigErr rather than Go's standard Unwrap method.
	var awsError awserr.Error
	return task_errors.As(err, &awsError) && isS3OperationTimeout(awsError.OrigErr())
}

func retryS3Call(ctx context.Context, maxRetries int, operation func() error) error {
	_, err := retryS3Operation(ctx, maxRetries, func() (struct{}, error) {
		return struct{}{}, operation()
	})
	return err
}

type retryingS3Destination struct {
	S3Destination
	maxRetries int
}

func (d retryingS3Destination) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return retryS3Operation(ctx, d.maxRetries, func() (bool, error) {
		return d.S3Destination.BucketExists(ctx, bucket)
	})
}

func (d retryingS3Destination) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	return retryS3Operation(ctx, d.maxRetries, func() (bool, error) {
		return d.S3Destination.ObjectExists(ctx, bucket, key)
	})
}

func (d retryingS3Destination) DeleteObject(ctx context.Context, bucket, key string) error {
	return retryS3Call(ctx, d.maxRetries, func() error {
		err := d.S3Destination.DeleteObject(ctx, bucket, key)
		// The legacy DeleteObject method marks most errors retryable,
		// including permanent HTTP failures. Do not loop on those.
		var awsError awserr.Error
		if task_errors.As(err, &awsError) {
			switch awsError.Code() {
			case "RequestTimeout", "RequestTimeoutException", "SlowDown",
				"Throttling", "ThrottlingException":
				return err
			case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
				"ExpiredToken", "InvalidToken", "TokenRefreshRequired",
				"InvalidRequest", "InvalidArgument", "InvalidBucketName",
				"AuthorizationHeaderMalformed", "PermanentRedirect", "EntityTooLarge",
				"BadDigest", "QuotaLimitExceeded", "NoSuchBucket", "NoSuchKey":
				return task_errors.NewNonRetriableError(err)
			}
		}
		var failure awserr.RequestFailure
		if task_errors.As(err, &failure) {
			status := failure.StatusCode()
			if status >= 400 && status < 500 &&
				status != http.StatusRequestTimeout &&
				status != http.StatusTooManyRequests {
				return task_errors.NewNonRetriableError(err)
			}
		}
		return err
	})
}

func (d retryingS3Destination) PutObjectWithOptions(
	ctx context.Context,
	bucket, key string,
	object persistence.S3Object,
	options persistence.S3PutObjectOptions,
) error {

	return retryS3Call(ctx, d.maxRetries, func() error {
		return d.S3Destination.PutObjectWithOptions(ctx, bucket, key, object, options)
	})
}

func (d retryingS3Destination) GetObjectWithLimit(
	ctx context.Context,
	bucket, key string,
	limit int64,
) ([]byte, error) {

	return retryS3Operation(ctx, d.maxRetries, func() ([]byte, error) {
		return d.S3Destination.GetObjectWithLimit(ctx, bucket, key, limit)
	})
}

// Classifies failures for task retries after bounded retries within a run.
func IsRetryableS3Error(err error) bool {
	return isRetryableS3OperationError(err)
}
