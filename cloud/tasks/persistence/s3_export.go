package persistence

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"math"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

// S3PutObjectOptions is an opt-in alternative to the legacy upload headers.
// ContentMD5 enables a transport checksum; ETags are not reconstruction hashes.
type S3PutObjectOptions struct {
	ContentType string
	ContentMD5  bool
	// ContentSHA256 is the precomputed hexadecimal SHA-256 of object.Data.
	// The caller must keep the data unchanged until all upload retries finish.
	// When omitted, the SDK computes the payload hash itself.
	ContentSHA256 string
}

// PutObjectWithOptions uses the SDK's bounded retry policy and the same
// seekable body for every attempt. Unlike PutObject, it does not retry quota
// failures by changing storage class, or set a content encoding.
func (c *S3Client) PutObjectWithOptions(
	ctx context.Context,
	bucket string,
	key string,
	object S3Object,
	options S3PutObjectOptions,
) (err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "putting object to s3, bucket %v, key %v", bucket, key)
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	defer c.metrics.StatCall(ctx, "PutObject", bucket, key)(&err)

	requestOptions := []request.Option{withS3ExportRetryer}
	if options.ContentSHA256 != "" {
		var sum []byte
		sum, err = hex.DecodeString(options.ContentSHA256)
		if err != nil || len(sum) != sha256.Size {
			return errors.NewNonRetriableErrorf("invalid precomputed S3 SHA-256")
		}

		// Both the SDK body-hash handler and Signature V4 signer reuse this
		// header instead of scanning the payload again.
		requestOptions = append(
			requestOptions,
			request.WithSetRequestHeaders(
				map[string]string{"X-Amz-Content-Sha256": options.ContentSHA256},
			),
		)
	}

	input := &aws_s3.PutObjectInput{
		Bucket:   &bucket,
		Key:      &key,
		Body:     bytes.NewReader(object.Data),
		Metadata: object.Metadata,
	}

	if options.ContentType != "" {
		input.ContentType = aws.String(options.ContentType)
	}

	if options.ContentMD5 {
		sum := md5.Sum(object.Data)
		input.ContentMD5 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
	}

	if object.StorageClass != "" {
		input.StorageClass = aws.String(object.StorageClass)
	}

	requestOptions = append(requestOptions, c.putObjectRequestOptions()...)
	_, err = c.s3.PutObjectWithContext(ctx, input, requestOptions...)
	return wrapS3ExportError(err)
}

// ObjectExists checks a destination key without downloading its payload.
func (c *S3Client) ObjectExists(
	ctx context.Context,
	bucket string,
	key string,
) (exists bool, err error) {

	ctx = withComponentLoggingField(ctx)
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	defer c.metrics.StatCall(ctx, "HeadObject", bucket, key)(&err)

	_, err = c.s3.HeadObjectWithContext(
		ctx,
		&aws_s3.HeadObjectInput{Bucket: &bucket, Key: &key},
		withS3ExportRetryer,
	)
	if err == nil {
		return true, nil
	}

	var failure awserr.RequestFailure
	if errors.As(err, &failure) && failure.StatusCode() == http.StatusNotFound {
		return false, nil
	}

	return false, wrapS3ExportError(err)
}

// GetObjectWithLimit is used to confirm an uncertain manifest publication.
// It bounds response memory, closes the response, and never retries outside the
// SDK. A read failure leaves publication unconfirmed.
func (c *S3Client) GetObjectWithLimit(
	ctx context.Context,
	bucket string,
	key string,
	maxBytes int64,
) (data []byte, err error) {

	if maxBytes < 0 || maxBytes == math.MaxInt64 {
		return nil, errors.NewNonRetriableErrorf("invalid S3 read limit %v", maxBytes)
	}

	ctx = withComponentLoggingField(ctx)
	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()
	defer c.metrics.StatCall(ctx, "GetObject", bucket, key)(&err)

	res, err := c.s3.GetObjectWithContext(
		ctx,
		&aws_s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},

		withS3ExportRetryer,
		withIdentityEncoding,
	)
	if err != nil {
		return nil, wrapS3ExportError(err)
	}
	defer res.Body.Close()
	data, err = io.ReadAll(io.LimitReader(res.Body, maxBytes+1))
	if err != nil {
		return nil, wrapS3ExportError(err)
	}

	if int64(len(data)) > maxBytes {
		return nil, errors.NewNonRetriableErrorf(
			"S3 object %v exceeds read limit %v",
			key,
			maxBytes,
		)
	}

	return data, nil
}

type s3ExportRetryer struct {
	request.Retryer
}

func (r s3ExportRetryer) ShouldRetry(req *request.Request) bool {
	return !isPermanentS3ExportError(req.Error) && r.Retryer.ShouldRetry(req)
}

func withS3ExportRetryer(req *request.Request) {
	req.Retryer = s3ExportRetryer{req.Retryer}
}

func isPermanentS3ExportError(err error) bool {
	var awsError awserr.Error
	if errors.As(err, &awsError) {
		switch awsError.Code() {
		case "RequestTimeout", "RequestTimeoutException", "SlowDown",
			"Throttling", "ThrottlingException":
			return false
		}
	}

	var failure awserr.RequestFailure
	if errors.As(err, &failure) {
		status := failure.StatusCode()
		if status >= 400 && status < 500 &&
			status != http.StatusRequestTimeout &&
			status != http.StatusTooManyRequests {
			return true
		}
	}

	if errors.As(err, &awsError) {
		switch awsError.Code() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
			"ExpiredToken", "InvalidToken", "TokenRefreshRequired",
			"InvalidRequest", "InvalidArgument", "InvalidBucketName",
			"AuthorizationHeaderMalformed", "PermanentRedirect", "EntityTooLarge",
			"BadDigest", "QuotaLimitExceeded", "NoSuchBucket", "NoSuchKey":
			return true
		}
	}

	return false
}

func wrapS3ExportError(err error) error {
	if err == nil {
		return nil
	}

	if isPermanentS3ExportError(err) {
		return errors.NewNonRetriableError(err)
	}

	return errors.NewRetriableError(err)
}
