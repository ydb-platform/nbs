package persistence

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_endpoints "github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

const (
	s3ErrCodeQuotaLimitExceeded = "QuotaLimitExceeded"
)

////////////////////////////////////////////////////////////////////////////////

func withComponentLoggingField(ctx context.Context) context.Context {
	return logging.WithComponent(ctx, logging.ComponentS3)
}

////////////////////////////////////////////////////////////////////////////////

type s3ClientRetryer struct {
	client.DefaultRetryer
	metrics *s3Metrics
}

func (r *s3ClientRetryer) RetryRules(req *request.Request) time.Duration {
	r.metrics.OnRetry(req)

	return r.DefaultRetryer.RetryRules(req)
}

////////////////////////////////////////////////////////////////////////////////

type s3TokenAuthTransport struct {
	inner         http.RoundTripper
	host          string
	tokenProvider credentials.Credentials
}

func newS3TokenAuthHTTPClient(
	host string,
	tokenProvider credentials.Credentials,
	inner http.RoundTripper,
) *http.Client {

	return &http.Client{
		Transport: &s3TokenAuthTransport{
			inner:         inner,
			host:          host,
			tokenProvider: tokenProvider,
		},
	}
}

func (t *s3TokenAuthTransport) RoundTrip(
	request *http.Request,
) (*http.Response, error) {

	// Do not set Authorization header for http.
	// Do not set Authorization header for redirects.
	if request.URL.Scheme != "https" || request.URL.Host != t.host {
		return t.inner.RoundTrip(request)
	}

	token, err := t.tokenProvider.Token(request.Context())
	if err != nil {
		return nil, err
	}

	request = request.Clone(request.Context())
	headers.SetAuthorizationHeader(request.Header, token)

	return t.inner.RoundTrip(request)
}

////////////////////////////////////////////////////////////////////////////////

type S3Client struct {
	s3          *aws_s3.S3
	callTimeout time.Duration
	metrics     *s3Metrics
	// Nil does not limit PutObject.
	putObjectLimiter *s3AttemptLimiter
}

type s3ClientOptions struct {
	emptySigningRegion            bool
	minPutObjectAttemptsPerSecond uint64
	maxPutObjectAttemptsPerSecond uint64
	minThrottleRetryDelay         time.Duration
	maxThrottleRetryDelay         time.Duration
}

type S3ClientOption func(*s3ClientOptions)

// WithS3EmptySigningRegion allows an empty region: requests are signed with
// the SigV4 default scope "us-east-1", which S3-compatible storages that do
// not use regions accept. Without the option an empty region is an error.
func WithS3EmptySigningRegion() S3ClientOption {
	return func(options *s3ClientOptions) {
		options.emptySigningRegion = true
	}
}

// WithS3PutObjectAttemptsPerSecond spaces the attempts of PutObject of this
// client, retries included, so that a request quota of the storage is not
// exceeded whatever the size of the objects. With minAttempts below
// maxAttempts the rate adapts between them to the throttling of the storage:
// down after a window with a 429 or SlowDown, up otherwise, so that hosts
// sharing a quota settle at their shares of it without being told the quota.
// A zero minAttempts keeps the rate at maxAttempts; a zero maxAttempts does
// not limit.
func WithS3PutObjectAttemptsPerSecond(
	minAttempts uint64,
	maxAttempts uint64,
) S3ClientOption {

	return func(options *s3ClientOptions) {
		options.minPutObjectAttemptsPerSecond = minAttempts
		options.maxPutObjectAttemptsPerSecond = maxAttempts
	}
}

// WithS3ThrottleRetryDelays bounds the pause before a throttled request
// (429, SlowDown) is retried. Zero keeps the SDK default: 500 ms, doubled on
// every retry up to 300 s.
func WithS3ThrottleRetryDelays(minDelay, maxDelay time.Duration) S3ClientOption {
	return func(options *s3ClientOptions) {
		options.minThrottleRetryDelay = minDelay
		options.maxThrottleRetryDelay = maxDelay
	}
}

const s3DefaultSigningRegion = "us-east-1"

// maxIdleConnsPerHost limits idle connections kept to the endpoint; 0 keeps
// the http.DefaultTransport value.
func NewS3Client(
	endpoint string,
	region string,
	credentials S3Credentials,
	callTimeout time.Duration,
	registry metrics.Registry,
	maxRetriableErrorCount uint64,
	availabilityMonitoring *AvailabilityMonitoring,
	tokenProvider credentials.Credentials,
	maxIdleConnsPerHost int,
	options ...S3ClientOption,
) (*S3Client, error) {

	var clientOptions s3ClientOptions
	for _, option := range options {
		option(&clientOptions)
	}

	err := validateThrottleRetryDelays(
		clientOptions.minThrottleRetryDelay,
		clientOptions.maxThrottleRetryDelay,
	)
	if err != nil {
		return nil, err
	}

	if clientOptions.minPutObjectAttemptsPerSecond >
		clientOptions.maxPutObjectAttemptsPerSecond {

		return nil, errors.NewNonRetriableErrorf(
			"S3 min PutObject attempts per second %v is greater than max %v",
			clientOptions.minPutObjectAttemptsPerSecond,
			clientOptions.maxPutObjectAttemptsPerSecond,
		)
	}

	if len(region) == 0 {
		if !clientOptions.emptySigningRegion {
			return nil, errors.NewNonRetriableErrorf(
				"S3 region is empty for endpoint %v",
				endpoint,
			)
		}

		region = s3DefaultSigningRegion
	}

	s3Metrics := newS3Metrics(
		callTimeout,
		registry,
		availabilityMonitoring,
	)

	sessionConfig := &aws.Config{
		Credentials: aws_credentials.NewStaticCredentials(
			credentials.ID,
			credentials.Secret,
			"", // token - only required for temporary security credentials retrieved via STS, we don't need that
		),
		Endpoint:         &endpoint,
		Region:           &region,
		S3ForcePathStyle: aws.Bool(true), // otherwise, we get DNS DDOS errors in tests
		Retryer: &s3ClientRetryer{
			DefaultRetryer: client.DefaultRetryer{
				NumMaxRetries:    int(maxRetriableErrorCount),
				MinThrottleDelay: clientOptions.minThrottleRetryDelay,
				MaxThrottleDelay: clientOptions.maxThrottleRetryDelay,
			},
			metrics: s3Metrics,
		},
	}
	transport := http.DefaultTransport
	if maxIdleConnsPerHost > 0 {
		custom := http.DefaultTransport.(*http.Transport).Clone()
		custom.MaxIdleConnsPerHost = maxIdleConnsPerHost
		transport = custom
	}

	if tokenProvider != nil {
		endpointURL, err := url.Parse(aws_endpoints.AddScheme(endpoint, false))
		if err != nil {
			return nil, errors.NewNonRetriableErrorf(
				"failed to parse S3 endpoint: %w",
				err,
			)
		}

		sessionConfig.HTTPClient = newS3TokenAuthHTTPClient(
			endpointURL.Host,
			tokenProvider,
			transport,
		)
	} else if maxIdleConnsPerHost > 0 {
		sessionConfig.HTTPClient = &http.Client{Transport: transport}
	}

	session, err := session.NewSession(sessionConfig)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return &S3Client{
		s3:          aws_s3.New(session),
		callTimeout: callTimeout,
		metrics:     s3Metrics,
		putObjectLimiter: newS3AttemptLimiter(
			clientOptions.minPutObjectAttemptsPerSecond,
			clientOptions.maxPutObjectAttemptsPerSecond,
			func(rate float64) {
				s3Metrics.OnAttemptRate("PutObject", rate)
			},
		),
	}, nil
}

// Every attempt of a request, a retry too, is signed again right before it
// is sent: waiting in a sign handler paces the retries of the SDK as well.
// Every attempt also completes through the handler that tells the limiter
// about throttling.
func (c *S3Client) putObjectRequestOptions() []request.Option {
	if c.putObjectLimiter == nil {
		return nil
	}

	return []request.Option{
		func(r *request.Request) {
			r.Handlers.Sign.PushFront(func(r *request.Request) {
				waitStart := time.Now()
				err := c.putObjectLimiter.wait(r.Context())
				c.metrics.OnAttemptWait(r.Operation.Name, time.Since(waitStart))
				if err != nil {
					r.Error = err
				}
			})
			r.Handlers.CompleteAttempt.PushBack(func(r *request.Request) {
				if r.Error != nil && r.IsErrorThrottle() {
					c.putObjectLimiter.onThrottle()
				}
			})
		},
	}
}

func NewS3ClientFromConfig(
	config *persistence_config.S3Config,
	registry metrics.Registry,
	availabilityMonitoring *AvailabilityMonitoring,
	tokenProvider credentials.Credentials,
) (*S3Client, error) {

	credentials, err := newS3CredentialsFromConfig(config)
	if err != nil {
		return nil, err
	}

	callTimeout, err := time.ParseDuration(config.GetCallTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse callTimeout: %w",
			err,
		)
	}

	if !config.GetUseIamToken() {
		tokenProvider = nil
	} else if tokenProvider == nil {
		return nil, errors.NewNonRetriableErrorf(
			"S3 IAM token authorization is enabled, but token provider is not configured",
		)
	}

	options := []S3ClientOption{
		WithS3PutObjectAttemptsPerSecond(
			config.GetMinPutObjectAttemptsPerSecond(),
			config.GetMaxPutObjectAttemptsPerSecond(),
		),
	}

	minThrottleRetryDelay, err := parseOptionalDuration(
		config.GetMinThrottleRetryDelay(),
	)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse MinThrottleRetryDelay: %w",
			err,
		)
	}

	maxThrottleRetryDelay, err := parseOptionalDuration(
		config.GetMaxThrottleRetryDelay(),
	)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse MaxThrottleRetryDelay: %w",
			err,
		)
	}

	options = append(options, WithS3ThrottleRetryDelays(
		minThrottleRetryDelay,
		maxThrottleRetryDelay,
	))

	return NewS3Client(
		config.GetEndpoint(),
		config.GetRegion(),
		credentials,
		callTimeout,
		registry,
		config.GetMaxRetriableErrorCount(),
		availabilityMonitoring,
		tokenProvider,
		0, // maxIdleConnsPerHost
		options...,
	)
}

// The SDK draws a pause from [min, 2*min) and, past the max, from
// [max/2, max): a bound that makes such a range empty panics there. Zero
// keeps the SDK default.
func validateThrottleRetryDelays(minDelay, maxDelay time.Duration) error {
	for _, delay := range []time.Duration{minDelay, maxDelay} {
		if delay != 0 && delay < time.Millisecond {
			return errors.NewNonRetriableErrorf(
				"S3 throttle retry delay %v is less than 1ms",
				delay,
			)
		}
	}

	if minDelay != 0 && maxDelay != 0 && minDelay > maxDelay {
		return errors.NewNonRetriableErrorf(
			"S3 min throttle retry delay %v is greater than max %v",
			minDelay,
			maxDelay,
		)
	}

	return nil
}

// An empty value is zero.
func parseOptionalDuration(value string) (time.Duration, error) {
	if len(value) == 0 {
		return 0, nil
	}

	return time.ParseDuration(value)
}

func newS3CredentialsFromConfig(
	config *persistence_config.S3Config,
) (S3Credentials, error) {

	credentialsFilePath := config.GetCredentialsFilePath()
	if config.GetUseIamToken() {
		if len(credentialsFilePath) != 0 {
			return S3Credentials{}, errors.NewNonRetriableErrorf(
				"S3 IAM token authorization and credentials file path are mutually exclusive",
			)
		}

		// AWS SDK v1 still expects credentials for signing; the transport
		// replaces the Authorization header with the IAM bearer token.
		return NewS3Credentials("iam-token", "iam-token"), nil
	}

	if len(credentialsFilePath) == 0 {
		return S3Credentials{}, errors.NewNonRetriableErrorf(
			"either S3 IAM token authorization or credentials file path must be configured",
		)
	}

	return NewS3CredentialsFromFile(credentialsFilePath)
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) CreateBucket(
	ctx context.Context,
	bucket string,
) (err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "creating bucket %v in s3", bucket)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "CreateBucket", bucket, "")(&err)

	_, err = c.s3.CreateBucketWithContext(ctx, &aws_s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case aws_s3.ErrCodeBucketAlreadyOwnedByYou:
				// Bucket is already created
				return nil
			}
		}

		return errors.NewRetriableError(err)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) BucketExists(
	ctx context.Context,
	bucket string,
) (exists bool, err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "checking if bucket %v exists in s3", bucket)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "GetBucket", bucket, "")(&err)
	_, err = c.s3.HeadBucketWithContext(ctx, &aws_s3.HeadBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		if aerr, ok := err.(awserr.RequestFailure); ok {
			if aerr.StatusCode() == 404 {
				return false, nil
			}
		}

		return false, errors.NewRetriableError(err)
	}

	return true, nil
}

// Objects written before were marked with Content-Encoding
// "application/octet-stream". A storage that compresses responses keeps that
// marking, so the Go transport does not decompress them and the caller gets
// gzip instead of the object. Asking for the bytes as stored avoids it. Set
// after signing, as the transport sets its own Accept-Encoding.
func withIdentityEncoding(r *request.Request) {
	r.Handlers.Send.PushFront(func(r *request.Request) {
		r.HTTPRequest.Header.Set("Accept-Encoding", "identity")
	})
}

func (c *S3Client) GetObject(
	ctx context.Context,
	bucket string,
	key string,
) (o S3Object, err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "getting object from s3, bucket %v, key %v", bucket, key)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "GetObject", bucket, key)(&err)

	res, err := c.s3.GetObjectWithContext(ctx, &aws_s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}, withIdentityEncoding)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case aws_s3.ErrCodeNoSuchKey:
				return S3Object{}, errors.NewSilentNonRetriableErrorf(
					"s3 object not found: %v",
					key,
				)
			case aws_s3.ErrCodeNoSuchBucket:
				return S3Object{}, errors.NewNonRetriableError(err)
			}
		}

		return S3Object{}, errors.NewRetriableError(err)
	}

	objData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return S3Object{}, errors.NewRetriableError(err)
	}

	storageClass := aws.StringValue(res.StorageClass)
	if len(storageClass) == 0 {
		storageClass = aws_s3.StorageClassStandard
	}

	return S3Object{
		Data:         objData,
		Metadata:     res.Metadata,
		StorageClass: storageClass,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) PutObject(
	ctx context.Context,
	bucket string,
	key string,
	object S3Object,
) (err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "putting object to s3, bucket %v, key %v", bucket, key)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "PutObject", bucket, key)(&err)

	err = c.putObjectWithStorageClass(ctx, bucket, key, object)
	if err == nil {
		return nil
	}

	if !isQuotaLimitExceeded(err) {
		return wrapError(err)
	}

	c.metrics.OnQuotaExceeded(ctx, "PutObject")

	if len(object.StorageClass) == 0 {
		return wrapError(err)
	}

	logging.Debug(
		ctx,
		"s3 put object with storage class %q failed with quota "+
			"error, retrying without storage class",
		object.StorageClass,
	)

	err = c.putObject(ctx, bucket, key, object)
	return wrapError(err)
}

////////////////////////////////////////////////////////////////////////////////

type S3PutObjectOptions struct {
	// Content-Type of the object; empty keeps the storage default.
	ContentType string
	// Sends Content-MD5 so that the storage rejects a corrupted upload.
	ContentMD5 bool
}

// PutObjectWithOptions uploads the object as is: no storage class and no
// quota fallback, with the Content-Type and Content-MD5 the options ask for.
// Used for objects written to buckets outside Disk Manager, such as exports.
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

	input := &aws_s3.PutObjectInput{
		Bucket:   &bucket,
		Key:      &key,
		Body:     bytes.NewReader(object.Data),
		Metadata: object.Metadata,
	}
	if len(options.ContentType) != 0 {
		input.ContentType = aws.String(options.ContentType)
	}

	if options.ContentMD5 {
		sum := md5.Sum(object.Data)
		input.ContentMD5 = aws.String(
			base64.StdEncoding.EncodeToString(sum[:]),
		)
	}

	_, err = c.s3.PutObjectWithContext(
		ctx,
		input,
		c.putObjectRequestOptions()...,
	)
	return wrapError(err)
}

// GetObjectWithLimit returns the object data when it is at most limit bytes
// long and a non-retriable error otherwise, so that a caller comparing an
// object with a known value never reads an unbounded body.
func (c *S3Client) GetObjectWithLimit(
	ctx context.Context,
	bucket string,
	key string,
	limit int64,
) (data []byte, err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "getting object from s3, bucket %v, key %v", bucket, key)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "GetObject", bucket, key)(&err)

	res, err := c.s3.GetObjectWithContext(ctx, &aws_s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}, withIdentityEncoding)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == aws_s3.ErrCodeNoSuchKey {
				return nil, errors.NewSilentNonRetriableErrorf(
					"s3 object not found: %v",
					key,
				)
			}
		}

		return nil, wrapError(err)
	}
	defer res.Body.Close()

	data, err = ioutil.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	if int64(len(data)) > limit {
		return nil, errors.NewNonRetriableErrorf(
			"s3 object %v is longer than %v bytes",
			key,
			limit,
		)
	}

	return data, nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) putObject(
	ctx context.Context,
	bucket string,
	key string,
	object S3Object,
) error {

	input := &aws_s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader(object.Data),
		Metadata:    object.Metadata,
		ContentType: aws.String("application/octet-stream"),
	}

	_, err := c.s3.PutObjectWithContext(
		ctx,
		input,
		c.putObjectRequestOptions()...,
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) putObjectWithStorageClass(
	ctx context.Context,
	bucket string,
	key string,
	object S3Object,
) error {

	input := &aws_s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        bytes.NewReader(object.Data),
		Metadata:    object.Metadata,
		ContentType: aws.String("application/octet-stream"),
	}
	if len(object.StorageClass) != 0 {
		input.StorageClass = aws.String(object.StorageClass)
	}

	_, err := c.s3.PutObjectWithContext(
		ctx,
		input,
		c.putObjectRequestOptions()...,
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func wrapError(err error) error {
	if err == nil {
		return nil
	}

	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case aws_s3.ErrCodeNoSuchBucket:
			return errors.NewNonRetriableError(err)
		}
	}

	return errors.NewRetriableError(err)
}

////////////////////////////////////////////////////////////////////////////////

func isQuotaLimitExceeded(err error) bool {
	aerr, ok := err.(awserr.Error)
	return ok && aerr.Code() == s3ErrCodeQuotaLimitExceeded
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) DeleteObject(
	ctx context.Context,
	bucket string,
	key string,
) (err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(
		ctx,
		"deleting object from s3, bucket %v, key %v",
		bucket,
		key,
	)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "DeleteObject", bucket, key)(&err)

	_, err = c.s3.DeleteObjectWithContext(ctx, &aws_s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case aws_s3.ErrCodeNoSuchBucket:
				return errors.NewNonRetriableError(err)
			}
		}

		return errors.NewRetriableError(err)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type S3Object struct {
	Data         []byte
	Metadata     map[string]*string
	StorageClass string
}

////////////////////////////////////////////////////////////////////////////////

type S3Credentials struct {
	ID     string `json:"id,omitempty"`
	Secret string `json:"secret,omitempty"`
}

func NewS3Credentials(id, secret string) S3Credentials {
	return S3Credentials{
		ID:     id,
		Secret: secret,
	}
}

func NewS3CredentialsFromFile(filePath string) (S3Credentials, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return S3Credentials{}, err
	}

	credentials := S3Credentials{}

	err = json.Unmarshal(file, &credentials)
	if err != nil {
		return S3Credentials{}, err
	}

	return credentials, nil
}
