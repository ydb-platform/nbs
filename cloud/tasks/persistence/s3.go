package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_endpoints "github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
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
	// Set when requests go through the proxies of the storage.
	proxies         *s3ProxyChooser
	proxyMaxRetries uint64
}

func (r *s3ClientRetryer) RetryRules(req *request.Request) time.Duration {
	r.metrics.OnRetry(req)

	return r.DefaultRetryer.RetryRules(req)
}

// A 5xx or no answer through a proxy means the proxy may be gone: the next
// attempt takes another one, and a few retries are enough, as the storage
// asks. Throttling is about the quota, not the proxy, and keeps its retries.
func (r *s3ClientRetryer) ShouldRetry(req *request.Request) bool {
	retry := r.DefaultRetryer.ShouldRetry(req)
	if r.proxies == nil || isThrottled(req) {
		return retry
	}

	if req.HTTPResponse == nil || req.HTTPResponse.StatusCode >= 500 {
		r.proxies.discard()
		return retry && uint64(req.RetryCount) < r.proxyMaxRetries
	}

	return retry
}

// The storage refused the attempt for its quota: 429, or SlowDown and the
// like in the error. Unlike the SDK, 502/503/504 alone do not count: a
// gateway in the way answers them for reasons of its own.
func isThrottled(req *request.Request) bool {
	if req.HTTPResponse != nil &&
		req.HTTPResponse.StatusCode == http.StatusTooManyRequests {

		return true
	}

	return request.IsErrorThrottle(req.Error)
}

////////////////////////////////////////////////////////////////////////////////

type s3TokenAuthTransport struct {
	inner         http.RoundTripper
	host          string
	tokenProvider credentials.Credentials
}

// Returns a clone of the default transport with room for the given number of
// idle connections. Must be called with a non-zero value: the default
// transport is used as is otherwise.
func newS3HTTPTransport(maxIdleConnsPerHost uint64) *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = int(maxIdleConnsPerHost)
	transport.MaxIdleConnsPerHost = int(maxIdleConnsPerHost)

	return transport
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
	emptySigningRegion         bool
	putObjectAttemptsPerSecond uint64
	minThrottleRetryDelay      time.Duration
	maxThrottleRetryDelay      time.Duration
	proxy                      *s3ProxyOptions
}

type s3ProxyOptions struct {
	hostnameURL     string
	httpPort        uint32
	httpsPort       uint32
	refreshInterval time.Duration
	maxRetries      uint64
}

// S3ClientOption changes only clients whose callers explicitly opt in.
type S3ClientOption func(*s3ClientOptions)

// WithS3EmptySigningRegion permits an empty Signature V4 region for a custom
// endpoint. The SDK normally rejects it before sending a request. Providers
// that require a signing region must still be configured with that region.
func WithS3EmptySigningRegion() S3ClientOption {
	return func(options *s3ClientOptions) {
		options.emptySigningRegion = true
	}
}

func applyS3EmptySigningRegion(s3 *aws_s3.S3) {
	if aws.StringValue(s3.Config.Region) != "" {
		return
	}

	s3.Handlers.Validate.RemoveByName(corehandlers.ValidateEndpointHandler.Name)
	s3.Handlers.Validate.PushBack(
		func(r *request.Request) {
			if r.ClientInfo.Endpoint == "" {
				r.Error = aws.ErrMissingEndpoint
			}
		},
	)
}

// WithS3PutObjectAttemptsPerSecond spaces the attempts of PutObject of this
// client to at most attemptsPerSecond, retries included, so that a request
// quota of the storage is not exceeded whatever the size of the objects;
// 0 does not limit them.
func WithS3PutObjectAttemptsPerSecond(attemptsPerSecond uint64) S3ClientOption {
	return func(options *s3ClientOptions) {
		options.putObjectAttemptsPerSecond = attemptsPerSecond
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

// WithS3Proxies sends the requests past the L3 balancer of the storage,
// straight to its proxies: hostnameURL names a proxy, kept for
// refreshInterval; httpPort and httpsPort are its ports for the two
// schemes. A 5xx or no answer through a proxy drops it and is retried at
// most maxRetries times, each time through another proxy.
func WithS3Proxies(
	hostnameURL string,
	httpPort uint32,
	httpsPort uint32,
	refreshInterval time.Duration,
	maxRetries uint64,
) S3ClientOption {

	return func(options *s3ClientOptions) {
		options.proxy = &s3ProxyOptions{
			hostnameURL:     hostnameURL,
			httpPort:        httpPort,
			httpsPort:       httpsPort,
			refreshInterval: refreshInterval,
			maxRetries:      maxRetries,
		}
	}
}

func NewS3Client(
	endpoint string,
	region string,
	credentials S3Credentials,
	callTimeout time.Duration,
	registry metrics.Registry,
	maxRetriableErrorCount uint64,
	availabilityMonitoring *AvailabilityMonitoring,
	tokenProvider credentials.Credentials,
	maxIdleConnsPerHost uint64,
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

	s3Metrics := newS3Metrics(
		callTimeout,
		registry,
		availabilityMonitoring,
	)

	retryer := &s3ClientRetryer{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries:    int(maxRetriableErrorCount),
			MinThrottleDelay: clientOptions.minThrottleRetryDelay,
			MaxThrottleDelay: clientOptions.maxThrottleRetryDelay,
		},
		metrics: s3Metrics,
	}

	sessionConfig := &aws.Config{
		Credentials: aws_credentials.NewStaticCredentials(
			credentials.ID,
			credentials.Secret,
			credentials.SessionToken,
		),
		Endpoint: &endpoint,
		Region:   &region,
		S3ForcePathStyle: aws.Bool(
			true,
		), // otherwise, we get DNS DDOS errors in tests
		Retryer: retryer,
	}

	var proxies *s3ProxyChooser
	if clientOptions.proxy != nil {
		proxies = newS3ProxyChooser(
			clientOptions.proxy.hostnameURL,
			clientOptions.proxy.httpPort,
			clientOptions.proxy.httpsPort,
			clientOptions.proxy.refreshInterval,
			s3Metrics,
		)
		retryer.proxies = proxies
		retryer.proxyMaxRetries = clientOptions.proxy.maxRetries
	}

	var transport http.RoundTripper = http.DefaultTransport
	if maxIdleConnsPerHost != 0 || proxies != nil {
		custom := http.DefaultTransport.(*http.Transport).Clone()
		if maxIdleConnsPerHost != 0 {
			custom = newS3HTTPTransport(maxIdleConnsPerHost)
		}
		if proxies != nil {
			custom.Proxy = proxies.proxy
		}
		transport = custom
		sessionConfig.HTTPClient = &http.Client{Transport: transport}
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
	}

	session, err := session.NewSession(sessionConfig)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	s3 := aws_s3.New(session)
	if clientOptions.emptySigningRegion {
		applyS3EmptySigningRegion(s3)
	}

	return &S3Client{
		s3:          s3,
		callTimeout: callTimeout,
		metrics:     s3Metrics,
		putObjectLimiter: newS3AttemptLimiter(
			clientOptions.putObjectAttemptsPerSecond,
		),
	}, nil
}

// Every attempt of a request, a retry too, is signed again right before it
// is sent: waiting in a sign handler paces the retries of the SDK as well.
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
			"S3 IAM token authorization is enabled, " +
				"but token provider is not configured",
		)
	}

	options := []S3ClientOption{
		WithS3PutObjectAttemptsPerSecond(
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

	if len(config.GetProxyHostnameUrl()) != 0 {
		refreshInterval, err := time.ParseDuration(
			config.GetProxyRefreshInterval(),
		)
		if err != nil {
			return nil, errors.NewNonRetriableErrorf(
				"failed to parse ProxyRefreshInterval: %w",
				err,
			)
		}

		options = append(options, WithS3Proxies(
			config.GetProxyHostnameUrl(),
			config.GetProxyHttpPort(),
			config.GetProxyHttpsPort(),
			refreshInterval,
			config.GetProxyMaxRetriableErrorCount(),
		))
	}

	return NewS3Client(
		config.GetEndpoint(),
		config.GetRegion(),
		credentials,
		callTimeout,
		registry,
		config.GetMaxRetriableErrorCount(),
		availabilityMonitoring,
		tokenProvider,
		config.GetMaxIdleConnsPerHost(),
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
				"S3 IAM token authorization and credentials file path " +
					"are mutually exclusive",
			)
		}

		// AWS SDK v1 still expects credentials for signing; the transport
		// replaces the Authorization header with the IAM bearer token.
		return NewS3Credentials("iam-token", "iam-token"), nil
	}

	if len(credentialsFilePath) == 0 {
		return S3Credentials{}, errors.NewNonRetriableErrorf(
			"either S3 IAM token authorization or credentials file path " +
				"must be configured",
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

	_, err = c.s3.CreateBucketWithContext(
		ctx,
		&aws_s3.CreateBucketInput{
			Bucket: &bucket,
		},
	)
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
	_, err = c.s3.HeadBucketWithContext(
		ctx,
		&aws_s3.HeadBucketInput{
			Bucket: &bucket,
		},
	)
	if err != nil {
		if aerr, ok := err.(awserr.RequestFailure); ok {
			if aerr.StatusCode() == 404 {
				return false, nil
			}
		}

		return false, wrapS3ExportError(err)
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

// Read the streaming response inside the SDK request so body read errors use
// the same retry limit, backoff and timeout as errors receiving the response.
func withS3ObjectBody(data *[]byte) request.Option {
	return func(r *request.Request) {
		r.Handlers.Unmarshal.PushBack(func(r *request.Request) {
			*data = nil
			if r.Error != nil {
				return
			}

			res := r.Data.(*aws_s3.GetObjectOutput)
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				if ctxErr := r.Context().Err(); ctxErr != nil {
					r.Error = awserr.New(
						request.CanceledErrorCode,
						"request context canceled",
						ctxErr,
					)
				} else {
					r.Error = awserr.New(
						request.ErrCodeRead,
						"failed to read S3 object body",
						err,
					)
				}
				return
			}

			*data = body
		})
	}
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

	var objData []byte
	res, err := c.s3.GetObjectWithContext(
		ctx,
		&aws_s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},
		withIdentityEncoding,
		withS3ObjectBody(&objData),
	)
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

	_, err = c.s3.DeleteObjectWithContext(
		ctx,
		&aws_s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &key,
		},
	)
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
	ID           string `json:"id,omitempty"`
	Secret       string `json:"secret,omitempty"`
	SessionToken string `json:"session_token,omitempty"`
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
