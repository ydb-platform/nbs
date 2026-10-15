package persistence

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

const maxRetriableErrorCount = 3

////////////////////////////////////////////////////////////////////////////////

type testS3TokenProvider struct {
	token string
}

func (p *testS3TokenProvider) Token(ctx context.Context) (string, error) {
	return p.token, nil
}

type testRoundTripper struct {
	request *http.Request
}

func (t *testRoundTripper) RoundTrip(
	request *http.Request,
) (*http.Response, error) {

	t.request = request
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func newS3Client(
	metricsRegistry *mocks.RegistryMock,
	callTimeout time.Duration,
) (*S3Client, error) {

	credentials := NewS3Credentials("test", "test")
	return NewS3Client(
		"endpoint",
		"region",
		credentials,
		callTimeout,
		metricsRegistry,
		maxRetriableErrorCount,
		nil, // availabilityMonitoring
		nil, // tokenProvider
		0,   // maxIdleConnsPerHost
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestNewS3CredentialsFromConfig(t *testing.T) {
	credentialsFilePath := filepath.Join(t.TempDir(), "credentials.json")
	err := os.WriteFile(
		credentialsFilePath,
		[]byte(`{"id":"file-id","secret":"file-secret"}`),
		0600,
	)
	require.NoError(t, err)

	tests := []struct {
		name                    string
		config                  *persistence_config.S3Config
		expectedCredentials     S3Credentials
		expectNonRetriableError bool
	}{
		{
			name: "IAM token",
			config: &persistence_config.S3Config{
				UseIamToken: proto.Bool(true),
			},
			expectedCredentials: NewS3Credentials("iam-token", "iam-token"),
		},
		{
			name: "credentials file",
			config: &persistence_config.S3Config{
				CredentialsFilePath: proto.String(credentialsFilePath),
			},
			expectedCredentials: NewS3Credentials("file-id", "file-secret"),
		},
		{
			name: "IAM token and credentials file",
			config: &persistence_config.S3Config{
				UseIamToken:         proto.Bool(true),
				CredentialsFilePath: proto.String(credentialsFilePath),
			},
			expectNonRetriableError: true,
		},
		{
			name:                    "neither IAM token nor credentials file",
			config:                  &persistence_config.S3Config{},
			expectNonRetriableError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credentials, err := newS3CredentialsFromConfig(test.config)
			if test.expectNonRetriableError {
				require.Error(t, err)
				require.True(
					t,
					errors.Is(err, errors.NewEmptyNonRetriableError()),
				)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedCredentials, credentials)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func roundTripS3TokenAuthRequest(
	t *testing.T,
	requestURL string,
	token string,
) string {

	inner := &testRoundTripper{}
	tokenProvider := &testS3TokenProvider{token: token}
	transport := &s3TokenAuthTransport{
		inner:         inner,
		host:          "s3.example.com",
		tokenProvider: tokenProvider,
	}

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		requestURL,
		nil,
	)
	require.NoError(t, err)

	_, err = transport.RoundTrip(request)
	require.NoError(t, err)

	return inner.request.Header.Get("Authorization")
}

func TestS3TokenAuthTransportShouldSetAuthorizationHeader(t *testing.T) {
	authorization := roundTripS3TokenAuthRequest(
		t,
		"https://s3.example.com",
		"test-token",
	)

	require.Equal(t, "Bearer test-token", authorization)
}

func TestS3TokenAuthTransportShouldNotDuplicateAuthorizationPrefix(t *testing.T) {
	authorization := roundTripS3TokenAuthRequest(
		t,
		"https://s3.example.com",
		"Bearer test-token",
	)

	require.Equal(t, "Bearer test-token", authorization)
}

func TestS3TokenAuthTransportShouldNotSetAuthorizationHeaderForAnotherHost(
	t *testing.T,
) {

	authorization := roundTripS3TokenAuthRequest(
		t,
		"https://another.example.com",
		"test-token",
	)

	require.Empty(t, authorization)
}

func TestS3TokenAuthTransportShouldNotSetAuthorizationHeaderForHTTP(t *testing.T) {
	authorization := roundTripS3TokenAuthRequest(
		t,
		"http://s3.example.com",
		"test-token",
	)

	require.Empty(t, authorization)
}

func TestNewS3HTTPTransportKeepsIdleConnections(t *testing.T) {
	transport := newS3HTTPTransport(64)
	require.EqualValues(t, 64, transport.MaxIdleConnsPerHost)
	require.EqualValues(t, 64, transport.MaxIdleConns)

	// The default transport is cloned, not modified.
	defaultTransport := http.DefaultTransport.(*http.Transport)
	require.NotSame(t, defaultTransport, transport)
	require.Zero(t, defaultTransport.MaxIdleConnsPerHost)
}

func TestNewS3TokenAuthHTTPClientWrapsGivenTransport(t *testing.T) {
	inner := newS3HTTPTransport(8)
	client := newS3TokenAuthHTTPClient(
		"s3.example.com",
		&testS3TokenProvider{token: "token"},
		inner,
	)

	transport, ok := client.Transport.(*s3TokenAuthTransport)
	require.True(t, ok)
	require.Same(t, inner, transport.inner)
}

////////////////////////////////////////////////////////////////////////////////

func TestS3ShouldSendErrorCanceledMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())

	metricsRegistry := mocks.NewRegistryMock()

	s3, err := newS3Client(metricsRegistry, 10*time.Second /* callTimeout */)
	require.NoError(t, err)

	cancel()

	metricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	metricsRegistry.GetCounter(
		"errors/canceled",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	err = s3.CreateBucket(ctx, "test")
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	metricsRegistry.AssertAllExpectations(t)
}

func TestS3ShouldSendErrorTimeoutMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	metricsRegistry := mocks.NewRegistryMock()

	s3, err := newS3Client(metricsRegistry, 0 /* callTimeout */)
	require.NoError(t, err)

	metricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	metricsRegistry.GetCounter(
		"hanging",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	metricsRegistry.GetCounter(
		"errors/timeout",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	err = s3.CreateBucket(ctx, "test")
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	metricsRegistry.AssertAllExpectations(t)
}

func TestS3ShouldRetryRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	metricsRegistry := mocks.NewRegistryMock()

	s3, err := newS3Client(metricsRegistry, 10*time.Second /* callTimeout */)
	require.NoError(t, err)

	metricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	metricsRegistry.GetCounter(
		"retry",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Times(maxRetriableErrorCount)

	err = s3.CreateBucket(ctx, "test")
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	metricsRegistry.AssertAllExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

func TestS3AttemptLimiterSpacesAttempts(t *testing.T) {
	ctx := newContext()
	limiter := newS3AttemptLimiter(100)

	start := time.Now()
	for i := 0; i < 5; i++ {
		require.NoError(t, limiter.wait(ctx))
	}

	// The first attempt goes at once, each next one 10 ms later.
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}

func TestS3AttemptLimiterStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	limiter := newS3AttemptLimiter(1)

	require.NoError(t, limiter.wait(ctx))

	cancel()
	err := limiter.wait(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestS3AttemptLimiterGivesBackSlotsOfCancelledWaits(t *testing.T) {
	limiter := newS3AttemptLimiter(10)

	// Fifty waits line up for five seconds and are cancelled.
	ctx, cancel := context.WithCancel(newContext())
	var waits sync.WaitGroup
	for i := 0; i < 50; i++ {
		waits.Add(1)
		go func() {
			defer waits.Done()
			_ = limiter.wait(ctx)
		}()
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
	waits.Wait()

	// A new wait is not held behind their empty slots.
	start := time.Now()
	require.NoError(t, limiter.wait(newContext()))
	require.Less(t, time.Since(start), time.Second)
}

// Some attempts in line are cancelled while others stay and new ones come:
// no two attempts go at the same time, and the cancelled ones leave no gap.
func TestS3AttemptLimiterKeepsPaceAcrossCancellations(t *testing.T) {
	const interval = 50 * time.Millisecond
	limiter := newS3AttemptLimiter(20)

	var mutex sync.Mutex
	var sent []time.Time
	var waits sync.WaitGroup
	attempt := func(ctx context.Context) {
		defer waits.Done()
		if limiter.wait(ctx) == nil {
			mutex.Lock()
			sent = append(sent, time.Now())
			mutex.Unlock()
		}
	}

	// Six attempts line up; two of them, not the first, are cancelled.
	kept := newContext()
	cancelled, cancel := context.WithCancel(newContext())
	for i := 0; i < 6; i++ {
		waits.Add(1)
		ctx := kept
		if i == 1 || i == 3 {
			ctx = cancelled
		}
		go attempt(ctx)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	cancel()

	// Three more come after the cancellations.
	for i := 0; i < 3; i++ {
		waits.Add(1)
		go attempt(kept)
	}
	waits.Wait()

	require.Len(t, sent, 7)
	sort.Slice(sent, func(i, j int) bool { return sent[i].Before(sent[j]) })
	for i := 1; i < len(sent); i++ {
		require.GreaterOrEqual(t, sent[i].Sub(sent[i-1]), interval-5*time.Millisecond)
	}
	// Seven attempts in six intervals: the cancelled ones took no time.
	require.Less(t, sent[6].Sub(sent[0]), 7*interval)
}

func TestS3AttemptLimiterIsNilForZeroRate(t *testing.T) {
	require.Nil(t, newS3AttemptLimiter(0))
}

// Answers SlowDown to the first throttled requests and records when every
// request came.
type throttlingS3 struct {
	mutex     sync.Mutex
	throttled int
	arrivals  []time.Time
}

func (s *throttlingS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	s.arrivals = append(s.arrivals, time.Now())
	throttle := len(s.arrivals) <= s.throttled
	s.mutex.Unlock()

	if throttle {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(
			"<Error><Code>SlowDown</Code><Message>slow down</Message></Error>",
		))
		return
	}

	w.Header().Set("ETag", `"etag"`)
	w.WriteHeader(http.StatusOK)
}

func TestS3ShouldRejectThrottleRetryDelaysThatPanicTheSDK(t *testing.T) {
	for _, delays := range [][2]time.Duration{
		{-time.Second, 0},
		{0, time.Nanosecond},
		{2 * time.Second, time.Second},
	} {
		_, err := NewS3Client(
			"endpoint",
			"region",
			NewS3Credentials("test", "test"),
			time.Second, // callTimeout
			empty.NewRegistry(),
			maxRetriableErrorCount,
			nil, // availabilityMonitoring
			nil, // tokenProvider
			0,   // maxIdleConnsPerHost
			WithS3ThrottleRetryDelays(delays[0], delays[1]),
		)
		require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	}
}

func TestS3ShouldLimitPutObjectAttemptsIncludingRetries(t *testing.T) {
	ctx := newContext()

	storage := &throttlingS3{throttled: 2}
	server := httptest.NewServer(storage)
	defer server.Close()

	s3, err := NewS3Client(
		server.URL,
		"region",
		NewS3Credentials("test", "test"),
		10*time.Second, // callTimeout
		empty.NewRegistry(),
		5,   // maxRetriableErrorCount
		nil, // availabilityMonitoring
		nil, // tokenProvider
		0,   // maxIdleConnsPerHost
		WithS3PutObjectAttemptsPerSecond(20),
		WithS3ThrottleRetryDelays(time.Millisecond, 2*time.Millisecond),
	)
	require.NoError(t, err)

	start := time.Now()
	err = s3.PutObject(ctx, "bucket", "key", S3Object{Data: []byte("data")})
	require.NoError(t, err)
	elapsed := time.Since(start)

	// Two throttled attempts and the successful one, each in its own slot
	// of 50 ms: the retries waited for the limiter too.
	require.Len(t, storage.arrivals, 3)
	for i := 1; i < len(storage.arrivals); i++ {
		require.GreaterOrEqual(
			t,
			storage.arrivals[i].Sub(storage.arrivals[i-1]),
			45*time.Millisecond,
		)
	}

	// The throttle pause is the configured milliseconds, not the SDK
	// default of 500 ms and more.
	require.Less(t, elapsed, 400*time.Millisecond)
}
