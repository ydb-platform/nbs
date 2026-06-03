package persistence

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
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
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestS3TokenAuthTransportShouldSetAuthorizationHeader(t *testing.T) {
	inner := &testRoundTripper{}
	transport := &s3TokenAuthTransport{
		inner: inner,
		tokenProvider: &testS3TokenProvider{
			token: "test-token",
		},
	}

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"http://example.com",
		nil,
	)
	require.NoError(t, err)
	request.Header.Set("Authorization", "AWS test-signature")

	_, err = transport.RoundTrip(request)
	require.NoError(t, err)

	require.Equal(t, "Bearer test-token", inner.request.Header.Get("Authorization"))
	require.Equal(t, "AWS test-signature", request.Header.Get("Authorization"))
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
