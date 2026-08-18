package persistence

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
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
) string {

	inner := &testRoundTripper{}
	tokenProvider := &testS3TokenProvider{token: "test-token"}
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
	)

	require.Equal(t, "Bearer test-token", authorization)
}

func TestS3TokenAuthTransportShouldNotSetAuthorizationHeaderForAnotherHost(
	t *testing.T,
) {

	authorization := roundTripS3TokenAuthRequest(
		t,
		"https://another.example.com",
	)

	require.Empty(t, authorization)
}

func TestS3TokenAuthTransportShouldNotSetAuthorizationHeaderForHTTP(t *testing.T) {
	authorization := roundTripS3TokenAuthRequest(
		t,
		"http://s3.example.com",
	)

	require.Empty(t, authorization)
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
