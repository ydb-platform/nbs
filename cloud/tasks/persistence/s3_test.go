package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

////////////////////////////////////////////////////////////////////////////////

const maxRetriableErrorCount = 3

////////////////////////////////////////////////////////////////////////////////

func newS3Client(
	ctx context.Context,
	callTimeout time.Duration,
	s3MetricsRegistry *mocks.RegistryMock,
	healthCheckStorage HealthStorage,
	healthMetricsRegistry *mocks.RegistryMock,
) (*S3Client, error) {

	credentials := NewS3Credentials("test", "test")
	return NewS3Client(
		ctx,
		"endpoint",
		"region",
		credentials,
		callTimeout,
		s3MetricsRegistry,
		healthCheckStorage,
		healthMetricsRegistry,
		maxRetriableErrorCount,
	)
}

////////////////////////////////////////////////////////////////////////////////

func TestS3ShouldSendErrorCanceledMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())

	s3MetricsRegistry := mocks.NewRegistryMock()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	healthCheckStorage := newStorage(t, ctx, db)
	healthMetricsRegistry := mocks.NewRegistryMock()

	s3, err := newS3Client(ctx, 10*time.Second /* callTimeout */, s3MetricsRegistry, healthCheckStorage, healthMetricsRegistry)
	require.NoError(t, err)

	cancel()

	s3MetricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	s3MetricsRegistry.GetCounter(
		"errors/canceled",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	err = s3.CreateBucket(ctx, "test")
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	s3MetricsRegistry.AssertAllExpectations(t)
}

func TestS3ShouldSendErrorTimeoutMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	s3MetricsRegistry := mocks.NewRegistryMock()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	healthCheckStorage := newStorage(t, ctx, db)
	healthMetricsRegistry := mocks.NewRegistryMock()

	s3, err := newS3Client(ctx, 0 /* callTimeout */, s3MetricsRegistry, healthCheckStorage, healthMetricsRegistry)
	require.NoError(t, err)

	s3MetricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	s3MetricsRegistry.GetCounter(
		"hanging",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	s3MetricsRegistry.GetCounter(
		"errors/timeout",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	err = s3.CreateBucket(ctx, "test")
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	s3MetricsRegistry.AssertAllExpectations(t)
}

func TestS3ShouldRetryRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	s3MetricsRegistry := mocks.NewRegistryMock()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	healthCheckStorage := newStorage(t, ctx, db)
	healthMetricsRegistry := mocks.NewRegistryMock()

	s3, err := newS3Client(ctx, 10*time.Second /* callTimeout */, s3MetricsRegistry, healthCheckStorage, healthMetricsRegistry)
	require.NoError(t, err)

	s3MetricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	s3MetricsRegistry.GetCounter(
		"retry",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Times(maxRetriableErrorCount)

	err = s3.CreateBucket(ctx, "test")
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	s3MetricsRegistry.AssertAllExpectations(t)
}

func TestS3ShouldSendHealthMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	s3MetricsRegistry := mocks.NewRegistryMock()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	healthCheckStorage := newStorage(t, ctx, db)
	healthMetricsRegistry := mocks.NewRegistryMock()

	s3, err := newS3Client(ctx, 10*time.Second /* callTimeout */, s3MetricsRegistry, healthCheckStorage, healthMetricsRegistry)
	require.NoError(t, err)

	s3MetricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Once()

	s3MetricsRegistry.GetCounter(
		"retry",
		map[string]string{"call": "CreateBucket"},
	).On("Inc").Times(maxRetriableErrorCount)

	err = s3.CreateBucket(ctx, "test")
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))

	s3MetricsRegistry.AssertAllExpectations(t)
}
