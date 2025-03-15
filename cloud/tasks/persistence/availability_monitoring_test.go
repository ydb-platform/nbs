package persistence

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newAvailabilityMonitoringStorage(
	t *testing.T,
	ctx context.Context,
	db *YDBClient,
	banInterval time.Duration,
	maxBanHostsCount uint64,
) *AvailabilityMonitoringStorageYDB {

	config := &tasks_config.TasksConfig{}
	storageFolder := t.Name()
	config.StorageFolder = &storageFolder
	err := CreateAvailabilityMonitoringYDBTables(
		ctx,
		config.GetStorageFolder(),
		db,
		false, // dropUnusedColums
	)
	require.NoError(t, err)

	storage := NewAvailabilityMonitoringStorage(
		config,
		db,
		banInterval,
		maxBanHostsCount,
	)
	require.NoError(t, err)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

func TestAvailabilityMonitoringShouldSendMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newAvailabilityMonitoringStorage(
		t,
		ctx,
		db,
		10*time.Second, // banInterval
		1,              // maxBanHostsCount
	)
	registry := mocks.NewRegistryMock()

	gaugeSetWg := sync.WaitGroup{}

	gaugeSetWg.Add(1)
	registry.GetGauge(
		"successRate",
		map[string]string{
			"component": "component",
			"host":      "host",
		},
	).On("Set", float64(1)).Once().Run(
		func(args mock.Arguments) {
			gaugeSetWg.Done()
		},
	)

	monitoring, err := NewAvailabilityMonitoring(
		ctx,
		"component",
		"host",
		15*time.Second, // availabilityReportingInterval
		0.99,           // successRateAvailabilityTrasehold
		storage,
		registry,
	)
	require.NoError(t, err)
	gaugeSetWg.Wait()

	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(errors.NewEmptyRetriableError())

	gaugeSetWg.Add(1)
	registry.GetGauge(
		"successRate",
		map[string]string{
			"component": "component",
			"host":      "host",
		},
	).On("Set", float64(3.0/4.0)).Once().Run(
		func(args mock.Arguments) {
			gaugeSetWg.Done()
		},
	)
	gaugeSetWg.Wait()

	registry.AssertAllExpectations(t)
}

func TestAvailabilityMonitoringShouldUpdateComponentsAvailability(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	banInterval := 10 * time.Second
	storage := newAvailabilityMonitoringStorage(
		t,
		ctx,
		db,
		banInterval,
		1, // maxBanHostsCount
	)
	registry := mocks.NewRegistryMock()

	gaugeSetWg := sync.WaitGroup{}

	gaugeSetWg.Add(1)
	registry.GetGauge(
		"successRate",
		map[string]string{
			"component": "component",
			"host":      "host",
		},
	).On("Set", float64(1)).Once().Run(
		func(args mock.Arguments) {
			gaugeSetWg.Done()
		},
	)

	availabilityReportingInterval := 5 * time.Second
	monitoring, err := NewAvailabilityMonitoring(
		ctx,
		"component",
		"host",
		availabilityReportingInterval,
		0.51, // successRateAvailabilityTrasehold
		storage,
		registry,
	)
	require.NoError(t, err)
	gaugeSetWg.Wait()

	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(errors.NewEmptyRetriableError())

	gaugeSetWg.Add(1)
	registry.GetGauge(
		"successRate",
		map[string]string{
			"component": "component",
			"host":      "host",
		},
	).On("Set", float64(3.0/4.0)).Once().Run(
		func(args mock.Arguments) {
			gaugeSetWg.Done()
		},
	)
	gaugeSetWg.Wait()

	time.Sleep(time.Second) // kostyl

	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(nil)
	monitoring.AccountQuery(errors.NewEmptyRetriableError())
	monitoring.AccountQuery(errors.NewEmptyRetriableError())

	gaugeSetWg.Add(1)
	registry.GetGauge(
		"successRate",
		map[string]string{
			"component": "component",
			"host":      "host",
		},
	).On("Set", float64(2.0/4.0)).Once().Run(
		func(args mock.Arguments) {
			gaugeSetWg.Done()
		},
	)
	gaugeSetWg.Wait()

	time.Sleep(time.Second) // kostyl

	results, err := storage.GetComponentsAvailabilityResults(
		ctx,
		"host",
	)
	require.NoError(t, err)

	require.Equal(t, 1, len(results))
	require.EqualValues(t, false, results[0].Availability)

	registry.GetGauge(
		"successRate",
		map[string]string{
			"component": "component",
			"host":      "host",
		},
	).On("Set", mock.Anything).Return(mock.Anything)

	time.Sleep(banInterval + availabilityReportingInterval)

	results, err = storage.GetComponentsAvailabilityResults(
		ctx,
		"host",
	)
	require.NoError(t, err)

	require.Equal(t, 1, len(results))
	require.EqualValues(t, true, results[0].Availability)

	registry.AssertAllExpectations(t)
}
