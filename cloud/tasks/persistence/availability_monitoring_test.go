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
) *AvailabilityMonitoringStorageYDB {

	config := &tasks_config.TasksConfig{}
	err := CreateAvailabilityMonitoringYDBTables(
		ctx,
		config.GetStorageFolder(),
		db,
		false, // dropUnusedColums
	)
	require.NoError(t, err)

	storage := NewAvailabilityMonitoringStorage(config, db)
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

	storage := newAvailabilityMonitoringStorage(t, ctx, db)
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

	monitoring := NewAvailabilityMonitoring(
		ctx,
		"component",
		"host",
		15*time.Second, // availabilityReportingInterval
		storage,
		registry,
	)
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
