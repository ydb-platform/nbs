package persistence

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *YDBClient,
) *storageYDB {

	config := &tasks_config.TasksConfig{}
	err := CreateYDBTables(
		ctx,
		config,
		db,
		false, // dropUnusedColums
	)
	require.NoError(t, err)

	storage := NewStorage(config, db)
	require.NoError(t, err)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

func TestHealthCheckMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	storage := newStorage(t, ctx, db)
	registry := mocks.NewRegistryMock()

	availabilityMonitoring := NewAvailabilityMonitoring(
		ctx,
		"component",
		"host",
		storage,
		registry,
	)

	gaugeSetWg := sync.WaitGroup{}

	gaugeSetWg.Add(1)
	registry.GetGauge(
		"successRate",
		map[string]string{"component": "test"},
	).On("Set", float64(0)).Once().Run(
		func(args mock.Arguments) {
			gaugeSetWg.Done()
		},
	)
	gaugeSetWg.Wait()

	availabilityMonitoring.AccountQuery(nil)
	availabilityMonitoring.AccountQuery(nil)
	availabilityMonitoring.AccountQuery(nil)
	availabilityMonitoring.AccountQuery(errors.NewEmptyRetriableError())

	gaugeSetWg.Add(1)
	registry.GetGauge(
		"successRate",
		map[string]string{"component": "test"},
	).On("Set", float64(3.0/4.0)).Once().Run(
		func(args mock.Arguments) {
			gaugeSetWg.Done()
		},
	)
	gaugeSetWg.Wait()

	registry.AssertAllExpectations(t)

	require.NotNil(t, nil)
}
