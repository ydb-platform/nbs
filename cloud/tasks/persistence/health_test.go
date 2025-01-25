package persistence

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestHealthCheckMetric(t *testing.T) {
	registry := mocks.NewRegistryMock()
	healthCheck := newHealthCheck("test", registry)

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

	healthCheck.accountQuery(nil)
	healthCheck.accountQuery(nil)
	healthCheck.accountQuery(nil)
	healthCheck.accountQuery(errors.NewEmptyRetriableError())

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
}
