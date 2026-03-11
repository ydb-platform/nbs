package mocks

import (
	"github.com/stretchr/testify/mock"
	client_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type MetricsMock struct {
	mock.Mock
}

func (m *MetricsMock) OnError(err error) {
	m.Called(err)
}

func (m *MetricsMock) StatRequest(name string) func(err *error) {
	args := m.Called(name)
	return args.Get(0).(func(err *error))
}

////////////////////////////////////////////////////////////////////////////////

func NewMetricsMock() *MetricsMock {
	return &MetricsMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that MetricsMock implements Metrics.
func assertMetricsMockIsMetrics(m *MetricsMock) client_metrics.Metrics {
	return m
}
