package persistence

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type AvailabilityMonitoring struct {
	component           string
	host                string
	queriesCount        uint64
	successQueriesCount uint64

	storage                      *AvailabilityMonitoringStorageYDB
	registry                     metrics.Registry
	successRateReportingInterval time.Duration

	mutex sync.Mutex
}

func (m *AvailabilityMonitoring) AccountQuery(err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.queriesCount++
	if err == nil {
		m.successQueriesCount++
	}
}

////////////////////////////////////////////////////////////////////////////////

func (m *AvailabilityMonitoring) reportSuccessRate(ctx context.Context) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.queriesCount == 0 {
		m.registry.Gauge("successRate").Set(1)
		return
	}

	successRate := float64(m.successQueriesCount) / float64(m.queriesCount)
	m.registry.Gauge("successRate").Set(successRate)
	err := m.storage.UpdateSuccessRate(
		ctx,
		m.component,
		m.host,
		successRate,
		time.Now(),
	)
	if err != nil {
		logging.Error(ctx, "got error %v on success rate updating", err)
	}
}

////////////////////////////////////////////////////////////////////////////////

func NewAvailabilityMonitoring(
	ctx context.Context,
	component string,
	host string,
	successRateReportingInterval time.Duration,
	storage *AvailabilityMonitoringStorageYDB,
	registry metrics.Registry,
) *AvailabilityMonitoring {

	subRegistry := registry.WithTags(map[string]string{
		"component": component,
		"host":      host,
	})

	m := AvailabilityMonitoring{
		component: component,
		host:      host,

		storage:                      storage,
		registry:                     subRegistry,
		successRateReportingInterval: successRateReportingInterval,
	}

	go func() {
		ticker := time.NewTicker(m.successRateReportingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.reportSuccessRate(ctx)
			}
		}
	}()

	return &m
}
