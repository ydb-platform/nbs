package persistence

import (
	"context"
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

	storage                          *AvailabilityMonitoringStorageYDB
	registry                         metrics.Registry
	successRateReportingInterval     time.Duration
	successRateAvailabilityTrasehold float64
}

func (m *AvailabilityMonitoring) AccountQuery(err error) {
	m.queriesCount++
	if err == nil {
		m.successQueriesCount++
	}
}

////////////////////////////////////////////////////////////////////////////////

func (m *AvailabilityMonitoring) reportSuccessRate(ctx context.Context) {
	if m.queriesCount == 0 {
		m.registry.Gauge("successRate").Set(1)
		logging.Info(ctx, "updated successRate 1!!! for host %v component %v", m.host, m.component)
		return
	}

	successRate := float64(m.successQueriesCount) / float64(m.queriesCount)
	m.registry.Gauge("successRate").Set(successRate)
	err := m.storage.UpdateSuccessRate(ctx, m.component, m.host, successRate, time.Now())
	if err != nil {
		logging.Error(ctx, "got error %v on success rate updating", err)
	}

	logging.Info(ctx, "updated successRate %v for host %v component %v", successRate, m.host, m.component)

	m.queriesCount = 0
	m.successQueriesCount = 0
}

func (m *AvailabilityMonitoring) isAvailable(results []AvailabilityMonitoringResult) bool {
	for _, result := range results {
		if result.SuccessRate < m.successRateAvailabilityTrasehold {
			return false
		}
	}

	return true
}

func (m *AvailabilityMonitoring) updateComponentsAvailability(
	ctx context.Context,
) error {

	results, err := m.storage.GetAvailabilityMonitoringResults(
		ctx,
		m.host,
		m.component,
		time.Now().Add(-5*m.successRateReportingInterval),
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "results is %v", results)

	if !m.isAvailable(results) {
		logging.Info(ctx, "returning false!!!!!")
		err := m.storage.UpdateComponentAvailability(ctx, m.host, m.component, false)
		logging.Info(ctx, "WTW!!!!!")
		logging.Info(ctx, "err is %v", err)
		if err != nil {
			logging.Info(ctx, "error occured %v", err)
			return err
		}

		results, err := m.storage.GetComponentsAvailabilityResults(ctx, m.host)
		if err != nil {
			logging.Info(ctx, "error occured %v", err)
			return err
		}

		logging.Info(ctx, "results on updating is %v", results)
	}

	return m.storage.UpdateComponentsBackFromUnavailable(ctx, m.host, m.component)
}

////////////////////////////////////////////////////////////////////////////////

func NewAvailabilityMonitoring(
	ctx context.Context,
	component string,
	host string,
	successRateReportingInterval time.Duration,
	successRateAvailabilityTrasehold float64,
	storage *AvailabilityMonitoringStorageYDB,
	registry metrics.Registry,
) (*AvailabilityMonitoring, error) {

	subRegistry := registry.WithTags(map[string]string{
		"component": component,
		"host":      host,
	})

	m := AvailabilityMonitoring{
		component: component,
		host:      host,

		storage:                          storage,
		registry:                         subRegistry,
		successRateReportingInterval:     successRateReportingInterval,
		successRateAvailabilityTrasehold: successRateAvailabilityTrasehold,
	}

	err := m.storage.UpdateComponentAvailability(ctx, m.host, m.component, true)
	if err != nil {
		return nil, err
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

				err := m.updateComponentsAvailability(ctx)
				if err != nil {
					logging.Info(ctx, "error in availability monitoring %v", err)
				}
			}
		}
	}()

	return &m, nil
}
