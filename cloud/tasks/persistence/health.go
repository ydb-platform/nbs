package persistence

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type AvailabilityMonitoring struct {
	componentName       string
	host                string
	queriesCount        uint64
	successQueriesCount uint64

	storage                   *storageYDB
	registry                  metrics.Registry
	metricsCollectionInterval time.Duration
}

func (h *AvailabilityMonitoring) reportSuccessRate(ctx context.Context) {
	if h.queriesCount == 0 {
		h.registry.Gauge("successRate").Set(0)
		return
	}

	successRate := float64(h.successQueriesCount) / float64(h.queriesCount)
	h.registry.Gauge("successRate").Set(successRate)
	h.storage.UpdateAvailabilityRate(ctx, h.componentName, h.host, successRate, time.Now())
}

////////////////////////////////////////////////////////////////////////////////

func (h *AvailabilityMonitoring) AccountQuery(err error) {
	h.queriesCount++
	if err == nil {
		h.successQueriesCount++
	}
}

func NewAvailabilityMonitoring(
	ctx context.Context,
	componentName string,
	host string,
	storage *storageYDB,
	registry metrics.Registry,
) *AvailabilityMonitoring {

	subRegistry := registry.WithTags(map[string]string{
		"component": componentName,
	})

	h := AvailabilityMonitoring{
		componentName: componentName,
		host:          host,

		storage:                   storage,
		registry:                  subRegistry,
		metricsCollectionInterval: 15 * time.Second, // todo
	}

	go func() {
		ticker := time.NewTicker(h.metricsCollectionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.reportSuccessRate(ctx)
			}
		}
	}()

	return &h
}
