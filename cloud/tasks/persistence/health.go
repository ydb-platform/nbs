package persistence

import (
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type healthCheck struct {
	queriesCount              uint64
	successQueriesCount       uint64
	registry                  metrics.Registry
	metricsCollectionInterval time.Duration
}

func (h *healthCheck) accountQuery(err error) {
	h.queriesCount++
	if err == nil {
		h.successQueriesCount++
	}
}

func (h *healthCheck) reportSuccessRate() {
	if h.queriesCount == 0 {
		h.registry.Gauge("successRate").Set(0)
		return
	}

	h.registry.Gauge("successRate").Set(float64(h.successQueriesCount) / float64(h.queriesCount))
}

func newHealthCheck(componentName string, registry metrics.Registry) *healthCheck {
	subRegistry := registry.WithTags(map[string]string{
		"component": componentName,
	})

	h := healthCheck{
		registry:                  subRegistry,
		metricsCollectionInterval: 15 * time.Second, // todo
	}

	go func() {
		ticker := time.NewTicker(h.metricsCollectionInterval)
		defer ticker.Stop()

		for range ticker.C {
			h.reportSuccessRate()
		}
	}()

	return &h
}
