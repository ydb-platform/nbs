package persistence

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type HealthCheck struct {
	queriesCount              uint64
	successQueriesCount       uint64
	storage                   HealthStorage
	registry                  metrics.Registry
	metricsCollectionInterval time.Duration
}

func (h *HealthCheck) reportSuccessRate(ctx context.Context) {
	if h.queriesCount == 0 {
		h.registry.Gauge("successRate").Set(0)
		return
	}

	h.registry.Gauge("successRate").Set(float64(h.successQueriesCount) / float64(h.queriesCount))
	h.storage.HeartbeatNode(ctx, time.Now())
}

////////////////////////////////////////////////////////////////////////////////

func (h *HealthCheck) AccountQuery(err error) {
	h.queriesCount++
	if err == nil {
		h.successQueriesCount++
	}
}

func NewHealthCheck(
	ctx context.Context,
	componentName string,
	storage HealthStorage,
	registry metrics.Registry,
) *HealthCheck {

	subRegistry := registry.WithTags(map[string]string{
		"component": componentName,
	})

	h := HealthCheck{
		storage:                   storage,
		registry:                  subRegistry,
		metricsCollectionInterval: 15 * time.Second, // todo
	}

	go func() {
		ticker := time.NewTicker(h.metricsCollectionInterval)
		defer ticker.Stop()

		for range ticker.C {
			h.reportSuccessRate(ctx)
		}
	}()

	return &h
}
