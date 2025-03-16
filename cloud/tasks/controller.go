package tasks

import (
	"context"
	"sync/atomic"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

// Controller starts/stops task runners.
type Controller interface {
	StartRunners() error

	HealthChangedCallback(status bool)
}

////////////////////////////////////////////////////////////////////////////////

type controller struct {
	runContext    context.Context
	runnersCancel context.CancelFunc
	running       atomic.Bool

	taskStorage                   storage.Storage
	availabilityMonitoringStorage *persistence.AvailabilityMonitoringStorageYDB
	registry                      *Registry
	runnerMetricsRegistry         metrics.Registry
	config                        *tasks_config.TasksConfig
	host                          string
}

////////////////////////////////////////////////////////////////////////////////

func (c *controller) StartRunners() error {
	return c.startRunners(c.runContext)
}

func (c *controller) HealthChangedCallback(status bool) {
	if !c.config.GetNodeEvictionEnabled() {
		return
	}

	// Evict node.
	if !status {
		c.stopRunners()
		logging.Debug(c.runContext, "Stopped runners due to health changed")
	}
	err := c.startRunners(c.runContext)
	if err != nil {
		logging.Error(c.runContext, "Could not restart runners, reason: %v", err)
	} else {
		logging.Debug(c.runContext, "Restarted runners")
	}
}

////////////////////////////////////////////////////////////////////////////////

func (c *controller) startRunners(ctx context.Context) error {
	if c.running.Load() {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	_ = cancel // hack to prevent lostcancel linter warning messages.
	err := StartRunners(
		ctx,
		c.taskStorage,
		c.availabilityMonitoringStorage,
		c.registry,
		c.runnerMetricsRegistry,
		c.config,
		c.host,
	)
	if err != nil {
		return err
	}

	c.runnersCancel = cancel
	c.running.Store(true)
	return nil
}

func (c *controller) stopRunners() {
	if !c.running.Load() {
		return
	}
	c.runnersCancel()
	c.running.Store(false)
}

////////////////////////////////////////////////////////////////////////////////

func NewController(
	ctx context.Context,
	taskStorage storage.Storage,
	availabilityMonitoringStorage *persistence.AvailabilityMonitoringStorageYDB,
	registry *Registry,
	runnerMetricsRegistry metrics.Registry,
	config *tasks_config.TasksConfig,
	host string,
) Controller {

	runContext := logging.WithComponent(ctx, logging.ComponentTaskRunner)

	return &controller{
		runContext:                    runContext,
		running:                       atomic.Bool{},
		taskStorage:                   taskStorage,
		availabilityMonitoringStorage: availabilityMonitoringStorage,
		registry:                      registry,
		runnerMetricsRegistry:         runnerMetricsRegistry,
		config:                        config,
		host:                          host,
	}
}
