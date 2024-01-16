package health

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const healthCheckPeriod = 30 * time.Second
const healthCheckTimeout = 5 * time.Second

////////////////////////////////////////////////////////////////////////////////

func MonitorHealth(
	ctx context.Context,
	registry metrics.Registry,
	db *persistence.YDBClient,
	nbsFactory nbs.Factory,
	s3 *persistence.S3Client,
	s3Bucket string,
	healthChangedCallback func(bool),
) {

	healthCheck := healthcheck{
		interval: healthCheckPeriod,
		checks:   make(map[string]check, 2),
		registry: registry,
	}

	bootDiskCheck := newBootDiskCheck()
	bootDiskCheck.Start(ctx)
	healthCheck.checks["health/bootDisk"] = bootDiskCheck

	ydbCheck := newYDBCheck(db)
	healthCheck.checks["health/ydb"] = ydbCheck

	if s3 != nil && len(s3Bucket) != 0 {
		s3Check := newS3Check(s3, s3Bucket)
		healthCheck.checks["health/s3"] = s3Check
	}

	for _, zone := range nbsFactory.GetZones() {
		nbsCheck := newNbsCheck(nbsFactory, zone)
		healthCheck.checks[fmt.Sprintf("health/nbs/%v", zone)] = nbsCheck
	}

	healthCheck.flapSuppressors = make(map[string]flapSuppressor)
	for name := range healthCheck.checks {
		healthCheck.flapSuppressors[name] = newFlapSuppressor()
	}

	healthCheck.monitorHealth(ctx)

	go evictionLoop(ctx, healthCheck.flapSuppressors, healthChangedCallback)
}

type healthcheck struct {
	checks          map[string]check
	flapSuppressors map[string]flapSuppressor
	interval        time.Duration
	registry        metrics.Registry
}

type check interface {
	Check(ctx context.Context) bool
}

// Iterates over suppressed checks, if any of the checks are failed,
// releases current tasks.
// When all the checks are successful again, restarts the tasks.
func evictionLoop(
	ctx context.Context,
	flapSuppressors map[string]flapSuppressor,
	healthChangedCallback func(bool),
) {

	evicted := false
	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()

mainLoop:
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		for name, flapSuppressor := range flapSuppressors {
			// If the check returns false, notify the task controller.
			if !flapSuppressor.status() {
				if evicted {
					continue mainLoop
				}

				logging.Warn(ctx, "Evicting the node because %q check has failed", name)
				healthChangedCallback(false)
				evicted = true
				continue mainLoop
			}
		}

		if evicted {
			healthChangedCallback(true)
			evicted = false
		}
	}
}

func (c *healthcheck) monitorHealth(ctx context.Context) {
	for name := range c.checks {
		go c.checkLoop(ctx, name)
	}
}

func (c *healthcheck) checkLoop(ctx context.Context, name string) {
	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()

	for range ticker.C {
		result := c.check(ctx, name)
		flapSuppressor := c.flapSuppressors[name]
		flapSuppressor.recordCheck(result)
	}
}

func (c *healthcheck) check(ctx context.Context, name string) bool {
	check := c.checks[name]

	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()

	success := check.Check(ctx)

	gauge := 1.0
	if !success {
		gauge = 0
	}

	c.registry.Gauge(name).Set(gauge)
	return success
}
