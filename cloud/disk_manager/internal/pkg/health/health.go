package health

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const healthCheckPeriod = 30 * time.Second
const healthCheckTimeout = 5 * time.Second

////////////////////////////////////////////////////////////////////////////////

func MonitorHealth(
	ctx context.Context,
	registry metrics.Registry,
	ydb *persistence.YDBClient,
	nbsFactory nbs.Factory,
	s3 *persistence.S3Client,
	s3Bucket string,
	healthCheckChangedCallback func(bool),
) {

	healthChecker := healthcheck{
		interval: healthCheckPeriod,
		checkers: make(map[string]checker, 2),
	}

	bootDiskChecker := newBootDiskCheck()
	bootDiskChecker.Start(ctx)
	healthChecker.checkers["health/bootDisk"] = bootDiskChecker

	ydbChecker := newYDBCheck(ydb)
	healthChecker.checkers["health/ydb"] = ydbChecker

	if s3 != nil && len(s3Bucket) != 0 {
		s3Checker := newS3Check(s3, s3Bucket)
		healthChecker.checkers["health/s3"] = s3Checker
	}

	for _, zone := range nbsFactory.GetZones() {
		nbsChecker := newNbsCheck(nbsFactory, zone)
		healthChecker.checkers[fmt.Sprintf("health/nbs/%v", zone)] = nbsChecker
	}

	suppressors := make(map[string]flapSuppressor)
	for name := range healthChecker.checkers {
		suppressors[name] = newFlapSuppressor()
	}

	healthChecker.monitorHealth(ctx, registry, suppressors)

	go detectorLoop(ctx, suppressors, healthCheckChangedCallback)
}

type healthcheck struct {
	checkers map[string]checker
	interval time.Duration
}

type checker interface {
	Check(ctx context.Context) bool
}

// Iterates over suppressed checks, if any of the checks are failed,
// releases current tasks.
// When all the checks are successful again, restarts the tasks.
func detectorLoop(
	ctx context.Context,
	healthChecks map[string]flapSuppressor,
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

		for name, healthCheck := range healthChecks {
			// If the check returns false, notify the task controller.
			if !healthCheck.status() {
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

func (h *healthcheck) monitorHealth(
	ctx context.Context,
	registry metrics.Registry,
	suppressors map[string]flapSuppressor,
) {

	for name, check := range h.checkers {
		go func(name string, check checker) {
			h.checkLoop(ctx, registry, name, check, suppressors[name])
		}(name, check)
	}
}

func (h *healthcheck) checkLoop(
	ctx context.Context,
	registry metrics.Registry,
	name string,
	check checker,
	suppressor flapSuppressor,
) {

	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()

	for range ticker.C {
		checkResult := h.check(ctx, registry, name, check)
		suppressor.recordCheck(checkResult)
	}
}

func (h *healthcheck) check(
	ctx context.Context,
	registry metrics.Registry,
	name string,
	check checker,
) bool {

	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()

	success := check.Check(ctx)

	gauge := 1.0
	if !success {
		gauge = 0
	}

	registry.Gauge(name).Set(gauge)
	return success
}
