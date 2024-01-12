package health

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const bootDiskProbePeriod = time.Minute
const bootDiskProbeAgeThreshold = 2 * time.Minute

////////////////////////////////////////////////////////////////////////////////

type bootDiskCheck struct {
	filePath string
	buffer   []byte

	lastCheck atomic.Pointer[checkResult]
}

type checkResult struct {
	timestamp time.Time
	success   bool
}

func newBootDiskCheck() *bootDiskCheck {
	check := bootDiskCheck{
		// Reading binary itself to check if disk is available.
		filePath: os.Args[0],
		buffer:   make([]byte, 4096),
	}
	check.updateResult(true)
	return &check
}

func (d *bootDiskCheck) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(bootDiskProbePeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				d.updateResult(d.probe(ctx))
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (d *bootDiskCheck) Check(ctx context.Context) bool {
	lastCheckResult := *d.lastCheck.Load()
	if time.Since(lastCheckResult.timestamp) > bootDiskProbeAgeThreshold {
		logging.Warn(ctx,
			"Boot disk health check failed because probe is hanging. Last check was at %v",
			lastCheckResult.timestamp.Format(time.RFC3339),
		)
		return false
	}

	return lastCheckResult.success
}

////////////////////////////////////////////////////////////////////////////////

func (d *bootDiskCheck) probe(ctx context.Context) bool {
	f, err := os.OpenFile(d.filePath, os.O_RDONLY|syscall.O_DIRECT, 0600)
	if err != nil {
		logging.Warn(ctx, "Boot disk health check failed to open file: %v", err)
		return false
	}
	defer f.Close()

	_, err = f.Read(d.buffer)
	if err != nil && err != io.EOF {
		logging.Warn(ctx, "Boot disk health check failed to read file: %v", err)
		return false
	}

	return true
}

func (d *bootDiskCheck) updateResult(success bool) {
	now := time.Now()
	result := checkResult{
		timestamp: now,
		success:   true,
	}
	d.lastCheck.Store(&result)
}
