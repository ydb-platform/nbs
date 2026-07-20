package snapshot

import (
	"context"

	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Config = snapshot_config.SnapshotConfig

// SnapshotStorageQuotaReporter.Report SHOULD
// report the following metrics: snapshots/quotas/usedBytes, snapshots/quotas/limitBytes
// albeit other implementations may report additional metrics as well.
type SnapshotStorageQuotaReporter interface {
	Report(ctx context.Context) error
}

type NewSnapshotStorageQuotaReporterFunc = func(
	registry metrics.Registry,
	config *Config,
	creds auth.Credentials,
) (SnapshotStorageQuotaReporter, error)

////////////////////////////////////////////////////////////////////////////////

type emptySnapshotStorageQuotaReporter struct{}

func (r *emptySnapshotStorageQuotaReporter) Report(_ context.Context) error {
	return nil
}

func NewEmptySnapshotStorageQuotaReporter(
	_ metrics.Registry,
	_ *Config,
	_ auth.Credentials,
) (SnapshotStorageQuotaReporter, error) {

	return &emptySnapshotStorageQuotaReporter{}, nil
}
