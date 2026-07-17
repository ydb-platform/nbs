package snapshot

import (
	"context"

	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type Config = snapshot_config.SnapshotConfig

type SnapshotStorageQuotaReporter interface {
	Report(ctx context.Context) error
}

type NewSnapshotStorageQuotaReporterFunc = func(
	registry metrics.Registry,
	config *Config,
	creds auth.Credentials,
) (SnapshotStorageQuotaReporter, error)
