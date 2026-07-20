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

////////////////////////////////////////////////////////////////////////////////

type stubSnapshotStorageQuotaReporter struct {
	registry metrics.Registry
}

func (r *stubSnapshotStorageQuotaReporter) Report(_ context.Context) error {
	r.registry.Gauge("snapshots/quotas/usedBytes").Set(0)
	r.registry.Gauge("snapshots/quotas/limitBytes").Set(1000)
	return nil
}

func NewStubSnapshotStorageQuotaReporter(
	registry metrics.Registry,
	config *Config,
	_ auth.Credentials,
) (SnapshotStorageQuotaReporter, error) {

	registry = registry.WithTags(map[string]string{
		"bucket": config.GetS3Bucket(),
	})

	return &stubSnapshotStorageQuotaReporter{
		registry: registry,
	}, nil
}
