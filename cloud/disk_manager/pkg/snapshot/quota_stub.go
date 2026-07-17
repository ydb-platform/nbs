package snapshot

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

type stubSnapshotStorageQuotaReporter struct {
	registry metrics.Registry
}

func (r *stubSnapshotStorageQuotaReporter) Report(_ context.Context) error {
	r.registry.Gauge("snapshots/quotas/used_bytes").Set(0)
	r.registry.Gauge("snapshots/quotas/limit_bytes").Set(1000)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

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
