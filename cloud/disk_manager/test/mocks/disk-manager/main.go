package main

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/app"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/snapshot"
)

////////////////////////////////////////////////////////////////////////////////

type stubSnapshotStorageQuotaReporter struct {
	registry metrics.Registry
}

func (r *stubSnapshotStorageQuotaReporter) Report(_ context.Context) error {
	r.registry.Gauge("snapshots/quotas/usedBytes").Set(0)
	r.registry.Gauge("snapshots/quotas/limitBytes").Set(1000)
	return nil
}

func newStubSnapshotStorageQuotaReporter(
	registry metrics.Registry,
	config *snapshot.Config,
	_ auth.Credentials,
) (snapshot.SnapshotStorageQuotaReporter, error) {

	registry = registry.WithTags(map[string]string{
		"bucket": config.GetS3Bucket(),
	})

	return &stubSnapshotStorageQuotaReporter{
		registry: registry,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	app.Run(
		"disk-manager",
		"/etc/disk-manager/server-config.txt",
		metrics.NewPrometheusRegistry,
		func(config *auth.AuthConfig, creds auth.Credentials) (auth.Authorizer, error) {
			return auth.NewStubAuthorizer(), nil
		},
		newStubSnapshotStorageQuotaReporter,
	)
}
