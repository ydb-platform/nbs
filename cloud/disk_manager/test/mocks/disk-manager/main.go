package main

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/app"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/snapshot"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	app.Run(
		"disk-manager",
		"/etc/disk-manager/server-config.txt",
		metrics.NewPrometheusRegistry,
		func(config *auth.AuthConfig, creds auth.Credentials) (auth.Authorizer, error) {
			return auth.NewStubAuthorizer(), nil
		},
		snapshot.NewStubSnapshotStorageQuotaReporter,
	)
}
