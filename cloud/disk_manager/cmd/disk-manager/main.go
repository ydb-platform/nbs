package main

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/app"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	app.Run(
		"disk-manager",
		"/etc/disk-manager/server-config.txt",
		metrics.NewPrometheusRegistry,
		func(config *auth.AuthConfig, creds auth.Credentials) (auth.Authorizer, error) {
			// TODO: should use real authorizer, not stub.
			return auth.NewStubAuthorizer(), nil
		},
	)
}
