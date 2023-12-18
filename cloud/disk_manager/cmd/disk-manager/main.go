package main

import (
	"net/http"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/app"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	app.Run(
		"disk-manager",
		"/etc/disk-manager/server-config.txt",
		func(mux *http.ServeMux, path string) metrics.Registry {
			// TODO: should use real metrics registry, not stub.
			return metrics.NewEmptyRegistry()
		},
		func(config *auth.AuthConfig, creds auth.Credentials) (auth.Authorizer, error) {
			// TODO: should use real authorizer, not stub.
			return auth.NewStubAuthorizer(), nil
		},
	)
}
