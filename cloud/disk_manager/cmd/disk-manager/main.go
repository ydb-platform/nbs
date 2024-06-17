package main

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/app"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/internal_auth"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	app.Run(
		"disk-manager",
		"/etc/disk-manager/server-config.txt",
		metrics.NewPrometheusRegistry,
		internal_auth.NewAuthenticator,
	)
}
