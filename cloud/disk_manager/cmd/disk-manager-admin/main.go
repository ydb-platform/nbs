package main

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/admin"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	admin.Run(
		"disk-manager-admin",
		"/etc/disk-manager/client-config.txt",
		"/etc/disk-manager/server-config.txt",
	)
}
