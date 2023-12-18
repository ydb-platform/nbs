package main

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/schema"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	schema.Create("disk-manager-init-db", "/etc/disk-manager/server-config.txt")
}
