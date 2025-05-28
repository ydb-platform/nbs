package shards

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	SelectShard(
		ctx context.Context,
		disk *disk_manager.DiskId,
	) string
}
