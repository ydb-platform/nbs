package shards

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	PickShard(
		ctx context.Context,
		disk *disk_manager.DiskId,
		folderID string,
	) (string, error)
}
