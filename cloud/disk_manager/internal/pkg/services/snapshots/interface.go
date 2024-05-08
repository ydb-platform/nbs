package snapshots

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	CreateSnapshot(
		ctx context.Context,
		req *disk_manager.CreateSnapshotRequest,
	) (string, error)

	DeleteSnapshot(
		ctx context.Context,
		req *disk_manager.DeleteSnapshotRequest,
	) (string, error)
}
