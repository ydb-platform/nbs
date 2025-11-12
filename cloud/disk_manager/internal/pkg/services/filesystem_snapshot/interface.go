package filesystem_snapshot

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	CreateFilesystemSnapshot(
		ctx context.Context,
		req *disk_manager.CreateFilesystemSnapshotRequest,
	) (string, error)

	DeleteFilesystemSnapshot(
		ctx context.Context,
		req *disk_manager.DeleteFilesystemSnapshotRequest,
	) (string, error)
}
