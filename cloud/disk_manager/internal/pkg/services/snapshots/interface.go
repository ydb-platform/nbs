package snapshots

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Service interface {
	ExportSnapshot(
		context.Context,
		*disk_manager.ExportSnapshotRequest) (string,
		error,
	)

	CreateSnapshot(
		ctx context.Context,
		req *disk_manager.CreateSnapshotRequest,
	) (string, error)

	DeleteSnapshot(
		ctx context.Context,
		req *disk_manager.DeleteSnapshotRequest,
	) (string, error)
}
