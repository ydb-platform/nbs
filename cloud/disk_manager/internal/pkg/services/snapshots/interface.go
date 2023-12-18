package snapshots

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/api/yandex/cloud/priv/disk_manager/v1"
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
