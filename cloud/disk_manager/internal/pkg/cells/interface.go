package cells

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	GetZoneIDForExistingDisk(
		ctx context.Context,
		diskID *disk_manager.DiskId,
	) (string, error)

	PrepareZoneID(
		ctx context.Context,
		diskID *types.Disk,
		folderID string,
	) (string, error)
}
