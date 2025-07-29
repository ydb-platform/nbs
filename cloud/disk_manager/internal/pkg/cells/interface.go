package cells

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	GetZoneIDForExistingDisk(
		ctx context.Context,
		diskID *disk_manager.DiskId,
	) (string, error)

	PrepareZoneID(
		ctx context.Context,
		req *disk_manager.CreateDiskRequest,
	) (string, error)

	isCellOfZone(cellID string, zoneID string) bool
}
