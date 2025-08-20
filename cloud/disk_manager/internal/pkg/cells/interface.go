package cells

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	SelectCell(
		diskID *types.Disk,
		folderID string,
	) string

	IsCellOfZone(cellID string, zoneID string) bool
}
