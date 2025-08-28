package cells

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	// Returns the most suitable Cell ID for the given valid zone. If the zone
	// is not divided into cells, or cells are not allowed for the folder,
	// returns the original zone.
	SelectCell(
		disk *types.Disk,
		folderID string,
	) string

	IsCellOfZone(cellID string, zoneID string) bool
}
