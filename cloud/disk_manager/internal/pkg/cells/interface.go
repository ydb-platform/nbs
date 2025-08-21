package cells

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	SelectCell(
		diskID *types.Disk,
		folderID string,
	) string

	SelectCellForLocalDisk(
		ctx context.Context,
		diskID *types.Disk,
		folderID string,
		agentIDs []string,
	) (string, error)

	IsCellOfZone(cellID string, zoneID string) bool
}
