package cells

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	// Returns an nbs Client for the most suitable cell in the specified zone.
	// If the Cells mechanism is not enabled for this folder, returns an
	// nbs Client for specified zone.
	SelectCell(
		ctx context.Context,
		zoneID string,
		folderID string,
		kind types.DiskKind,
	) (nbs.Client, error)

	IsCellOfZone(cellID string, zoneID string) bool
}
