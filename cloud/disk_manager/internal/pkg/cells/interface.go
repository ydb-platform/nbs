package cells

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelector interface {
	// Returns copy of disk with replaced zone ID with cell ID.
	ReplaceZoneIdWithCellIdInDiskMeta(
		ctx context.Context,
		storage resources.Storage,
		disk *types.Disk,
	) (*types.Disk, error)

	// Returns an nbs Client for the most suitable cell in the specified zone.
	// If the Cells mechanism is not enabled for this folder, returns an
	// nbs Client for specified zone.
	SelectCell(
		ctx context.Context,
		zoneID string,
		folderID string,
		kind types.DiskKind,
	) (nbs.Client, error)

	SelectCellForLocalDisk(
		ctx context.Context,
		zoneID string,
		agentIDs []string,
	) (nbs.Client, error)

	ZoneContainsCell(zoneID string, cellID string) bool
}
