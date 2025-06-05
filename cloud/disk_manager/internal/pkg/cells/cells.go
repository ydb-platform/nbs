package cells

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
<<<<<<< HEAD
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
)

////////////////////////////////////////////////////////////////////////////////
=======
)

// //////////////////////////////////////////////////////////////////////////////
>>>>>>> add cells component

type cellSelector struct {
	config *cells_config.CellsConfig
}

func NewCellSelector(
	config *cells_config.CellsConfig,
) CellSelector {

	return &cellSelector{
		config: config,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) SelectCell(
	ctx context.Context,
	disk *disk_manager.DiskId,
) string {

	cells := s.getCells(disk.ZoneId)

	if len(cells) == 0 {
		// We end up here if a zone not divided into cells or a cell
		// of a zone is provided as ZoneId.
		return disk.ZoneId
	}

	return cells[0]
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) getCells(zoneID string) []string {
	cells, ok := s.config.Cells[zoneID]
	if !ok {
		return []string{}
	}

	return cells.Cells
}
