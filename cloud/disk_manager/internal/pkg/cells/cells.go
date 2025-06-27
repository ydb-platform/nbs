package cells

import (
	"context"
	"slices"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
)

////////////////////////////////////////////////////////////////////////////////

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
	req *disk_manager.CreateDiskRequest,
) string {

	if !s.isFolderAllowed(req.FolderId) {
		return req.DiskId.ZoneId
	}

	cells := s.GetCells(req.DiskId.ZoneId)

	return cells[0]
}

func (s *cellSelector) IsCellOfZone(cellID string, zoneID string) bool {
	return slices.Contains(s.GetCells(zoneID), cellID)
}

func (s *cellSelector) GetCells(zoneID string) []string {
	cells, ok := s.config.Cells[zoneID]
	if !ok {
		return []string{}
	}

	return cells.Cells
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) isFolderAllowed(folderID string) bool {
	if slices.Contains(s.config.GetFolderDenyList(), folderID) {
		return false
	}

	return len(s.config.GetFolderAllowList()) == 0 ||
		slices.Contains(s.config.GetFolderAllowList(), folderID)
}
