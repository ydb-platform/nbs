package cells

import (
	"context"
	"slices"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type cellSelector struct {
	config  *cells_config.CellsConfig
	factory nbs.Factory
}

func NewCellSelector(
	config *cells_config.CellsConfig,
	factory nbs.Factory,
) CellSelector {

	return &cellSelector{
		config:  config,
		factory: factory,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) SelectCell(
	ctx context.Context,
	zoneID string,
	folderID string,
) (nbs.Client, error) {

	if s.config == nil {
		return s.factory.GetClient(ctx, zoneID)
	}

	if !s.isFolderAllowed(folderID) {
		return s.factory.GetClient(ctx, zoneID)
	}

	if s.isOneOfCells(zoneID) {
		return s.factory.GetClient(ctx, zoneID)
	}

	cells := s.getCells(zoneID)

	if len(cells) == 0 {
		return nil, errors.NewNonCancellableErrorf(
			"incorrect zone ID provided: %q",
			zoneID,
		)
	}

	return s.factory.GetClient(ctx, cells[0])
}

func (s *cellSelector) IsCellOfZone(cellID string, zoneID string) bool {
	return slices.Contains(s.getCells(zoneID), cellID)
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) getCells(zoneID string) []string {
	cells, ok := s.config.Cells[zoneID]
	if !ok {
		return []string{}
	}

	return cells.Cells
}

func (s *cellSelector) isFolderAllowed(folderID string) bool {
	if slices.Contains(s.config.GetFolderDenyList(), folderID) {
		return false
	}

	return len(s.config.GetFolderAllowList()) == 0 ||
		slices.Contains(s.config.GetFolderAllowList(), folderID)
}

func (s *cellSelector) isOneOfCells(zoneID string) bool {
	for _, cells := range s.config.Cells {
		if slices.Contains(cells.Cells, zoneID) {
			return true
		}
	}

	return false
}
