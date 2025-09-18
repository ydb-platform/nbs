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
	config     *cells_config.CellsConfig
	nbsFactory nbs.Factory
}

func NewCellSelector(
	config *cells_config.CellsConfig,
	nbsFactory nbs.Factory,
) CellSelector {

	return &cellSelector{
		config:     config,
		nbsFactory: nbsFactory,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) SelectCell(
	ctx context.Context,
	zoneID string,
	folderID string,
) (nbs.Client, error) {

	cellID, err := s.selectCell(zoneID, folderID)
	if err != nil {
		return nil, err
	}

	return s.nbsFactory.GetClient(ctx, cellID)
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

func (s *cellSelector) isCell(zoneID string) bool {
	for _, cells := range s.config.Cells {
		if slices.Contains(cells.Cells, zoneID) {
			return true
		}
	}

	return false
}

func (s *cellSelector) selectCell(
	zoneID string,
	folderID string,
) (string, error) {

	if s.config == nil {
		return zoneID, nil
	}

	if !s.isFolderAllowed(folderID) {
		return zoneID, nil
	}

	cells := s.getCells(zoneID)

	if len(cells) == 0 {
		if s.isCell(zoneID) {
			return zoneID, nil
		}

		return "", errors.NewNonCancellableErrorf(
			"incorrect zone ID provided: %q",
			zoneID,
		)
	}

	return cells[0], nil
}
