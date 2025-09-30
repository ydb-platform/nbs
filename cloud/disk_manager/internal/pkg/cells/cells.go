package cells

import (
	"context"
	"slices"
	"sort"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type cellSelector struct {
	config     *cells_config.CellsConfig
	storage    storage.Storage
	nbsFactory nbs.Factory
}

func NewCellSelector(
	config *cells_config.CellsConfig,
	storage storage.Storage,
	nbsFactory nbs.Factory,
) CellSelector {

	return &cellSelector{
		config:     config,
		storage:    storage,
		nbsFactory: nbsFactory,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) SelectCell(
	ctx context.Context,
	zoneID string,
	folderID string,
	kind types.DiskKind,
) (nbs.Client, error) {

	cellID, err := s.selectCell(ctx, zoneID, folderID, kind)
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
	ctx context.Context,
	zoneID string,
	folderID string,
	kind types.DiskKind,
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

	switch s.config.GetCellSelectionPolicy() {
	case cells_config.CellSelectionPolicy_FIRST_IN_CONFIG:
		return cells[0], nil
	case cells_config.CellSelectionPolicy_MAX_FREE_BYTES:
		capacities, err := s.storage.GetRecentClusterCapacities(ctx, zoneID, kind)
		if err != nil {
			return "", err
		}

		sort.Slice(capacities, func(i, j int) bool {
			return capacities[i].FreeBytes > capacities[j].FreeBytes
		})

		return capacities[0].CellID, nil
	default:
		return "", errors.NewNonCancellableErrorf(
			"unknown cell selection policy: %q",
			s.config.CellSelectionPolicy,
		)
	}
}
