package cells

import (
	"context"
	"slices"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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
	diskID *types.Disk,
	folderID string,
) string {

	if !s.isFolderAllowed(folderID) {
		return diskID.ZoneId
	}

	cells := s.getCells(diskID.ZoneId)

	if len(cells) == 0 {
		// We end up here if a zone not divided into cells or a cell
		// of a zone is provided as ZoneId.
		return diskID.ZoneId
	}

	return cells[0]
}

func (s *cellSelector) SelectCellForLocalDisk(
	ctx context.Context,
	diskID *types.Disk,
	folderID string,
	agentIDs []string,
) (string, error) {

	if !s.isFolderAllowed(folderID) {
		return diskID.ZoneId, nil
	}

	cells := s.getCells(diskID.ZoneId)

	if len(cells) == 0 {
		// We end up here if a zone not divided into cells or a cell
		// of a zone is provided as ZoneId.
		return diskID.ZoneId, nil
	}

	for _, cellID := range cells {
		client, err := s.nbsFactory.GetClient(ctx, cellID)
		if err != nil {
			return "", err
		}

		infos, err := client.QueryAvailableStorage(ctx, agentIDs)
		if err != nil {
			return "", err
		}

		if len(infos) == 0 {
			continue
		}

		// If the only available storage info is empty, agent is unavailable.
		if len(infos) == 1 &&
			infos[0].ChunkSize == 0 &&
			infos[0].ChunkCount == 0 {
			continue
		}

		return cellID, nil
	}

	return "",
		errors.NewRetriableErrorf("There are no cells with such agents available: %v", agentIDs)
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
