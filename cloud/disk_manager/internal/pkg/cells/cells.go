package cells

import (
	"context"
	"slices"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type cellSelector struct {
	config          *cells_config.CellsConfig
	nbsFactory      nbs.Factory
	resourceStorage resources.Storage
}

func NewCellSelector(
	config *cells_config.CellsConfig,
	nbsFactory nbs.Factory,
	resourcesStorage resources.Storage,
) CellSelector {

	return &cellSelector{
		config:          config,
		nbsFactory:      nbsFactory,
		resourceStorage: resourcesStorage,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) GetZoneIDForExistingDisk(
	ctx context.Context,
	diskID *disk_manager.DiskId,
) (string, error) {

	diskMeta, err := s.resourceStorage.GetDiskMeta(ctx, diskID.DiskId)
	if err != nil {
		return "", err
	}

	if diskMeta == nil {
		return "", common.NewInvalidArgumentError(
			"no such disk: %v",
			diskID,
		)
	}

	if diskMeta.ZoneID != diskID.ZoneId &&
		!s.isCellOfZone(diskMeta.ZoneID, diskID.ZoneId) {
		return "", common.NewInvalidArgumentError(
			"provided zone ID %v does not match with an actual zone ID %v",
			diskID.ZoneId,
			diskMeta.ZoneID,
		)
	}

	return diskMeta.ZoneID, nil
}

func (s *cellSelector) PrepareZoneID(
	ctx context.Context,
	diskID *types.Disk,
	folderID string,
) (string, error) {

	diskMeta, err := s.resourceStorage.GetDiskMeta(ctx, diskID.DiskId)
	if err != nil {
		return "", err
	}

	if diskMeta != nil {
		return diskMeta.ZoneID, nil
	}

	return s.selectCell(diskID, folderID), nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *cellSelector) selectCell(
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

func (s *cellSelector) isCellOfZone(cellID string, zoneID string) bool {
	return slices.Contains(s.getCells(zoneID), cellID)
}

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
