package cells

import (
	"context"
	"slices"
	"sync"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"golang.org/x/sync/errgroup"
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

func (s *cellSelector) SelectCellForLocalDisk(
	ctx context.Context,
	zoneID string,
	agentIDs []string,
) (nbs.Client, error) {

	cells := s.getCells(zoneID)

	if len(cells) == 0 {
		if s.isCell(zoneID) {
			return s.nbsFactory.GetClient(ctx, zoneID)
		}

		return nil, errors.NewNonCancellableErrorf(
			"incorrect zone ID provided: %q",
			zoneID,
		)
	}

	if len(agentIDs) != 1 {
		return nil, errors.NewNonCancellableErrorf(
			"cell for local disk may be selected, only when one agentID provided, not %v",
			len(agentIDs),
		)
	}

	errGroup := errgroup.Group{}

	selectedClient := make(chan nbs.Client, 1)

	var sendClientOnce sync.Once

	for _, cellID := range cells {
		errGroup.Go(func(cellID string) func() error {
			return func() error {
				client, err := s.nbsFactory.GetClient(ctx, cellID)
				if err != nil {
					return err
				}

				availableStorageInfos, err := client.QueryAvailableStorage(
					ctx,
					agentIDs,
				)
				if err != nil {
					return err
				}

				if !s.isAgentAvailable(availableStorageInfos) {
					return nil
				}

				// Found a valid cell - send its client once.
				sendClientOnce.Do(func() {
					selectedClient <- client
				})

				return nil
			}
		}(cellID))
	}

	err := errGroup.Wait()

	select {
	case client := <-selectedClient:
		return client, nil
	default:
		if err != nil {
			return nil, err
		}

		return nil, errors.NewNonRetriableErrorf(
			"no cells with such agents in zone %v available: %v",
			zoneID,
			agentIDs,
		)
	}
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

func (s *cellSelector) isAgentAvailable(
	availableStorageInfos []nbs.AvailableStorageInfo,
) bool {

	if len(availableStorageInfos) == 0 {
		return false
	}

	// If the only available storage info is empty, agent is
	// unavailable.
	if len(availableStorageInfos) == 1 &&
		availableStorageInfos[0].ChunkSize == 0 &&
		availableStorageInfos[0].ChunkCount == 0 {
		return false
	}

	return true
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
