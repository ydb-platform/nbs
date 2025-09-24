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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	errorGroup := errgroup.Group{}

	resultClient := make(chan nbs.Client, 1)

	var once sync.Once

	for _, cellID := range cells {
		errorGroup.Go(func(cellID string) func() error {
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

				if len(availableStorageInfos) == 0 {
					return nil
				}

				// If the only available storage info is empty, agent is
				// unavailable.
				if len(availableStorageInfos) == 1 &&
					availableStorageInfos[0].ChunkSize == 0 &&
					availableStorageInfos[0].ChunkCount == 0 {
					return nil
				}

				// Found a valid cell - send it once and cancel other goroutines.
				once.Do(func() {
					select {
					case resultClient <- client:
					default:
					}
				})

				return nil
			}
		}(cellID))
	}

	// Wait for either a result or all goroutines to complete
	done := make(chan error, 1)
	go func() {
		done <- errorGroup.Wait()
	}()

	select {
	case client := <-resultClient:
		return client, nil
	case err := <-done:
		if err != nil {
			return nil, err
		}

		return nil, errors.NewRetriableErrorf(
			"There are no cells with such agents available: %v",
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
