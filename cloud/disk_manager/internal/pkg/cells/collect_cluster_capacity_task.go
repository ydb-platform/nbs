package cells

import (
	"context"
	"time"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
)

////////////////////////////////////////////////////////////////////////////////

type collectClusterCapacityTask struct {
	config            *cells_config.CellsConfig
	storage           storage.Storage
	nbsFactory        nbs.Factory
	state             *protos.CollectClusterCapacityTaskState
	expirationTimeout time.Duration
}

func (t *collectClusterCapacityTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *collectClusterCapacityTask) Load(_, state []byte) error {
	t.state = &protos.CollectClusterCapacityTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *collectClusterCapacityTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	group := errgroup.Group{}

	cellsToCollect := tasks_common.NewStringSet()
	cellIDToZoneID := make(map[string]string)

	for zoneID, cells := range t.config.Cells {
		for _, cellID := range cells.Cells {
			cellsToCollect.Add(cellID)
			cellIDToZoneID[cellID] = zoneID
		}
	}

	cellsToCollect.Subtract(tasks_common.NewStringSet(t.state.ProcessedCells...))

	deleteOlderThan := time.Now().Add(-t.expirationTimeout)
	completedCells := make(chan string)

	for _, cellID := range cellsToCollect.List() {
		group.Go(func(zoneID string, cellID string) func() error {
			return func() error {
				err := t.updateCellCapacity(ctx, zoneID, cellID, deleteOlderThan)
				if err != nil {
					return err
				}

				completedCells <- cellID

				return nil
			}
		}(cellIDToZoneID[cellID], cellID))
	}

	// Need to close channel only after end of all goroutines.
	go func() {
		_ = group.Wait()
		close(completedCells)
	}()

	for cell := range completedCells {
		t.state.ProcessedCells = append(t.state.ProcessedCells, cell)
		err := execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	// Processing group error after saving task progress.
	err := group.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (t *collectClusterCapacityTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *collectClusterCapacityTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *collectClusterCapacityTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *collectClusterCapacityTask) updateCellCapacity(
	ctx context.Context,
	zoneID string,
	cellID string,
	deleteOlderThan time.Time,
) error {

	logging.Info(
		ctx,
		"Getting cluster capacity for cell %s of zone %s",
		cellID,
		zoneID,
	)

	client, err := t.nbsFactory.GetClient(ctx, cellID)
	if err != nil {
		return err
	}

	capacityInfos, err := client.GetClusterCapacity(ctx)
	if err != nil {
		logging.Error(
			ctx,
			"Failed to get cluster capacity from cell %s: %v",
			cellID,
			err,
		)
		return err
	}

	var capacities []storage.ClusterCapacity
	for _, info := range capacityInfos {
		capacities = append(capacities, storage.ClusterCapacity{
			ZoneID:     zoneID,
			CellID:     cellID,
			Kind:       info.DiskKind,
			FreeBytes:  info.FreeBytes,
			TotalBytes: info.TotalBytes,
		})
	}

	err = t.storage.UpdateClusterCapacities(
		ctx,
		capacities,
		deleteOlderThan,
	)
	if err != nil {
		return err
	}

	logging.Info(
		ctx,
		"Successfully finished getting capacity for cell: %v",
		cellID,
	)
	return nil
}
