package cells

import (
	"context"
	"slices"
	"time"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
)

////////////////////////////////////////////////////////////////////////////////

type collectClusterCapacityTask struct {
	config     *cells_config.CellsConfig
	storage    storage.Storage
	nbsFactory nbs.Factory
	state      *protos.CollectClusterCapacityState
}

func (t *collectClusterCapacityTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *collectClusterCapacityTask) Load(_, state []byte) error {
	t.state = &protos.CollectClusterCapacityState{}
	return proto.Unmarshal(state, t.state)
}

func (t *collectClusterCapacityTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	group, ctx := errgroup.WithContext(ctx)

	completedCells := make(chan string)

	for zoneID, cells := range t.config.Cells {
		logging.Info(ctx, "Getting cluster capacity for zone %s", zoneID)

		for _, cellID := range cells.Cells {
			group.Go(func(zoneID string, cellID string) func() error {
				return func() error {
					if slices.Contains(t.state.ProcessedCells, cellID) {
						return nil
					}

					logging.Info(ctx, "Getting cluster capacity for cell %s", cellID)

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

					expirationTimeout, err := time.ParseDuration(
						t.config.GetClusterCapacityExpirationTimeout(),
					)
					if err != nil {
						return err
					}

					deleteBefore := time.Now().Add(-expirationTimeout)
					err = t.storage.UpdateClusterCapacities(
						ctx,
						capacities,
						deleteBefore,
					)
					if err != nil {
						return err
					}

					logging.Info(
						ctx,
						"Successfully finished getting capacity for cell: %v",
						cellID,
					)

					select {
					case completedCells <- cellID:
					case <-ctx.Done():
						return ctx.Err()
					}
					return nil
				}
			}(zoneID, cellID))
		}
	}

	go func() {
		_ = group.Wait()
		close(completedCells)
	}()

	for cell := range completedCells {
		t.state.ProcessedCells = append(t.state.ProcessedCells, cell)
	}

	err := execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	err = group.Wait()
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
