package cells

import (
	"context"
	"time"

	cells_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
)

////////////////////////////////////////////////////////////////////////////////

type getClusterCapacityTask struct {
	storage    storage.Storage
	nbsFactory nbs.Factory
	config     *cells_config.CellsConfig
	state      *protos.GetClusterCapacityState
}

func (t *getClusterCapacityTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *getClusterCapacityTask) Load(_, state []byte) error {
	t.state = &protos.GetClusterCapacityState{}
	return proto.Unmarshal(state, t.state)
}

func (t *getClusterCapacityTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	for zoneID, cells := range t.config.Cells {
		logging.Debug(ctx, "Getting cluster capacity for zone %s", zoneID)

		for _, cellID := range cells {
			client, err := t.nbsFactory.GetClient(ctx, cellID)
			if err != nil {
				return err
			}

			capacityInfos, err := client.GetClusterCapacity(ctx)
			if err != nil {
				logging.Warn(ctx, "Failed to get cluster capacity from cell %s: %v", cellID, err)
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

			err = t.storage.UpdateClusterCapacities(ctx, capacities, time.Now())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (t *getClusterCapacityTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *getClusterCapacityTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *getClusterCapacityTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
