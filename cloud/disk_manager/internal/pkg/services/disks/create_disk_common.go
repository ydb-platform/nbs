package disks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type CreateDiskTaskState interface {
	GetSelectedCellId() string
}

func SelectZone(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nbsFactory nbs.Factory,
	cellSelector cells.CellSelector,
	params *protos.CreateDiskParams,
	state CreateDiskTaskState,
) (nbs.Client, error) {

	var client nbs.Client
	var err error

	if len(state.GetSelectedCellId()) > 0 {
		client, err = nbsFactory.GetClient(ctx, state.GetSelectedCellId())
		if err != nil {
			return nil, err
		}
	} else {
		client, err = cellSelector.SelectCell(
			ctx,
			params.Disk.ZoneId,
			params.FolderId,
		)
		if err != nil {
			return nil, err
		}

		switch s := state.(type) {
		case *protos.CreateEmptyDiskTaskState:
			s.SelectedCellId = client.ZoneID()
		case *protos.CreateDiskFromImageTaskState:
			s.SelectedCellId = client.ZoneID()
		case *protos.CreateDiskFromSnapshotTaskState:
			s.SelectedCellId = client.ZoneID()
		case *protos.CreateOverlayDiskTaskState:
			s.SelectedCellId = client.ZoneID()
		default:
			return nil, errors.NewNonRetriableErrorf(
				"unsupported proto type: %T",
				state,
			)
		}

		err = execCtx.SaveState(ctx)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}
