package disks

import (
	"context"
	"slices"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type CreateDiskTaskState interface {
	GetSelectedCellId() string
}

func SelectCellForDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	state CreateDiskTaskState,
	params *protos.CreateDiskParams,
	cellSelector cells.CellSelector,
	nbsFactory nbs.Factory,
	storage resources.Storage,
) (nbs.Client, error) {

	var client nbs.Client
	var err error

	// Idempotently retrieve cell, where disk should be created.
	if len(state.GetSelectedCellId()) > 0 {
		client, err = nbsFactory.GetClient(ctx, state.GetSelectedCellId())
		if err != nil {
			return nil, err
		}
	} else {
		if len(params.PlacementGroupId) > 0 {
			// Pin disk to the same cell where its placement group was
			// created so that DR placement constraints work correctly.
			var pgMeta *resources.PlacementGroupMeta
			pgMeta, err = storage.GetPlacementGroupMeta(
				ctx,
				params.PlacementGroupId,
			)
			if err != nil {
				return nil, err
			}
			if pgMeta == nil {
				return nil, errors.NewNonRetriableErrorf(
					"placement group %v not found",
					params.PlacementGroupId,
				)
			}

			if params.RequireExactCellIdMatch {
				if pgMeta.ZoneID != params.Disk.ZoneId {
					return nil, errors.NewNonRetriableErrorf(
						"placement group %v is in zone %v, not in requested zone %v",
						params.PlacementGroupId,
						pgMeta.ZoneID,
						params.Disk.ZoneId,
					)
				}
			} else {
				var zoneCells []string
				zoneCells, err = cellSelector.ResolveCells(params.Disk.ZoneId)
				if err != nil {
					return nil, err
				}
				if !slices.Contains(zoneCells, pgMeta.ZoneID) {
					return nil, errors.NewNonRetriableErrorf(
						"placement group %v is in zone %v, not in requested zone %v",
						params.PlacementGroupId,
						pgMeta.ZoneID,
						params.Disk.ZoneId,
					)
				}
			}

			if common.IsLocalDiskKind(params.Kind) {
				var localClient nbs.Client
				localClient, err = cellSelector.SelectCellForLocalDisk(
					ctx,
					params.Disk.ZoneId,
					params.AgentIds,
				)
				if err != nil {
					return nil, err
				}
				if localClient.ZoneID() != pgMeta.ZoneID {
					return nil, errors.NewNonRetriableErrorf(
						"agent for local disk is in cell %v, "+
							"but placement group %v is in cell %v",
						localClient.ZoneID(),
						params.PlacementGroupId,
						pgMeta.ZoneID,
					)
				}
				client = localClient
			} else {
				client, err = nbsFactory.GetClient(ctx, pgMeta.ZoneID)
			}
		} else if common.IsLocalDiskKind(params.Kind) {
			// There is an agent, where local disk should be created. Can't use
			// default cell selection mechanism.
			client, err = cellSelector.SelectCellForLocalDisk(
				ctx,
				params.Disk.ZoneId,
				params.AgentIds,
			)
		} else {
			client, err = cellSelector.SelectCellForDisk(
				ctx,
				params.Disk.ZoneId,
				params.FolderId,
				params.Kind,
				params.RequireExactCellIdMatch,
			)
		}
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
