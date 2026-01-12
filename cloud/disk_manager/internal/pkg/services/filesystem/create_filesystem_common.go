package filesystem

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type CreateFilesystemTaskState interface {
	GetSelectedCellId() string
}

func SelectCellForFilesystem(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	state CreateFilesystemTaskState,
	request *protos.CreateFilesystemRequest,
	cellSelector cells.CellSelector,
	nfsFactory nfs.Factory,
) (nfs.Client, error) {

	var client nfs.Client
	// Idempotently retrieve cell, where filesystem should be created.
	if len(state.GetSelectedCellId()) > 0{
		return nfsFactory.NewClient(ctx, state.GetSelectedCellId())
	}

	zoneID := request.Filesystem.ZoneId
	if cellSelector == nil {
		return nfsFactory.NewClient(ctx, zoneID)
	}
	client, err := cellSelector.SelectCellForFilesystem(
		ctx,
		zoneID,
		request.FolderId,
	)
	if err != nil {
		return nil, err
	}

	switch s := state.(type) {
	case *protos.CreateFilesystemTaskState:
		s.SelectedCellId = client.ZoneID()
	// We will implement filesystem-from-snapshot creation later.
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

	return client, nil
}
