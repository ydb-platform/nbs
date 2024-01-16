package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type rebaseOverlayDiskTask struct {
	storage    storage.Storage
	nbsFactory nbs.Factory
	request    *protos.RebaseOverlayDiskRequest
	state      *protos.RebaseOverlayDiskTaskState
}

func (t *rebaseOverlayDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *rebaseOverlayDiskTask) Load(request, state []byte) error {
	t.request = &protos.RebaseOverlayDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.RebaseOverlayDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *rebaseOverlayDiskTask) rebaseOverlayDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	logging.Info(
		ctx,
		"rebaseOverlayDiskTask: taskID %v, request %v",
		execCtx.GetTaskID(),
		t.request,
	)

	client, err := t.nbsFactory.GetClient(ctx, t.request.OverlayDisk.ZoneId)
	if err != nil {
		return err
	}

	err = t.storage.OverlayDiskRebasing(ctx, storage.RebaseInfo{
		OverlayDisk:      t.request.OverlayDisk,
		BaseDiskID:       t.request.BaseDiskId,
		TargetBaseDiskID: t.request.TargetBaseDiskId,
		SlotGeneration:   t.request.SlotGeneration,
	})
	if err != nil {
		if nbs.IsDiskNotFoundError(err) {
			// Disk might not be created yet.
			// Restart task to avoid this race (NBS-3761).
			return errors.NewInterruptExecutionError()
		}

		return err
	}

	err = client.Rebase(
		ctx,
		func() error { return execCtx.SaveState(ctx) },
		t.request.OverlayDisk.DiskId,
		t.request.BaseDiskId,
		t.request.TargetBaseDiskId,
	)
	if err != nil {
		return err
	}

	return t.storage.OverlayDiskRebased(ctx, storage.RebaseInfo{
		OverlayDisk:      t.request.OverlayDisk,
		BaseDiskID:       t.request.BaseDiskId,
		TargetBaseDiskID: t.request.TargetBaseDiskId,
		SlotGeneration:   t.request.SlotGeneration,
	})
}

func (t *rebaseOverlayDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.rebaseOverlayDisk(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *rebaseOverlayDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *rebaseOverlayDiskTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *rebaseOverlayDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
