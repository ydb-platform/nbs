package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type acquireBaseDiskTask struct {
	scheduler tasks.Scheduler
	storage   storage.Storage
	request   *protos.AcquireBaseDiskRequest
	state     *protos.AcquireBaseDiskTaskState
}

func (t *acquireBaseDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *acquireBaseDiskTask) Load(request, state []byte) error {
	t.request = &protos.AcquireBaseDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.AcquireBaseDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *acquireBaseDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	logging.Info(
		ctx,
		"acquire base disk imageID %v overlayDisk %v",
		t.request.SrcImageId,
		t.request.OverlayDisk,
	)

	baseDisk, err := t.storage.AcquireBaseDiskSlot(
		ctx,
		t.request.SrcImageId,
		storage.Slot{
			OverlayDisk:     t.request.OverlayDisk,
			OverlayDiskKind: t.request.OverlayDiskKind,
			OverlayDiskSize: t.request.OverlayDiskSize,
		},
	)
	if err != nil {
		return err
	}

	logging.Info(
		ctx,
		"overlayDisk %v acquired slot on baseDisk %v",
		t.request.OverlayDisk,
		baseDisk,
	)

	t.state.BaseDisk = &types.Disk{
		ZoneId: baseDisk.ZoneID,
		DiskId: baseDisk.ID,
	}
	t.state.BaseDiskCheckpointId = baseDisk.CheckpointID

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	if baseDisk.Ready {
		logging.Info(
			ctx,
			"imageID %v overlayDisk %v no need to create new disk",
			t.request.SrcImageId,
			t.request.OverlayDisk,
		)

		return nil
	}

	logging.Info(
		ctx,
		"imageID %v overlayDisk %v waiting for new disk created by %v",
		t.request.SrcImageId,
		t.request.OverlayDisk,
		baseDisk.CreateTaskID,
	)
	_, err = t.scheduler.WaitTask(ctx, execCtx, baseDisk.CreateTaskID)
	return err
}

func (t *acquireBaseDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	overlayDisk := t.request.OverlayDisk

	logging.Info(
		ctx,
		"cancel aquire base disk imageID %v overlayDisk %v",
		t.request.SrcImageId,
		overlayDisk,
	)

	_, err := t.storage.ReleaseBaseDiskSlot(
		ctx,
		overlayDisk,
	)
	return err
}

func (t *acquireBaseDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *acquireBaseDiskTask) GetResponse() proto.Message {
	return &protos.AcquireBaseDiskResponse{
		BaseDiskId:           t.state.BaseDisk.DiskId,
		BaseDiskCheckpointId: t.state.BaseDiskCheckpointId,
	}
}
