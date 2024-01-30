package pools

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type retireBaseDisksTask struct {
	scheduler tasks.Scheduler
	storage   storage.Storage
	request   *protos.RetireBaseDisksRequest
	state     *protos.RetireBaseDisksTaskState
}

func (t *retireBaseDisksTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *retireBaseDisksTask) Load(request, state []byte) error {
	t.request = &protos.RetireBaseDisksRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.RetireBaseDisksTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *retireBaseDisksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	imageID := t.request.ImageId
	zoneID := t.request.ZoneId
	useBaseDiskAsSrc := t.request.UseBaseDiskAsSrc

	// TODO: can't lock deleted pool, but useBaseDiskAsSrc should work even
	// when pool is deleted.
	shouldLockPool := !useBaseDiskAsSrc
	var lockID string
	if shouldLockPool {
		lockID = execCtx.GetTaskID()
	}

	if len(t.state.BaseDiskIds) == 0 {
		if shouldLockPool {
			locked, err := t.storage.LockPool(ctx, imageID, zoneID, lockID)
			if err != nil {
				return err
			}

			if !locked {
				return errors.NewNonCancellableErrorf(
					"failed to lock pool, imageID %v, zoneID %v",
					imageID,
					zoneID,
				)
			}
		}

		baseDisks, err := t.storage.ListBaseDisks(ctx, imageID, zoneID)
		if err != nil {
			return err
		}

		logging.Info(
			ctx,
			"pools.RetireBaseDisks listed base disks %v",
			baseDisks,
		)

		t.state.BaseDiskIds = make([]string, 0)
		for _, baseDisk := range baseDisks {
			t.state.BaseDiskIds = append(t.state.BaseDiskIds, baseDisk.ID)
		}

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	for _, baseDiskID := range t.state.BaseDiskIds {
		idempotencyKey := fmt.Sprintf("%v_%v", execCtx.GetTaskID(), baseDiskID)

		var srcDisk *types.Disk
		var srcDiskCheckpointID string
		if t.request.UseBaseDiskAsSrc {
			srcDisk = &types.Disk{
				ZoneId: zoneID,
				DiskId: baseDiskID,
			}
			// Note: we use image id as checkpoint id.
			srcDiskCheckpointID = imageID
		}

		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
			"pools.RetireBaseDisk",
			fmt.Sprintf(
				"Retire base disks from pool, imageID=%v, zoneID=%v",
				imageID,
				zoneID,
			),
			&protos.RetireBaseDiskRequest{
				BaseDiskId: baseDiskID,
				SrcDisk:    srcDisk,
			},
			"",
			"",
		)
		if err != nil {
			return err
		}

		_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			return err
		}
	}

	return t.storage.UnlockPool(ctx, imageID, zoneID, lockID)
}

func (t *retireBaseDisksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.UnlockPool(
		ctx,
		t.request.ImageId,
		t.request.ZoneId,
		execCtx.GetTaskID(), // lockID
	)
}

func (t *retireBaseDisksTask) GetMetadata(
	ctx context.Context,
	taskID string,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *retireBaseDisksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
