package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type scheduleBaseDisksTask struct {
	scheduler tasks.Scheduler
	storage   storage.Storage
}

func (t *scheduleBaseDisksTask) Save() ([]byte, error) {
	return nil, nil
}

func (t *scheduleBaseDisksTask) Load(_, _ []byte) error {
	return nil
}

func (t *scheduleBaseDisksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	baseDisks, err := t.storage.TakeBaseDisksToSchedule(ctx)
	if err != nil {
		return err
	}

	for i := 0; i < len(baseDisks); i++ {
		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				"_base_disk_id_"+baseDisks[i].ID,
			),
			"pools.CreateBaseDisk",
			"",
			&protos.CreateBaseDiskRequest{
				SrcImageId:          baseDisks[i].ImageID,
				SrcDisk:             baseDisks[i].SrcDisk,
				SrcDiskCheckpointId: baseDisks[i].SrcDiskCheckpointID,
				BaseDisk: &types.Disk{
					ZoneId: baseDisks[i].ZoneID,
					DiskId: baseDisks[i].ID,
				},
				BaseDiskCheckpointId:                baseDisks[i].CheckpointID,
				BaseDiskSize:                        baseDisks[i].Size,
				UseDataplaneTasksForLegacySnapshots: true, // TODO: remove it.
			},
		)
		if err != nil {
			return err
		}

		baseDisks[i].CreateTaskID = taskID
	}

	return t.storage.BaseDisksScheduled(ctx, baseDisks)
}

func (t *scheduleBaseDisksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBaseDisksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scheduleBaseDisksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
