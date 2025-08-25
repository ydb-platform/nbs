package disks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type deleteDiskTask struct {
	storage     resources.Storage
	scheduler   tasks.Scheduler
	poolService pools.Service
	nbsFactory  nbs.Factory
	request     *protos.DeleteDiskRequest
	state       *protos.DeleteDiskTaskState
}

func (t *deleteDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteDiskTask) Load(request, state []byte) error {
	t.request = &protos.DeleteDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteDiskTask) deleteDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()
	diskID := t.request.Disk.DiskId
	sync := t.request.Sync

	diskMeta, err := t.storage.DeleteDisk(
		ctx,
		diskID,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if diskMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			diskID,
		)
	}

	zoneID := diskMeta.ZoneID
	if len(zoneID) == 0 {
		zoneID = t.request.Disk.ZoneId
	}
	if len(zoneID) == 0 {
		// If diskMeta has no zoneID, the disk was not in the database before
		// calling storage.DeleteDisk, so it has already been marked as deleted.
		// Need to call neither storage.DiskDeleted nor client.Delete.
		return nil
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			selfTaskID+"_delete_disk_from_incremental",
		),
		"dataplane.DeleteDiskFromIncremental",
		"",
		&dataplane_protos.DeleteDiskFromIncrementalRequest{
			Disk: &types.Disk{
				ZoneId: zoneID,
				DiskId: diskID,
			},
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	client, err := t.nbsFactory.GetClient(ctx, zoneID)
	if err != nil {
		return err
	}

	if sync {
		err = client.DeleteSync(ctx, diskID)
	} else {
		err = client.Delete(ctx, diskID)
	}
	if err != nil {
		return err
	}

	// Only overlay disks (created from image) should be released.
	if len(diskMeta.SrcImageID) != 0 {
		taskID, err = t.poolService.ReleaseBaseDisk(
			headers.SetIncomingIdempotencyKey(
				ctx,
				selfTaskID+"_release_base_disk",
			),
			&pools_protos.ReleaseBaseDiskRequest{
				OverlayDisk: &types.Disk{
					ZoneId: zoneID,
					DiskId: diskID,
				},
			},
		)
		if err != nil {
			return err
		}

		_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
		if err != nil {
			return err
		}
	}

	return t.storage.DiskDeleted(ctx, diskID, time.Now())
}

func (t *deleteDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteDisk(ctx, execCtx)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}

	return nil
}

func (t *deleteDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.deleteDisk(ctx, execCtx)
}

func (t *deleteDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.DeleteDiskMetadata{
		DiskId: &disk_manager.DiskId{
			ZoneId: t.request.Disk.ZoneId,
			DiskId: t.request.Disk.DiskId,
		},
	}, nil
}

func (t *deleteDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
