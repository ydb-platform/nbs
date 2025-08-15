package pools

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createBaseDiskTask struct {
	cloudID  string
	folderID string

	scheduler       tasks.Scheduler
	storage         storage.Storage
	nbsFactory      nbs.Factory
	resourceStorage resources.Storage
	request         *protos.CreateBaseDiskRequest
	state           *protos.CreateBaseDiskTaskState
}

func (t *createBaseDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createBaseDiskTask) Load(request, state []byte) error {
	t.request = &protos.CreateBaseDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateBaseDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createBaseDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return tasks.RunSteps(t.state.Step, []tasks.StepFunc{
		func(stop *bool) error {
			baseDisk := t.request.BaseDisk

			client, err := t.nbsFactory.GetClient(ctx, baseDisk.ZoneId)
			if err != nil {
				return err
			}

			if t.request.SrcDisk != nil {
				srcParams, err := client.Describe(ctx, t.request.SrcDisk.DiskId)
				if err != nil {
					return err
				}

				if common.IsLocalDiskKind(srcParams.Kind) {
					return errors.NewNonCancellableErrorf(
						"cannot create base disk from local disk %v",
						t.request.SrcDisk.DiskId,
					)
				}
			}

			baseDiskSize := t.request.BaseDiskSize

			var baseDiskBlockCount uint64
			if baseDiskSize == 0 {
				baseDiskBlockCount = defaultBaseDiskBlockCount
			} else {
				if baseDiskSize%baseDiskBlockSize != 0 {
					return common.NewInvalidArgumentError(
						"baseDiskSize should be divisible by baseDiskBlockSize, baseDiskSize %v, baseDiskBlockSize %v",
						baseDiskSize,
						baseDiskBlockSize,
					)
				}

				baseDiskBlockCount = baseDiskSize / baseDiskBlockSize
			}

			return client.Create(ctx, nbs.CreateDiskParams{
				ID:              baseDisk.DiskId,
				BlocksCount:     baseDiskBlockCount,
				BlockSize:       baseDiskBlockSize,
				Kind:            types.DiskKind_DISK_KIND_SSD,
				CloudID:         t.cloudID,
				FolderID:        t.folderID,
				PartitionsCount: 1, // Should be deleted after NBS-1896.
				IsSystem:        true,
			})
		},
		func(stop *bool) (err error) {
			var taskID string

			zoneID := t.request.BaseDisk.ZoneId

			if t.request.SrcDisk != nil {
				taskID, err = t.scheduler.ScheduleZonalTask(
					headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()),
					"dataplane.TransferFromDiskToDisk",
					"",
					zoneID,
					&dataplane_protos.TransferFromDiskToDiskRequest{
						SrcDisk:                 t.request.SrcDisk,
						SrcDiskBaseCheckpointId: "",
						SrcDiskCheckpointId:     t.request.SrcDiskCheckpointId,
						DstDisk:                 t.request.BaseDisk,
					},
				)
				if err != nil {
					return err
				}
			} else {
				image, err := t.resourceStorage.GetImageMeta(
					ctx,
					t.request.SrcImageId,
				)
				if err != nil {
					return err
				}

				// Old images without metadata we consider as not dataplane.
				if image != nil && image.UseDataplaneTasks {
					taskID, err = t.scheduler.ScheduleZonalTask(
						headers.SetIncomingIdempotencyKey(
							ctx,
							execCtx.GetTaskID(),
						),
						"dataplane.TransferFromSnapshotToDisk",
						"",
						zoneID,
						&dataplane_protos.TransferFromSnapshotToDiskRequest{
							SrcSnapshotId: t.request.SrcImageId,
							DstDisk:       t.request.BaseDisk,
						},
					)
				} else {
					taskID, err = t.scheduler.ScheduleZonalTask(
						headers.SetIncomingIdempotencyKey(
							ctx,
							execCtx.GetTaskID(),
						),
						"dataplane.TransferFromLegacySnapshotToDisk",
						"",
						zoneID,
						&dataplane_protos.TransferFromSnapshotToDiskRequest{
							SrcSnapshotId: t.request.SrcImageId,
							DstDisk:       t.request.BaseDisk,
						},
					)
				}
				if err != nil {
					return err
				}
			}

			_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
			return err
		},
		func(stop *bool) error {
			checkpointID := t.request.BaseDiskCheckpointId
			baseDisk := t.request.BaseDisk

			client, err := t.nbsFactory.GetClient(ctx, baseDisk.ZoneId)
			if err != nil {
				return err
			}

			return client.CreateCheckpoint(
				ctx,
				nbs.CheckpointParams{
					DiskID:       baseDisk.DiskId,
					CheckpointID: checkpointID,
				},
			)
		},
		func(stop *bool) error {
			return t.storage.BaseDiskCreated(ctx, storage.BaseDisk{
				ID:      t.request.BaseDisk.DiskId,
				ImageID: t.request.SrcImageId,
				ZoneID:  t.request.BaseDisk.ZoneId,
			})
		}},
		func(step uint32) error {
			t.state.Step = step
			return execCtx.SaveState(ctx)
		})
}

func (t *createBaseDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.BaseDiskCreationFailed(ctx, storage.BaseDisk{
		ID:      t.request.BaseDisk.DiskId,
		ImageID: t.request.SrcImageId,
		ZoneID:  t.request.BaseDisk.ZoneId,
	})
}

func (t *createBaseDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *createBaseDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
