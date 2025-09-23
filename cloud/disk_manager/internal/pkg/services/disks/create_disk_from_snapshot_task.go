package disks

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createDiskFromSnapshotTask struct {
	storage      resources.Storage
	scheduler    tasks.Scheduler
	nbsFactory   nbs.Factory
	request      *protos.CreateDiskFromSnapshotRequest
	state        *protos.CreateDiskFromSnapshotTaskState
	cellSelector cells.CellSelector
}

func (t *createDiskFromSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createDiskFromSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.CreateDiskFromSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateDiskFromSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createDiskFromSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	params := t.request.Params

	if common.IsLocalDiskKind(params.Kind) {
		return errors.NewNonCancellableErrorf(
			"local disk creation from snapshot is forbidden",
		)
	}

	client, err := SelectCell(
		ctx,
		execCtx,
		t.state,
		params,
		t.cellSelector,
		t.nbsFactory,
	)
	if err != nil {
		return err
	}

	disk := &types.Disk{
		DiskId: params.Disk.DiskId,
		ZoneId: t.state.SelectedCellId,
	}

	selfTaskID := execCtx.GetTaskID()

	diskMeta, err := t.storage.CreateDisk(ctx, resources.DiskMeta{
		ID:            disk.DiskId,
		ZoneID:        disk.ZoneId,
		SrcSnapshotID: t.request.SrcSnapshotId,
		BlocksCount:   params.BlocksCount,
		BlockSize:     params.BlockSize,
		Kind:          common.DiskKindToString(params.Kind),
		CloudID:       params.CloudId,
		FolderID:      params.FolderId,

		CreateRequest: t.request,
		CreateTaskID:  selfTaskID,
		CreatingAt:    time.Now(),
		CreatedBy:     "", // TODO: Extract CreatedBy from execCtx
	})
	if err != nil {
		return err
	}

	if diskMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			params.Disk.DiskId,
		)
	}

	snapshotMeta, err := t.storage.GetSnapshotMeta(ctx, t.request.SrcSnapshotId)
	if err != nil {
		return err
	}

	diskEncryption := types.EncryptionMode_NO_ENCRYPTION
	snapshotEncryption := types.EncryptionMode_NO_ENCRYPTION

	if params.EncryptionDesc != nil {
		diskEncryption = params.EncryptionDesc.Mode
	}

	if snapshotMeta != nil && snapshotMeta.Encryption != nil {
		snapshotEncryption = snapshotMeta.Encryption.Mode
	}

	if snapshotEncryption != types.EncryptionMode_NO_ENCRYPTION &&
		diskEncryption != snapshotEncryption {

		message := fmt.Sprintf(
			"encryption mode should be the same for disk (%v) and encrypted snapshot (%v)",
			diskEncryption,
			snapshotEncryption,
		)
		if snapshotMeta == nil {
			return errors.NewSilentNonRetriableErrorf(message)
		}
		return errors.NewNonRetriableErrorf(message)
	}

	encryption := params.EncryptionDesc
	if snapshotEncryption != types.EncryptionMode_NO_ENCRYPTION {
		encryption = snapshotMeta.Encryption
	}

	err = client.Create(ctx, nbs.CreateDiskParams{
		ID:                      disk.DiskId,
		BlocksCount:             params.BlocksCount,
		BlockSize:               params.BlockSize,
		Kind:                    params.Kind,
		CloudID:                 params.CloudId,
		FolderID:                params.FolderId,
		TabletVersion:           params.TabletVersion,
		PlacementGroupID:        params.PlacementGroupId,
		PlacementPartitionIndex: params.PlacementPartitionIndex,
		StoragePoolName:         params.StoragePoolName,
		AgentIds:                params.AgentIds,
		EncryptionDesc:          encryption,
	})
	if err != nil {
		return err
	}

	var taskID string

	// Old snapshots without metadata we consider as not dataplane
	if snapshotMeta != nil && snapshotMeta.UseDataplaneTasks {
		taskID, err = t.scheduler.ScheduleZonalTask(
			headers.SetIncomingIdempotencyKey(ctx, selfTaskID),
			"dataplane.TransferFromSnapshotToDisk",
			"",
			disk.ZoneId,
			&dataplane_protos.TransferFromSnapshotToDiskRequest{
				SrcSnapshotId: t.request.SrcSnapshotId,
				DstDisk:       disk,
				DstEncryption: encryption,
			},
		)

		t.state.DataplaneTaskId = taskID
	} else {
		taskID, err = t.scheduler.ScheduleZonalTask(
			headers.SetIncomingIdempotencyKey(ctx, selfTaskID),
			"dataplane.TransferFromLegacySnapshotToDisk",
			"",
			disk.ZoneId,
			&dataplane_protos.TransferFromSnapshotToDiskRequest{
				SrcSnapshotId: t.request.SrcSnapshotId,
				DstDisk:       disk,
				DstEncryption: encryption,
			},
		)

		t.state.DataplaneTaskId = taskID
	}
	if err != nil {
		return err
	}

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	diskMeta.CreatedAt = time.Now()
	return t.storage.DiskCreated(ctx, *diskMeta)
}

func (t *createDiskFromSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	params := t.request.Params

	selfTaskID := execCtx.GetTaskID()

	// Idempotently retrieve zone, where disk should be located.
	diskMeta, err := t.storage.DeleteDisk(
		ctx,
		params.Disk.DiskId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if diskMeta == nil {
		return nil
	}

	client, err := t.nbsFactory.GetClient(ctx, diskMeta.ZoneID)
	if err != nil {
		return err
	}

	err = client.Delete(ctx, diskMeta.ID)
	if err != nil {
		return err
	}

	return t.storage.DiskDeleted(ctx, diskMeta.ID, time.Now())
}

func (t *createDiskFromSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.CreateDiskMetadata{}

	if len(t.state.DataplaneTaskId) != 0 {
		message, err := t.scheduler.GetTaskMetadata(
			ctx,
			t.state.DataplaneTaskId,
		)
		if err != nil {
			return nil, err
		}

		transferMetadata, ok := message.(*dataplane_protos.TransferFromSnapshotToDiskMetadata)
		if ok {
			metadata.Progress = transferMetadata.Progress
		}
	}

	return metadata, nil
}

func (t *createDiskFromSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
