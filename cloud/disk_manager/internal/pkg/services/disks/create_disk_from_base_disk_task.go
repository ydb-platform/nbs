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
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

// Creates a disk from an image using a base disk from the pool as a data
// source instead of transferring the data from the snapshot storage (S3/YDB).
//
// The flow mirrors createOverlayDiskTask (acquire a base disk slot), but the
// resulting disk is not layered: the data is copied into it by
// dataplane.TransferFromDiskToDisk. To avoid reads via the base disk tablet,
// a proxy overlay disk (the same mechanism that createSnapshotFromDiskTask
// uses) is created on top of the base disk checkpoint and mounted locally by
// the dataplane, so the data is read directly from the storage nodes.
//
// The proxy overlay disk is the actual overlay disk of the base disk, so the
// base disk slot is acquired for it (and not for the resulting disk), and the
// transfer refers to the proxy overlay disk only (GetChangedBlocks on an
// overlay disk includes the blocks of its current base disk). So if the base
// disk is retired during the transfer, pools.RebaseOverlayDisk rebases the
// proxy overlay disk onto another base disk transparently for the transfer,
// as it does for regular overlay disks.
type createDiskFromBaseDiskTask struct {
	storage                  resources.Storage
	scheduler                tasks.Scheduler
	poolService              pools.Service
	nbsFactory               nbs.Factory
	proxyOverlayDiskIDPrefix string
	request                  *protos.CreateDiskFromBaseDiskRequest
	state                    *protos.CreateDiskFromBaseDiskTaskState
	cellSelector             cells.CellSelector
}

func (t *createDiskFromBaseDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createDiskFromBaseDiskTask) Load(request, state []byte) error {
	t.request = &protos.CreateDiskFromBaseDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateDiskFromBaseDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createDiskFromBaseDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	params := t.request.Params

	if common.IsLocalDiskKind(params.Kind) {
		return errors.NewNonCancellableErrorf(
			"local disk creation from image is forbidden",
		)
	}

	client, err := SelectCellForDisk(
		ctx,
		execCtx,
		t.state,
		params,
		t.cellSelector,
		t.nbsFactory,
		t.storage,
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
		ID:          disk.DiskId,
		ZoneID:      disk.ZoneId,
		SrcImageID:  t.request.SrcImageId,
		BlocksCount: params.BlocksCount,
		BlockSize:   params.BlockSize,
		Kind:        common.DiskKindToString(params.Kind),
		CloudID:     params.CloudId,
		FolderID:    params.FolderId,

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

	if !t.state.DataTransferred {
		err = t.createAndFillDisk(ctx, execCtx, client, disk)
		if err != nil {
			return err
		}

		t.state.DataTransferred = true

		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	}

	// Proxy overlay disk is a one-shot entity: it is not needed anymore once
	// the data has been transferred. Done outside of createAndFillDisk because
	// the base disk may be deleted as soon as its last slot is released.
	err = t.deleteProxyOverlayDiskAndReleaseBaseDisk(ctx, execCtx)
	if err != nil {
		return err
	}

	diskMeta.CreatedAt = time.Now()
	return t.storage.DiskCreated(ctx, *diskMeta)
}

func (t *createDiskFromBaseDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Should be done regardless of the disk state: the disk may have already
	// been deleted by a concurrent disks.DeleteDisk task, which knows nothing
	// about the proxy overlay disk and the base disk slot acquired for it.
	err := t.deleteProxyOverlayDiskAndReleaseBaseDisk(ctx, execCtx)
	if err != nil {
		return err
	}

	params := t.request.Params

	diskMeta, err := t.storage.DeleteDisk(
		ctx,
		params.Disk.DiskId,
		execCtx.GetTaskID(),
		time.Now(),
	)
	if err != nil {
		return err
	}

	if len(t.state.SelectedCellId) != 0 {
		client, err := t.nbsFactory.GetClient(ctx, t.state.SelectedCellId)
		if err != nil {
			return err
		}

		// Delete the disk from NBS even if it has already been deleted by a
		// concurrent disks.DeleteDisk task (diskMeta == nil): creation from
		// base disk takes a while and this task may have created the disk in
		// NBS after that task has finished. A foreign disk with the same id
		// can't be deleted here: creation with an already used id fails with
		// non-cancellable error, so Cancel is not even invoked.
		err = client.Delete(ctx, params.Disk.DiskId)
		if err != nil {
			return err
		}
	}

	if diskMeta == nil {
		return nil
	}

	return t.storage.DiskDeleted(ctx, diskMeta.ID, time.Now())
}

func (t *createDiskFromBaseDiskTask) GetMetadata(
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

		transferMetadata, ok := message.(*dataplane_protos.TransferFromDiskToDiskMetadata)
		if ok {
			metadata.Progress = transferMetadata.Progress
		}
	}

	return metadata, nil
}

func (t *createDiskFromBaseDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

// Pools storage computes the units allotted on a base disk for ssd and hdd
// overlay disks only (any other kind is charged the minimum of 1 unit), so map
// Disk Registry based kinds to their media kind to account the slot according
// to the disk size.
func baseDiskSlotKind(kind types.DiskKind) types.DiskKind {
	switch kind {
	case types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		types.DiskKind_DISK_KIND_SSD_MIRROR2,
		types.DiskKind_DISK_KIND_SSD_MIRROR3:
		return types.DiskKind_DISK_KIND_SSD
	case types.DiskKind_DISK_KIND_HDD_NONREPLICATED:
		return types.DiskKind_DISK_KIND_HDD
	default:
		return kind
	}
}

// Acquires a base disk, creates a proxy overlay disk on top of it, creates
// the disk and transfers the data from the proxy overlay disk into the disk.
func (t *createDiskFromBaseDiskTask) createAndFillDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	client nbs.Client,
	disk *types.Disk,
) error {

	params := t.request.Params
	selfTaskID := execCtx.GetTaskID()
	proxyOverlayDisk := t.getProxyOverlayDisk(selfTaskID)

	err := t.checkEncryption(ctx)
	if err != nil {
		return err
	}

	// Acquire base disk slot for the proxy overlay disk (which is the actual
	// overlay disk of the base disk).
	taskID, err := t.poolService.AcquireBaseDisk(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_acquire"),
		&pools_protos.AcquireBaseDiskRequest{
			SrcImageId:      t.request.SrcImageId,
			OverlayDisk:     proxyOverlayDisk,
			OverlayDiskKind: baseDiskSlotKind(params.Kind),
			OverlayDiskSize: uint64(params.BlockSize) * params.BlocksCount,
		},
	)
	if err != nil {
		return err
	}

	acquireResponse, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	baseDiskID, baseDiskCheckpointID, err := parseAcquireBaseDiskResponse(
		acquireResponse,
	)
	if err != nil {
		return err
	}

	err = t.createProxyOverlayDisk(
		ctx,
		execCtx,
		client,
		proxyOverlayDisk,
		baseDiskID,
		baseDiskCheckpointID,
	)
	if err != nil {
		return err
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
		EncryptionDesc:          params.EncryptionDesc,
	})
	if err != nil {
		return err
	}

	taskID, err = t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID),
		"dataplane.TransferFromDiskToDisk",
		// Image id and disk kind are not present in the request params.
		fmt.Sprintf(
			"Fill %v disk %v from image %v using base disk %v",
			common.DiskKindToString(params.Kind),
			disk.DiskId,
			t.request.SrcImageId,
			baseDiskID,
		),
		disk.ZoneId,
		&dataplane_protos.TransferFromDiskToDiskRequest{
			// Don't refer to the base disk here: it may be retired (and the
			// proxy overlay disk rebased onto another one) during the
			// transfer. Proxy overlay disk has a checkpoint with the same id
			// as the base disk one.
			SrcDisk:             proxyOverlayDisk,
			SrcDiskCheckpointId: baseDiskCheckpointID,
			MountSrcDiskLocally: true,
			DstDisk:             disk,
		},
	)
	if err != nil {
		return err
	}

	t.state.DataplaneTaskId = taskID

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	return err
}

func (t *createDiskFromBaseDiskTask) createProxyOverlayDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	client nbs.Client,
	proxyOverlayDisk *types.Disk,
	baseDiskID string,
	baseDiskCheckpointID string,
) error {

	if t.state.ProxyOverlayDiskCreated {
		return nil
	}

	created, err := client.CreateProxyOverlayDisk(
		ctx,
		proxyOverlayDisk.DiskId,
		baseDiskID,
		baseDiskCheckpointID,
	)
	if err != nil {
		return err
	}

	if !created {
		// Should not happen: base disks from pools are ydb-based, not
		// encrypted and not layered. Don't fall back to reading from the base
		// disk directly, since the whole point is to avoid reads via the base
		// disk tablet.
		return errors.NewNonRetriableErrorf(
			"failed to create proxy overlay disk %v: base disk %v can't be used as base disk",
			proxyOverlayDisk.DiskId,
			baseDiskID,
		)
	}

	logging.Info(
		ctx,
		"created proxy overlay disk %v for base disk %v (checkpoint %v) to fill disk %v",
		proxyOverlayDisk.DiskId,
		baseDiskID,
		baseDiskCheckpointID,
		t.request.Params.Disk.DiskId,
	)

	t.state.ProxyOverlayDiskCreated = true
	return execCtx.SaveState(ctx)
}

func (t *createDiskFromBaseDiskTask) checkEncryption(ctx context.Context) error {
	params := t.request.Params

	// Proxy overlay disks (and base disks) don't support encryption.
	if params.EncryptionDesc != nil &&
		params.EncryptionDesc.Mode != types.EncryptionMode_NO_ENCRYPTION {

		return errors.NewNonRetriableErrorf(
			"encrypted disk %v can't be created from base disk",
			params.Disk.DiskId,
		)
	}

	imageMeta, err := t.storage.GetImageMeta(ctx, t.request.SrcImageId)
	if err != nil {
		return err
	}

	if imageMeta != nil &&
		imageMeta.Encryption != nil &&
		imageMeta.Encryption.Mode != types.EncryptionMode_NO_ENCRYPTION {

		return errors.NewNonRetriableErrorf(
			"disk %v can't be created from encrypted image %v using base disk",
			params.Disk.DiskId,
			t.request.SrcImageId,
		)
	}

	return nil
}

// Proxy overlay disk lives in the same cell as the disk being created. Its id
// is derived from the task id (not from the image id) to make it unique per
// creation attempt: pools storage keeps tombstones for released slots for a
// while, so a repeated id would be rejected if the same disk is deleted and
// recreated shortly after.
func (t *createDiskFromBaseDiskTask) getProxyOverlayDisk(
	selfTaskID string,
) *types.Disk {

	return &types.Disk{
		ZoneId: t.state.SelectedCellId,
		DiskId: dataplane_common.GetProxyOverlayDiskID(
			t.proxyOverlayDiskIDPrefix,
			t.request.Params.Disk.DiskId,
			selfTaskID,
		),
	}
}

// Deletes the proxy overlay disk and releases the base disk slot acquired for
// it. Idempotent, safe to call even if neither has been created yet.
func (t *createDiskFromBaseDiskTask) deleteProxyOverlayDiskAndReleaseBaseDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.SelectedCellId) == 0 {
		// Cell is selected (and persisted) before anything is created, so
		// there is nothing to clean up.
		return nil
	}

	proxyOverlayDisk := t.getProxyOverlayDisk(execCtx.GetTaskID())

	client, err := t.nbsFactory.GetClient(ctx, proxyOverlayDisk.ZoneId)
	if err != nil {
		return err
	}

	// Should be deleted before the slot is released: the base disk may be
	// deleted right after its last slot is released.
	err = client.Delete(ctx, proxyOverlayDisk.DiskId)
	if err != nil {
		return err
	}

	taskID, err := t.poolService.ReleaseBaseDisk(
		headers.SetIncomingIdempotencyKey(ctx, execCtx.GetTaskID()+"_release"),
		&pools_protos.ReleaseBaseDiskRequest{
			OverlayDisk: proxyOverlayDisk,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	return err
}
