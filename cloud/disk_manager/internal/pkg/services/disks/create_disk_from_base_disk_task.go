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
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
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
// Pools are configured per cell, so if it turns out that no pool is configured
// for the image in the cell selected for the disk, the data is transferred
// from the snapshot storage after all (see selectFillSource).
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
	performanceConfig        *performance_config.PerformanceConfig
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

	if t.state.FillSource == protos.FillSource_FILL_SOURCE_BASE_DISK {
		// Proxy overlay disk is a one-shot entity: it is not needed anymore
		// once the data has been transferred. Performed outside of
		// createAndFillDisk, after DataTransferred has been persisted, so that
		// a retry performs it even though createAndFillDisk is skipped.
		err = t.deleteProxyOverlayDiskAndReleaseBaseDisk(ctx, execCtx)
		if err != nil {
			return err
		}
	}

	diskMeta.CreatedAt = time.Now()
	return t.storage.DiskCreated(ctx, *diskMeta)
}

func (t *createDiskFromBaseDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.FillSource == protos.FillSource_FILL_SOURCE_BASE_DISK {
		// Should be done regardless of the disk state: the disk may have
		// already been deleted by a concurrent disks.DeleteDisk task, which
		// knows nothing about the proxy overlay disk and the base disk slot
		// acquired for it. Nothing is acquired before the source is persisted.
		err := t.deleteProxyOverlayDiskAndReleaseBaseDisk(ctx, execCtx)
		if err != nil {
			return err
		}
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

		switch transferMetadata := message.(type) {
		case *dataplane_protos.TransferFromDiskToDiskMetadata:
			metadata.Progress = transferMetadata.Progress
		case *dataplane_protos.TransferFromSnapshotToDiskMetadata:
			metadata.Progress = transferMetadata.Progress
		}
	}

	return metadata, nil
}

func (t *createDiskFromBaseDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

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

	// The proxy overlay disk has the size and the block size of the base disk,
	// the latter may differ from the block size of the disk being created (the
	// transfer operates on chunks that are multiples of both).
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

// Acquires a base disk slot for the proxy overlay disk and creates the proxy
// overlay disk on top of the acquired base disk. Returns the base disk and its
// checkpoint.
func (t *createDiskFromBaseDiskTask) prepareProxyOverlayDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	client nbs.Client,
) (baseDiskID string, baseDiskCheckpointID string, err error) {

	selfTaskID := execCtx.GetTaskID()
	proxyOverlayDisk := t.getProxyOverlayDisk(selfTaskID)

	// The transfer reads the base disk at the transfer bandwidth regardless of
	// the size of the disk being created, so the slot is charged by this
	// bandwidth rather than by the overlay disk size.
	taskID, err := t.poolService.AcquireBaseDisk(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_acquire"),
		&pools_protos.AcquireBaseDiskRequest{
			SrcImageId:                t.request.SrcImageId,
			OverlayDisk:               proxyOverlayDisk,
			OverlayDiskKind:           types.DiskKind_DISK_KIND_SSD,
			ExpectedReadBandwidthMiBs: t.performanceConfig.GetTransferFromDiskToDiskBandwidthMiBs(),
		},
	)
	if err != nil {
		return "", "", err
	}

	acquireResponse, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return "", "", err
	}

	baseDiskID, baseDiskCheckpointID, err = parseAcquireBaseDiskResponse(
		acquireResponse,
	)
	if err != nil {
		return "", "", err
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
		return "", "", err
	}

	return baseDiskID, baseDiskCheckpointID, nil
}

// Schedules the transfer of the data from the proxy overlay disk (prepared by
// prepareProxyOverlayDisk) into the disk.
func (t *createDiskFromBaseDiskTask) scheduleTransferFromProxyOverlayDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	disk *types.Disk,
	baseDiskID string,
	baseDiskCheckpointID string,
) (string, error) {

	params := t.request.Params
	selfTaskID := execCtx.GetTaskID()

	return t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_transfer_from_base_disk"),
		"dataplane.TransferFromDiskToDisk",
		// Image id and disk kind are not present in the transfer request.
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
			SrcDisk:             t.getProxyOverlayDisk(selfTaskID),
			SrcDiskCheckpointId: baseDiskCheckpointID,
			MountSrcDiskLocally: true,
			DstDisk:             disk,
			// The proxy overlay disk has the size of the base disk (4 TiB
			// by default), which may be much larger than the disk being
			// created, the image fits into both. Without this bound the
			// transfer would scan the unused tail of the proxy overlay disk
			// (chunk by chunk) to estimate the amount of data to transfer,
			// and it does that from scratch on every restart.
			MaxSizeBytes: params.BlocksCount * uint64(params.BlockSize),
		},
	)
}

// Fallback for the case when no pool is configured for the image in the
// selected cell: the same transfer as in disks.CreateDiskFromImage.
func (t *createDiskFromBaseDiskTask) scheduleTransferFromSnapshotStorage(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	disk *types.Disk,
) (string, error) {

	return t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(
			ctx,
			execCtx.GetTaskID()+"_transfer_from_snapshot_storage",
		),
		"dataplane.TransferFromSnapshotToDisk",
		"",
		disk.ZoneId,
		&dataplane_protos.TransferFromSnapshotToDiskRequest{
			SrcSnapshotId: t.request.SrcImageId,
			DstDisk:       disk,
			DstEncryption: t.request.Params.EncryptionDesc,
		},
	)
}

// Pools are configured per cell, and the cell is selected by the task (the
// disk service only checks that a pool is configured for the requested zone),
// so the source is chosen here, once the cell is selected. The pool may be
// reconfigured while the task is running, so the choice is made once and
// persisted before anything is acquired for the proxy overlay disk.
func (t *createDiskFromBaseDiskTask) selectFillSource(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	cellID string,
) error {

	if t.state.FillSource != protos.FillSource_FILL_SOURCE_UNSPECIFIED {
		return nil
	}

	configured, err := t.poolService.IsPoolConfigured(
		ctx,
		t.request.SrcImageId,
		cellID,
	)
	if err != nil {
		return err
	}

	if configured {
		t.state.FillSource = protos.FillSource_FILL_SOURCE_BASE_DISK
	} else {
		logging.Info(
			ctx,
			"no pool is configured for image %v in cell %v, disk %v is filled from the snapshot storage",
			t.request.SrcImageId,
			cellID,
			t.request.Params.Disk.DiskId,
		)
		t.state.FillSource = protos.FillSource_FILL_SOURCE_SNAPSHOT_STORAGE
	}

	return execCtx.SaveState(ctx)
}

// Creates the disk and fills it with the image data: from a base disk (via a
// proxy overlay disk) if a pool is configured for the image in the selected
// cell, from the snapshot storage otherwise.
func (t *createDiskFromBaseDiskTask) createAndFillDisk(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	client nbs.Client,
	disk *types.Disk,
) error {

	params := t.request.Params

	err := t.selectFillSource(ctx, execCtx, disk.ZoneId)
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

	var taskID string
	if t.state.FillSource == protos.FillSource_FILL_SOURCE_SNAPSHOT_STORAGE {
		taskID, err = t.scheduleTransferFromSnapshotStorage(ctx, execCtx, disk)
	} else {
		var baseDiskID, baseDiskCheckpointID string
		baseDiskID, baseDiskCheckpointID, err = t.prepareProxyOverlayDisk(
			ctx,
			execCtx,
			client,
		)
		if err != nil {
			return err
		}

		taskID, err = t.scheduleTransferFromProxyOverlayDisk(
			ctx,
			execCtx,
			disk,
			baseDiskID,
			baseDiskCheckpointID,
		)
	}
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
