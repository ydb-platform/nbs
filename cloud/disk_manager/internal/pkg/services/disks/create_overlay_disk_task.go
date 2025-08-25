package disks

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools"
	pools_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type createOverlayDiskTask struct {
	storage     resources.Storage
	scheduler   tasks.Scheduler
	poolService pools.Service
	nbsFactory  nbs.Factory
	request     *protos.CreateOverlayDiskRequest
	state       *protos.CreateOverlayDiskTaskState
}

func (t *createOverlayDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createOverlayDiskTask) Load(request, state []byte) error {
	t.request = &protos.CreateOverlayDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateOverlayDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createOverlayDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	params := t.request.Params
	overlayDisk := params.Disk

	client, err := t.nbsFactory.GetClient(ctx, overlayDisk.ZoneId)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	diskMeta, err := t.storage.CreateDisk(ctx, resources.DiskMeta{
		ID:          overlayDisk.DiskId,
		ZoneID:      overlayDisk.ZoneId,
		SrcImageID:  t.request.SrcImageId,
		BlocksCount: params.BlocksCount,
		BlockSize:   params.BlockSize,
		Kind:        common.DiskKindToString(params.Kind),
		CloudID:     params.CloudId,
		FolderID:    params.FolderId,

		CreateRequest: params,
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

	taskID, err := t.poolService.AcquireBaseDisk(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_acquire"),
		&pools_protos.AcquireBaseDiskRequest{
			SrcImageId:      t.request.SrcImageId,
			OverlayDisk:     overlayDisk,
			OverlayDiskKind: params.Kind,
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

	err = client.Create(ctx, nbs.CreateDiskParams{
		ID:                      overlayDisk.DiskId,
		BaseDiskID:              baseDiskID,
		BaseDiskCheckpointID:    baseDiskCheckpointID,
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

	diskMeta.CreatedAt = time.Now()

	return t.storage.DiskCreated(ctx, *diskMeta)
}

func (t *createOverlayDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	params := t.request.Params
	overlayDisk := params.Disk

	client, err := t.nbsFactory.GetClient(ctx, overlayDisk.ZoneId)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	diskMeta, err := t.storage.DeleteDisk(
		ctx,
		overlayDisk.DiskId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if diskMeta == nil {
		return errors.NewNonCancellableErrorf(
			"id %v is not accepted",
			overlayDisk.DiskId,
		)
	}

	if len(diskMeta.ZoneID) == 0 {
		// If diskMeta has no zoneID, the disk was not in the database before
		// calling storage.DeleteDisk, so it has already been marked as deleted.
		// Need to call neither storage.DiskDeleted, nor client.Delete, nor
		// poolService.ReleaseBaseDisk.
		return nil
	}

	err = client.Delete(ctx, overlayDisk.DiskId)
	if err != nil {
		return err
	}

	taskID, err := t.poolService.ReleaseBaseDisk(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_release"),
		&pools_protos.ReleaseBaseDiskRequest{
			OverlayDisk: overlayDisk,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	return t.storage.DiskDeleted(ctx, overlayDisk.DiskId, time.Now())
}

func (t *createOverlayDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &disk_manager.CreateDiskMetadata{}, nil
}

func (t *createOverlayDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func parseAcquireBaseDiskResponse(
	response proto.Message,
) (string, string, error) {

	typedResponse, ok := response.(*pools_protos.AcquireBaseDiskResponse)
	if !ok {
		return "", "", errors.NewNonRetriableErrorf(
			"invalid acquire base disk response type %v",
			response,
		)
	}

	baseDiskID := typedResponse.BaseDiskId
	if baseDiskID == "" {
		return "", "", errors.NewNonRetriableErrorf(
			"baseDiskID should not be empty",
		)
	}

	baseDiskCheckpointID := typedResponse.BaseDiskCheckpointId
	if baseDiskCheckpointID == "" {
		return "", "", errors.NewNonRetriableErrorf(
			"baseDiskCheckpointID should not be empty",
		)
	}

	return baseDiskID, baseDiskCheckpointID, nil
}
