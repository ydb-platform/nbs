package snapshots

import (
	"context"
	"encoding/json"
	"time"

	"github.com/golang/protobuf/proto"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	internal_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/bucket"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotFromDiskTask struct {
	scheduler    tasks.Scheduler
	storage      resources.Storage
	nbsFactory   nbs.Factory
	snapshotS3   *bucket.S3
	request      *protos.CreateSnapshotFromDiskRequest
	state        *protos.CreateSnapshotFromDiskTaskState
	cellSelector cells.CellSelector
}

func (t *createSnapshotFromDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createSnapshotFromDiskTask) Load(request, state []byte) error {
	t.request = &protos.CreateSnapshotFromDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateSnapshotFromDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createSnapshotFromDiskTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nbsClient nbs.Client,
	disk *types.Disk,
) (string, error) {

	selfTaskID := execCtx.GetTaskID()

	diskParams, err := nbsClient.Describe(ctx, disk.DiskId)
	if err != nil {
		return "", err
	}

	if internal_common.IsLocalDiskKind(diskParams.Kind) {
		return "", errors.NewNonCancellableErrorf(
			"snapshot creation from local disk is forbidden",
		)
	}

	snapshotMeta, err := t.storage.CreateSnapshot(ctx, resources.SnapshotMeta{
		BlockSize:     diskParams.BlockSize,
		ID:            t.request.DstSnapshotId,
		FolderID:      t.request.FolderId,
		Disk:          disk,
		CreateRequest: t.request,
		CreateTaskID:  selfTaskID,
		CreatingAt:    time.Now(),
		CreatedBy:     "", // TODO: extract CreatedBy from execCtx.
		Encryption:    diskParams.EncryptionDesc,
	})
	if err != nil {
		return "", err
	}

	if snapshotMeta.Ready {
		// Already created.
		return snapshotMeta.CheckpointID, nil
	}

	checkpointID, err := common.CreateCheckpoint(
		ctx,
		execCtx,
		t.scheduler,
		nbsClient,
		disk,
		t.request.DstSnapshotId,
		selfTaskID,
		diskParams.IsDiskRegistryBasedDisk,
		t.request.RetryBrokenDRBasedDiskCheckpoint,
	)
	if err != nil {
		return "", err
	}

	taskID, err := t.scheduler.ScheduleZonalTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_run"),
		"dataplane.CreateSnapshotFromDisk",
		"",
		disk.ZoneId,
		&dataplane_protos.CreateSnapshotFromDiskRequest{
			BlockSize:           diskParams.BlockSize,
			SrcDisk:             disk,
			SrcDiskCheckpointId: checkpointID,
			DstSnapshotId:       t.request.DstSnapshotId,
			UseS3:               t.request.UseS3,
			UseProxyOverlayDisk: t.request.UseProxyOverlayDisk,
		},
	)
	if err != nil {
		return "", err
	}

	t.state.DataplaneTaskID = taskID

	response, err := t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return "", err
	}

	typedResponse, ok := response.(*dataplane_protos.CreateSnapshotFromDiskResponse)
	if !ok {
		return "", errors.NewNonRetriableErrorf(
			"invalid dataplane.CreateSnapshotFromDisk response type %T",
			response,
		)
	}

	t.state.SnapshotSize = int64(typedResponse.SnapshotSize)
	t.state.SnapshotStorageSize = int64(typedResponse.SnapshotStorageSize)

	err = execCtx.SaveState(ctx)
	if err != nil {
		return "", err
	}

	// The meta goes to the snapshot bucket right before the snapshot is marked
	// ready: an S3 snapshot with meta.json there is created, as with a backup.
	err = t.putMeta(ctx, snapshotMeta, checkpointID, typedResponse)
	if err != nil {
		return "", err
	}

	err = t.storage.SnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		checkpointID,
		time.Now(),
		uint64(t.state.SnapshotSize),
		uint64(t.state.SnapshotStorageSize),
	)
	if err != nil {
		return "", err
	}

	return checkpointID, nil
}

func (t *createSnapshotFromDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Disk cell may differ from the zone presented in the request.
	disk, err := t.cellSelector.ReplaceZoneIdWithCellIdInDiskMeta(
		ctx,
		t.storage,
		t.request.SrcDisk,
	)
	if err != nil {
		return err
	}

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	checkpointID, err := t.run(ctx, execCtx, nbsClient, disk)
	if err != nil {
		return err
	}

	diskParams, err := nbsClient.Describe(ctx, disk.DiskId)
	if err != nil {
		return err
	}

	if diskParams.IsDiskRegistryBasedDisk {
		return nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
	}

	return nbsClient.DeleteCheckpointData(ctx, disk.DiskId, checkpointID)
}

func (t *createSnapshotFromDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// Disk cell may differ from the zone presented in the request.
	disk, err := t.cellSelector.ReplaceZoneIdWithCellIdInDiskMeta(
		ctx,
		t.storage,
		t.request.SrcDisk,
	)
	if err != nil {
		return err
	}

	nbsClient, err := t.nbsFactory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return err
	}

	selfTaskID := execCtx.GetTaskID()

	checkpointID, err := common.CancelCheckpointCreation(
		ctx,
		t.scheduler,
		nbsClient,
		disk,
		t.request.DstSnapshotId,
		selfTaskID,
		t.request.RetryBrokenDRBasedDiskCheckpoint,
	)
	if err != nil {
		return err
	}

	if checkpointID != "" {
		err = nbsClient.DeleteCheckpoint(ctx, disk.DiskId, checkpointID)
		if err != nil {
			return err
		}
	}

	snapshotMeta, err := t.storage.DeleteSnapshot(
		ctx,
		t.request.DstSnapshotId,
		selfTaskID,
		time.Now(),
	)
	if err != nil {
		return err
	}

	if snapshotMeta == nil {
		// Nothing to do.
		return nil
	}

	// Hack for NBS-2225.
	if snapshotMeta.DeleteTaskID != selfTaskID {
		return t.scheduler.WaitTaskEnded(ctx, snapshotMeta.DeleteTaskID)
	}

	taskID, err := t.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, selfTaskID+"_cancel"),
		"dataplane.DeleteSnapshot",
		"",
		&dataplane_protos.DeleteSnapshotRequest{
			SnapshotId: t.request.DstSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	_, err = t.scheduler.WaitTask(ctx, execCtx, taskID)
	if err != nil {
		return err
	}

	if t.snapshotS3 != nil {
		err = t.snapshotS3.DeleteSnapshotMeta(
			ctx,
			snapshotMeta.Disk.GetDiskId(),
			t.request.DstSnapshotId,
		)
		if err != nil {
			return err
		}
	}

	return t.storage.SnapshotDeleted(ctx, t.request.DstSnapshotId, time.Now())
}

func (t *createSnapshotFromDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	metadata := &disk_manager.CreateSnapshotMetadata{}

	if len(t.state.DataplaneTaskID) != 0 {
		message, err := t.scheduler.GetTaskMetadata(
			ctx,
			t.state.DataplaneTaskID,
		)
		if err != nil {
			return nil, err
		}

		createMetadata, ok := message.(*dataplane_protos.CreateSnapshotFromDiskMetadata)
		if ok {
			metadata.Progress = createMetadata.Progress
		}
	} else {
		metadata.Progress = t.state.Progress
	}

	return metadata, nil
}

func (t *createSnapshotFromDiskTask) GetResponse() proto.Message {
	return &disk_manager.CreateSnapshotResponse{
		Size:        t.state.SnapshotSize,
		StorageSize: t.state.SnapshotStorageSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

// Writes meta.json of the snapshot to the snapshot bucket, as the backup
// writes it to the follower. A snapshot stored in YDB has nothing in the
// bucket.
func (t *createSnapshotFromDiskTask) putMeta(
	ctx context.Context,
	snapshotMeta resources.SnapshotMeta,
	checkpointID string,
	response *dataplane_protos.CreateSnapshotFromDiskResponse,
) error {

	if t.snapshotS3 == nil || !t.request.UseS3 {
		return nil
	}

	// What SnapshotCreated is about to store.
	snapshotMeta.CheckpointID = checkpointID
	snapshotMeta.Size = response.SnapshotSize
	snapshotMeta.StorageSize = response.SnapshotStorageSize

	meta, err := bucket.NewSnapshotMeta(snapshotMeta)
	if err != nil {
		return err
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	return t.snapshotS3.PutObject(
		ctx,
		bucket.SnapshotMetaKey(snapshotMeta.Disk.GetDiskId(), snapshotMeta.ID),
		persistence.S3Object{Data: data},
	)
}
