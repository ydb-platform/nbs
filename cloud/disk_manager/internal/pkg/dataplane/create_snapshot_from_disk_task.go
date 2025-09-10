package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotFromDiskTask struct {
	nbsFactory        nbs_client.Factory
	storage           storage.Storage
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	request           *protos.CreateSnapshotFromDiskRequest
	state             *protos.CreateSnapshotFromDiskTaskState
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

func (t *createSnapshotFromDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.run(ctx, execCtx)
	if err != nil {
		return err
	}

	nbsClient, err := t.getNbsClient(ctx)
	if err != nil {
		return err
	}

	if len(t.state.BaseSnapshotId) != 0 {
		err = nbsClient.DeleteCheckpoint(
			ctx,
			t.request.SrcDisk.DiskId,
			t.state.BaseCheckpointId,
		)
		if err != nil {
			return err
		}
	}

	return t.deleteProxyOverlayDiskIfNeeded(ctx)
}

func (t *createSnapshotFromDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.deleteProxyOverlayDiskIfNeeded(ctx)
	if err != nil {
		return err
	}

	_, err = t.storage.DeletingSnapshot(ctx, t.request.DstSnapshotId, execCtx.GetTaskID())
	if err != nil {
		return err
	}

	// NBS-3192.
	return t.unlockBaseSnapshot(ctx, execCtx)
}

func (t *createSnapshotFromDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.CreateSnapshotFromDiskMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *createSnapshotFromDiskTask) GetResponse() proto.Message {
	return &protos.CreateSnapshotFromDiskResponse{
		SnapshotSize:        t.state.SnapshotSize,
		SnapshotStorageSize: t.state.SnapshotStorageSize,
		TransferredDataSize: t.state.TransferredDataSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *createSnapshotFromDiskTask) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.ChunkCount != 0 {
		t.state.Progress =
			float64(t.state.MilestoneChunkIndex) / float64(t.state.ChunkCount)
	}

	logging.Debug(ctx, "saving state %+v", t.state)
	return execCtx.SaveState(ctx)
}

func (t *createSnapshotFromDiskTask) getNbsClient(
	ctx context.Context,
) (nbs_client.Client, error) {

	return t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
}

func (t *createSnapshotFromDiskTask) lockBaseSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	snapshotMeta storage.SnapshotMeta,
) (string, string, error) {

	nbsClient, err := t.getNbsClient(ctx)
	if err != nil {
		return "", "", err
	}

	diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return "", "", err
	}

	baseSnapshotID := snapshotMeta.BaseSnapshotID
	baseCheckpointID := snapshotMeta.BaseCheckpointID

	if diskParams.IsDiskRegistryBasedDisk {
		logging.Info(
			ctx,
			"Performing full snapshot %v of disk %v because it is Disk Registry based",
			snapshotMeta.ID,
			t.request.SrcDisk.DiskId,
		)
		// TODO: enable incremental snapshots for such disks.
		baseSnapshotID = ""
		baseCheckpointID = ""
	}

	if len(baseSnapshotID) != 0 {
		// Lock base snapshot to prevent deletion.
		locked, err := t.storage.LockSnapshot(
			ctx,
			baseSnapshotID,
			execCtx.GetTaskID(),
		)
		if err != nil {
			return "", "", err
		}

		if locked {
			logging.Info(
				ctx,
				"Locked snapshot with id %v",
				baseSnapshotID,
			)
		} else {
			logging.Info(
				ctx,
				"Snapshot with id %v can't be locked",
				baseSnapshotID,
			)
			logging.Info(
				ctx,
				"Performing full snapshot %v of disk %v",
				snapshotMeta.ID,
				t.request.SrcDisk.DiskId,
			)
			baseSnapshotID = ""
			baseCheckpointID = ""
		}
	}

	return baseSnapshotID, baseCheckpointID, nil
}

func (t *createSnapshotFromDiskTask) unlockBaseSnapshot(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if len(t.state.BaseSnapshotId) != 0 {
		err := t.storage.UnlockSnapshot(
			ctx,
			t.state.BaseSnapshotId,
			execCtx.GetTaskID(),
		)
		if err != nil {
			return err
		}
		logging.Info(
			ctx,
			"Unlocked snapshot with id %v",
			t.state.BaseSnapshotId,
		)
	}

	return nil
}

func (t *createSnapshotFromDiskTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.CreateSnapshot(
		ctx,
		storage.SnapshotMeta{
			ID:           t.request.DstSnapshotId,
			Disk:         t.request.SrcDisk,
			CheckpointID: t.request.SrcDiskCheckpointId,
			CreateTaskID: execCtx.GetTaskID(),
		},
	)
	if err != nil {
		return err
	}

	if snapshotMeta.Ready {
		// Already created.
		return nil
	}

	baseSnapshotID, baseCheckpointID, err := t.lockBaseSnapshot(ctx, execCtx, *snapshotMeta)
	if err != nil {
		return err
	}
	t.state.BaseSnapshotId = baseSnapshotID
	t.state.BaseCheckpointId = baseCheckpointID

	incremental := len(t.state.BaseSnapshotId) != 0

	err = t.setEstimate(
		ctx,
		execCtx,
		incremental,
	)
	if err != nil {
		return err
	}

	nbsClient, err := t.getNbsClient(ctx)
	if err != nil {
		return err
	}

	proxyOverlayDiskID, err := t.createProxyOverlayDiskIfNeeded(ctx, execCtx)
	if err != nil {
		return err
	}

	diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return err
	}

	source, err := nbs.NewDiskSource(
		ctx,
		nbsClient,
		t.request.SrcDisk.DiskId,
		proxyOverlayDiskID,
		t.state.BaseCheckpointId,
		t.request.SrcDiskCheckpointId,
		diskParams.EncryptionDesc,
		chunkSize,
		incremental, // duplicateChunkIndices
		false,       // ignoreBaseDisk
		false,       // dontReadFromCheckpoint
	)
	if err != nil {
		return err
	}
	defer source.Close(ctx)

	chunkCount, err := source.ChunkCount(ctx)
	if err != nil {
		return err
	}

	t.state.ChunkCount = chunkCount

	ignoreZeroChunks := true

	if incremental {
		// Don't ignore zero chunks for incremental snapshots.
		ignoreZeroChunks = false
	}

	target := snapshot.NewSnapshotTarget(
		selfTaskID,
		t.request.DstSnapshotId,
		t.storage,
		ignoreZeroChunks,
		t.request.UseS3,
	)
	defer target.Close(ctx)

	transferer := common.Transferer{
		ReaderCount:         t.config.GetReaderCount(),
		WriterCount:         t.config.GetWriterCount(),
		ChunksInflightLimit: t.config.GetChunksInflightLimit(),
		ChunkSize:           chunkSize,

		ShallowCopyWorkerCount:   t.config.SnapshotConfig.GetShallowCopyWorkerCount(),
		ShallowCopyInflightLimit: t.config.SnapshotConfig.GetShallowCopyInflightLimit(),
	}

	if incremental {
		shallowSource := snapshot.NewSnapshotShallowSource(
			t.state.BaseSnapshotId,
			t.request.DstSnapshotId,
			t.storage,
		)
		defer shallowSource.Close(ctx)

		transferer.ShallowSource = shallowSource
	}

	transferredChunkCount, err := transferer.Transfer(
		ctx,
		source,
		target,
		common.Milestone{
			ChunkIndex:            t.state.MilestoneChunkIndex,
			TransferredChunkCount: t.state.TransferredChunkCount,
		},
		func(ctx context.Context, milestone common.Milestone) error {
			if incremental {
				_, err := t.storage.CheckSnapshotReady(
					ctx,
					t.state.BaseSnapshotId,
				)
				if err != nil {
					return err
				}
			}

			err = t.storage.CheckSnapshotAlive(ctx, t.request.DstSnapshotId)
			if err != nil {
				return err
			}

			t.state.MilestoneChunkIndex = milestone.ChunkIndex
			t.state.TransferredChunkCount = milestone.TransferredChunkCount
			return t.saveProgress(ctx, execCtx)
		},
	)
	if err != nil {
		return err
	}

	t.state.MilestoneChunkIndex = t.state.ChunkCount
	t.state.TransferredChunkCount = transferredChunkCount
	t.state.Progress = 1

	dataChunkCount, err := t.storage.GetDataChunkCount(
		ctx,
		t.request.DstSnapshotId,
	)
	if err != nil {
		return err
	}

	size := uint64(chunkCount) * chunkSize
	storageSize := dataChunkCount * chunkSize

	t.state.SnapshotSize = size
	t.state.SnapshotStorageSize = storageSize
	t.state.TransferredDataSize =
		uint64(t.state.TransferredChunkCount) * chunkSize

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	err = t.unlockBaseSnapshot(ctx, execCtx)
	if err != nil {
		return err
	}

	return t.storage.SnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		size,
		storageSize,
		t.state.ChunkCount,
		diskParams.EncryptionDesc,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (t *createSnapshotFromDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	incremental bool,
) error {

	nbsClient, err := t.getNbsClient(ctx)
	if err != nil {
		return err
	}

	diskParams, err := nbsClient.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return err
	}

	diskSize := diskParams.BlocksCount * uint64(diskParams.BlockSize)

	bytesToReplicate, err := nbsClient.GetChangedBytes(
		ctx,
		t.request.SrcDisk.DiskId,
		t.state.BaseCheckpointId,
		t.request.SrcDiskCheckpointId,
		false, // ignoreBaseDisk
	)
	if err != nil {
		if !nbs_client.IsGetChangedBlocksNotSupportedError(err) {
			return err
		}

		bytesToReplicate = diskSize
	}

	logging.Info(ctx, "bytes to replicate is %v", bytesToReplicate)

	replicateDuration := performance.Estimate(
		bytesToReplicate,
		t.performanceConfig.GetTransferFromDiskToSnapshotBandwidthMiBs(),
	)
	shallowCopyDuration := performance.Estimate(
		diskSize,
		t.performanceConfig.GetSnapshotShallowCopyBandwidthMiBs(),
	)

	// Data transfer and shallow copy is performed in parallel
	execCtx.SetEstimatedInflightDuration(max(replicateDuration, shallowCopyDuration))

	return nil
}

func (t *createSnapshotFromDiskTask) createProxyOverlayDiskIfNeeded(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) (string, error) {

	if !t.request.UseProxyOverlayDisk {
		return "", nil
	}

	diskID := t.request.SrcDisk.DiskId
	proxyOverlayDiskID := t.getProxyOverlayDiskID()

	if t.state.ProxyOverlayDiskCreated != nil {
		if *t.state.ProxyOverlayDiskCreated {
			return proxyOverlayDiskID, nil
		}

		return "", nil
	}

	nbsClient, err := t.getNbsClient(ctx)
	if err != nil {
		return "", err
	}

	created, err := nbsClient.CreateProxyOverlayDisk(
		ctx,
		proxyOverlayDiskID,
		diskID, // baseDiskID
		t.request.SrcDiskCheckpointId,
	)
	if err != nil {
		return "", err
	}

	t.state.ProxyOverlayDiskCreated = &created

	err = execCtx.SaveState(ctx)
	if err != nil {
		return "", err
	}

	if created {
		return proxyOverlayDiskID, nil
	}

	return "", nil
}

func (t *createSnapshotFromDiskTask) deleteProxyOverlayDiskIfNeeded(
	ctx context.Context,
) error {

	if !t.request.UseProxyOverlayDisk {
		return nil
	}

	nbsClient, err := t.getNbsClient(ctx)
	if err != nil {
		return err
	}

	return nbsClient.Delete(ctx, t.getProxyOverlayDiskID())
}

func (t *createSnapshotFromDiskTask) getProxyOverlayDiskID() string {
	return common.GetProxyOverlayDiskID(
		t.config.GetProxyOverlayDiskIdPrefix(),
		t.request.SrcDisk.DiskId,
		t.request.DstSnapshotId,
	)
}
