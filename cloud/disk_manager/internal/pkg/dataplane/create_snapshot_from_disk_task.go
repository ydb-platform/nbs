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
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type createSnapshotFromDiskTask struct {
	nbsFactory nbs_client.Factory
	storage    storage.Storage
	config     *config.DataplaneConfig
	request    *protos.CreateSnapshotFromDiskRequest
	state      *protos.CreateSnapshotFromDiskTaskState
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

	return t.storage.DeletingSnapshot(ctx, t.request.DstSnapshotId)
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

	if t.state.Progress == 1 {
		return nil
	}

	if t.state.ChunkCount != 0 {
		t.state.Progress =
			float64(t.state.MilestoneChunkIndex) / float64(t.state.ChunkCount)
	}

	return execCtx.SaveState(ctx)
}

func (t *createSnapshotFromDiskTask) run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	selfTaskID := execCtx.GetTaskID()

	snapshotMeta, err := t.storage.CreateSnapshot(
		ctx,
		storage.SnapshotMeta{
			ID:             t.request.DstSnapshotId,
			Disk:           t.request.SrcDisk,
			CheckpointID:   t.request.SrcDiskCheckpointId,
			BaseSnapshotID: t.request.BaseSnapshotId,
		},
	)
	if err != nil {
		return err
	}

	if snapshotMeta.Ready {
		// Already created.
		return nil
	}

	client, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return err
	}

	proxyOverlayDiskID, err := t.createProxyOverlayDiskIfNeeded(
		ctx,
		execCtx,
		client,
	)
	if err != nil {
		return err
	}

	diskParams, err := client.Describe(ctx, t.request.SrcDisk.DiskId)
	if err != nil {
		return err
	}

	incremental := len(t.request.BaseSnapshotId) != 0

	source, err := nbs.NewDiskSource(
		ctx,
		client,
		t.request.SrcDisk.DiskId,
		proxyOverlayDiskID,
		t.request.SrcDiskBaseCheckpointId,
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
			t.request.BaseSnapshotId,
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
					t.request.BaseSnapshotId,
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

	return t.storage.SnapshotCreated(
		ctx,
		t.request.DstSnapshotId,
		size,
		storageSize,
		t.state.ChunkCount,
		diskParams.EncryptionDesc,
	)
}

func (t *createSnapshotFromDiskTask) createProxyOverlayDiskIfNeeded(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	client nbs_client.Client,
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

	created, err := client.CreateProxyOverlayDisk(
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

	client, err := t.nbsFactory.GetClient(ctx, t.request.SrcDisk.ZoneId)
	if err != nil {
		return err
	}

	return client.Delete(ctx, t.getProxyOverlayDiskID())
}

func (t *createSnapshotFromDiskTask) getProxyOverlayDiskID() string {
	return common.GetProxyOverlayDiskID(
		t.config.GetProxyOverlayDiskIdPrefix(),
		t.request.SrcDisk.DiskId,
		t.request.DstSnapshotId,
	)
}
