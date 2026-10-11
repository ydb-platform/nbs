package dataplane

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"

	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

// Fills an existing disk with data of the snapshot or image backed up to S3.
// Needs neither resource storage nor snapshot storage.
type transferFromBackupToDiskTask struct {
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	nbsFactory        nbs_client.Factory
	backupReader      backup.ObjectReader
	metrics           storage_metrics.Metrics
	request           *protos.TransferFromBackupToDiskRequest
	state             *protos.TransferFromBackupToDiskTaskState
}

func (t *transferFromBackupToDiskTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *transferFromBackupToDiskTask) Load(request, state []byte) error {
	t.request = &protos.TransferFromBackupToDiskRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.TransferFromBackupToDiskTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *transferFromBackupToDiskTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.validateRequest()
	if err != nil {
		return err
	}

	loaded, err := backup.Load(
		ctx,
		t.backupReader,
		backup.LoadOptions{
			Kind:             t.request.SrcKind,
			ID:               t.request.SrcId,
			DiskID:           t.request.SrcDiskId,
			ExpectedFolderID: t.request.ExpectedFolderId,
		},
	)

	if err != nil {
		return err
	}

	err = t.pinBackup(
		ctx,
		execCtx,
		loaded.MetaData,
		loaded.ChunkMapData,
		len(loaded.ChunkIDs),
	)
	if err != nil {
		return err
	}

	source := backup.NewBackupSource(
		t.backupReader,
		loaded.ChunkIDs,
		loaded.Meta.StorageSize,
		t.metrics,
	)
	defer source.Close(ctx)

	err = t.setEstimate(ctx, execCtx, source)
	if err != nil {
		return err
	}

	target, err := nbs.NewDiskTarget(
		ctx,
		t.nbsFactory,
		t.request.DstDisk,
		t.request.DstEncryption,
		chunkSize,
		t.config.GetTransferFromBackupToDiskIgnoreZeroChunks(),
		0, // Fill generation.
		0, // Fill sequence number.
	)
	if err != nil {
		return err
	}
	defer target.Close(ctx)

	transferer := common.Transferer{
		ReaderCount:         t.config.GetReaderCount(),
		WriterCount:         t.config.GetWriterCount(),
		ChunksInflightLimit: t.config.GetChunksInflightLimit(),
		ChunkSize:           chunkSize,
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
	return nil
}

func (t *transferFromBackupToDiskTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *transferFromBackupToDiskTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.TransferFromBackupToDiskMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *transferFromBackupToDiskTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *transferFromBackupToDiskTask) validateRequest() error {
	if len(t.request.SrcId) == 0 {
		return errors.NewNonRetriableErrorf("backup source id is empty")
	}

	isSnapshot := t.request.SrcKind ==
		protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT
	if isSnapshot && len(t.request.SrcDiskId) == 0 {
		return errors.NewNonRetriableErrorf(
			"source disk id is required for backup of snapshot %v",
			t.request.SrcId,
		)
	}

	dstDisk := t.request.DstDisk
	if dstDisk == nil || len(dstDisk.ZoneId) == 0 || len(dstDisk.DiskId) == 0 {
		return errors.NewNonRetriableErrorf(
			"destination zone id and disk id are required, got %v",
			dstDisk,
		)
	}

	return nil
}

// Milestone saved in the state is valid only for the meta and the chunk map
// seen by the first attempt, so the following attempts should see the same.
func (t *transferFromBackupToDiskTask) pinBackup(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	metaData []byte,
	chunkMapData []byte,
	chunkCount int,
) error {

	metaHash := sha256.Sum256(metaData)
	chunkMapHash := sha256.Sum256(chunkMapData)

	if len(t.state.MetaSha256) == 0 {
		t.state.MetaSha256 = metaHash[:]
		t.state.ChunkMapSha256 = chunkMapHash[:]
		t.state.ChunkCount = uint32(chunkCount)
		return execCtx.SaveState(ctx)
	}

	if !bytes.Equal(t.state.MetaSha256, metaHash[:]) {
		return errors.NewNonRetriableErrorf(
			"meta of backup %v has changed",
			t.request.SrcId,
		)
	}

	if !bytes.Equal(t.state.ChunkMapSha256, chunkMapHash[:]) {
		return errors.NewNonRetriableErrorf(
			"chunk map of backup %v has changed",
			t.request.SrcId,
		)
	}

	return nil
}

func (t *transferFromBackupToDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	source common.Source,
) error {

	bytesToTransfer, err := source.EstimatedBytesToRead(ctx)
	if err != nil {
		return err
	}

	bandwidth := t.performanceConfig.
		GetTransferBetweenDiskAndSnapshotBandwidthMiBs()
	if t.request.DstEncryption != nil {
		bandwidth = t.performanceConfig.
			GetTransferBetweenEncryptedDiskAndSnapshotBandwidthMiBs()
	}

	estimatedDuration := performance.Estimate(bytesToTransfer, bandwidth)
	execCtx.SetEstimatedInflightDuration(estimatedDuration)

	logging.Info(
		ctx,
		"bytes to transfer is %v, has encryption is %v, "+
			"estimated duration is %v",
		bytesToTransfer,
		t.request.DstEncryption != nil,
		estimatedDuration,
	)

	return nil
}

func (t *transferFromBackupToDiskTask) saveProgress(
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
