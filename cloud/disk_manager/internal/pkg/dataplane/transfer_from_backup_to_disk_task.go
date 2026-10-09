package dataplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"math"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type transferFromBackupToDiskTask struct {
	config            *config.DataplaneConfig
	performanceConfig *performance_config.PerformanceConfig
	nbsFactory        nbs_client.Factory
	backupReader      backup.ObjectReader
	metrics           metrics.Metrics
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

	contents, err := t.readBackup(ctx)
	if err != nil {
		return err
	}

	if len(t.state.MetaSha256) == 0 && len(t.state.ChunkMapSha256) == 0 {
		t.state.MetaSha256 = contents.metaSHA256
		t.state.ChunkMapSha256 = contents.chunkMapSHA256
		t.state.ChunkCount = uint32(len(contents.chunkIDs))
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}
	} else if !bytes.Equal(t.state.MetaSha256, contents.metaSHA256) ||
		!bytes.Equal(t.state.ChunkMapSha256, contents.chunkMapSHA256) {

		return errors.NewNonRetriableErrorf(
			"backup metadata or chunk map changed for %q",
			t.request.SrcId,
		)
	}

	source := backup.NewSource(
		t.backupReader,
		contents.chunkIDs,
		contents.storageSize,
		t.metrics,
	)
	defer source.Close(ctx)
	t.setEstimate(ctx, execCtx, contents.storageSize)

	target, err := nbs.NewDiskTarget(
		ctx,
		t.nbsFactory,
		t.request.DstDisk,
		t.request.DstEncryption,
		chunkSize,
		t.config.GetTransferFromBackupToDiskIgnoreZeroChunks(),
		0, // fillGeneration
		0, // fillSeqNumber
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
			t.state.Progress =
				float64(milestone.ChunkIndex) / float64(t.state.ChunkCount)
			return execCtx.SaveState(ctx)
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
	switch t.request.SrcKind {
	case protos.TransferFromBackupToDiskRequest_SNAPSHOT:
		if len(t.request.SrcDiskId) == 0 {
			return errors.NewNonRetriableErrorf(
				"source disk id is required for a snapshot backup",
			)
		}
	case protos.TransferFromBackupToDiskRequest_IMAGE:
	default:
		return errors.NewNonRetriableErrorf(
			"invalid backup source kind: %v",
			t.request.SrcKind,
		)
	}

	if len(t.request.SrcId) == 0 {
		return errors.NewNonRetriableErrorf("backup source id is required")
	}
	if t.request.DstDisk == nil || len(t.request.DstDisk.ZoneId) == 0 ||
		len(t.request.DstDisk.DiskId) == 0 {

		return errors.NewNonRetriableErrorf(
			"destination zone or cell and disk id are required",
		)
	}

	return nil
}

type backupContents struct {
	chunkIDs       []string
	storageSize    uint64
	metaSHA256     []byte
	chunkMapSHA256 []byte
}

func (t *transferFromBackupToDiskTask) readBackup(
	ctx context.Context,
) (*backupContents, error) {

	metaKey := backup.ImageMetaKey(t.request.SrcId)
	if t.request.SrcKind == protos.TransferFromBackupToDiskRequest_SNAPSHOT {
		metaKey = backup.SnapshotMetaKey(t.request.SrcDiskId, t.request.SrcId)
	}
	object, err := t.backupReader.GetObject(ctx, metaKey)
	if err != nil {
		return nil, err
	}

	var id, folderID string
	var size, storageSize uint64
	if t.request.SrcKind == protos.TransferFromBackupToDiskRequest_SNAPSHOT {
		var meta backup.SnapshotMeta
		err = json.Unmarshal(object.Data, &meta)
		if err != nil {
			return nil, errors.NewNonRetriableErrorf(
				"invalid snapshot backup metadata %q: %v",
				metaKey,
				err,
			)
		}
		if meta.DiskID != t.request.SrcDiskId {
			return nil, errors.NewNonRetriableErrorf(
				"backup source disk id mismatch: expected %q, actual %q",
				t.request.SrcDiskId,
				meta.DiskID,
			)
		}

		id, folderID = meta.ID, meta.FolderID
		size, storageSize = meta.Size, meta.StorageSize
	} else {
		var meta backup.ImageMeta
		err = json.Unmarshal(object.Data, &meta)
		if err != nil {
			return nil, errors.NewNonRetriableErrorf(
				"invalid image backup metadata %q: %v",
				metaKey,
				err,
			)
		}

		id, folderID = meta.ID, meta.FolderID
		size, storageSize = meta.Size, meta.StorageSize
	}

	if id != t.request.SrcId {
		return nil, errors.NewNonRetriableErrorf(
			"backup id mismatch: expected %q, actual %q",
			t.request.SrcId,
			id,
		)
	}
	if len(t.request.ExpectedFolderId) != 0 &&
		folderID != t.request.ExpectedFolderId {

		return nil, errors.NewNonRetriableErrorf(
			"backup folder id mismatch: expected %q, actual %q",
			t.request.ExpectedFolderId,
			folderID,
		)
	}
	if size == 0 || size%chunkSize != 0 {
		return nil, errors.NewNonRetriableErrorf(
			"backup size %v must be a positive multiple of chunk size %v",
			size,
			chunkSize,
		)
	}

	// Divide before narrowing to the uint32 transfer interfaces.
	chunkCount := size / chunkSize
	if chunkCount > math.MaxUint32 {
		return nil, errors.NewNonRetriableErrorf(
			"backup chunk count %v exceeds uint32",
			chunkCount,
		)
	}
	metaHash := sha256.Sum256(object.Data)

	object, err = t.backupReader.GetObject(
		ctx,
		backup.ChunkMapKey(t.request.SrcId),
	)
	if err != nil {
		return nil, err
	}
	var chunkMap protos.BackupChunkMap
	err = proto.Unmarshal(object.Data, &chunkMap)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"invalid backup chunk map for %q: %v",
			t.request.SrcId,
			err,
		)
	}
	if uint64(len(chunkMap.ChunkIds)) != chunkCount {
		return nil, errors.NewNonRetriableErrorf(
			"backup chunk map length mismatch: expected %v, actual %v",
			chunkCount,
			len(chunkMap.ChunkIds),
		)
	}
	chunkMapHash := sha256.Sum256(object.Data)

	return &backupContents{
		chunkIDs:       chunkMap.ChunkIds,
		storageSize:    storageSize,
		metaSHA256:     metaHash[:],
		chunkMapSHA256: chunkMapHash[:],
	}, nil
}

func (t *transferFromBackupToDiskTask) setEstimate(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	bytesToTransfer uint64,
) {

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
}
