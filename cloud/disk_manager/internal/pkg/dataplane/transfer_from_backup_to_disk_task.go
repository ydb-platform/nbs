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
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

// Fields of backup.SnapshotMeta and backup.ImageMeta needed to restore a disk.
type backupMeta struct {
	id          string
	folderID    string
	size        uint64
	storageSize uint64
}

////////////////////////////////////////////////////////////////////////////////

// Fills an existing disk with data of the snapshot or image backed up to S3.
// Needs neither resource nor snapshot tables.
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

	meta, metaData, err := t.readMeta(ctx)
	if err != nil {
		return err
	}

	chunkIDs, chunkMapData, err := t.readChunkMap(ctx, meta)
	if err != nil {
		return err
	}

	err = t.pinBackup(ctx, execCtx, metaData, chunkMapData, len(chunkIDs))
	if err != nil {
		return err
	}

	source := backup.NewBackupSource(
		t.backupReader,
		chunkIDs,
		meta.storageSize,
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

// Checks what nothing else does: without it an empty id addresses a malformed
// key, which is reported as a silent error, and a missing destination is a nil
// dereference. Source kind is checked by readMeta.
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

// Reads the object and parses it as JSON into |value|. Returns the object's
// data.
func (t *transferFromBackupToDiskTask) readJSON(
	ctx context.Context,
	key string,
	value any,
) ([]byte, error) {

	object, err := t.backupReader.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(object.Data, value)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse %v: %w",
			key,
			err,
		)
	}

	return object.Data, nil
}

func (t *transferFromBackupToDiskTask) readSnapshotMeta(
	ctx context.Context,
) (backupMeta, []byte, error) {

	srcDiskID := t.request.SrcDiskId
	key := backup.SnapshotMetaKey(srcDiskID, t.request.SrcId)

	var meta backup.SnapshotMeta
	data, err := t.readJSON(ctx, key, &meta)
	if err != nil {
		return backupMeta{}, nil, err
	}

	if meta.DiskID != srcDiskID {
		return backupMeta{}, nil, errors.NewNonRetriableErrorf(
			"%v has disk id %q, expected %q",
			key,
			meta.DiskID,
			srcDiskID,
		)
	}

	return backupMeta{
		id:          meta.ID,
		folderID:    meta.FolderID,
		size:        meta.Size,
		storageSize: meta.StorageSize,
	}, data, nil
}

func (t *transferFromBackupToDiskTask) readImageMeta(
	ctx context.Context,
) (backupMeta, []byte, error) {

	var meta backup.ImageMeta
	data, err := t.readJSON(ctx, backup.ImageMetaKey(t.request.SrcId), &meta)
	if err != nil {
		return backupMeta{}, nil, err
	}

	return backupMeta{
		id:          meta.ID,
		folderID:    meta.FolderID,
		size:        meta.Size,
		storageSize: meta.StorageSize,
	}, data, nil
}

// Returns validated meta of the backup and the data it was parsed from.
func (t *transferFromBackupToDiskTask) readMeta(
	ctx context.Context,
) (backupMeta, []byte, error) {

	var meta backupMeta
	var data []byte
	var err error

	switch t.request.SrcKind {
	case protos.BackupSourceKind_BACKUP_SOURCE_KIND_SNAPSHOT:
		meta, data, err = t.readSnapshotMeta(ctx)
	case protos.BackupSourceKind_BACKUP_SOURCE_KIND_IMAGE:
		meta, data, err = t.readImageMeta(ctx)
	default:
		err = errors.NewNonRetriableErrorf(
			"unknown backup source kind %v",
			t.request.SrcKind,
		)
	}
	if err != nil {
		return backupMeta{}, nil, err
	}

	srcID := t.request.SrcId

	if meta.id != srcID {
		return backupMeta{}, nil, errors.NewNonRetriableErrorf(
			"meta of backup %v has id %q",
			srcID,
			meta.id,
		)
	}

	expectedFolderID := t.request.ExpectedFolderId
	if len(expectedFolderID) != 0 && meta.folderID != expectedFolderID {
		return backupMeta{}, nil, errors.NewNonRetriableErrorf(
			"backup %v has folder id %q, expected %q",
			srcID,
			meta.folderID,
			expectedFolderID,
		)
	}

	if meta.size == 0 || meta.size%chunkSize != 0 {
		return backupMeta{}, nil, errors.NewNonRetriableErrorf(
			"backup %v has size %v, expected non-zero multiple of %v",
			srcID,
			meta.size,
			chunkSize,
		)
	}

	// Chunk indices are uint32.
	if meta.size/chunkSize > math.MaxUint32 {
		return backupMeta{}, nil, errors.NewNonRetriableErrorf(
			"backup %v of size %v has too many chunks",
			srcID,
			meta.size,
		)
	}

	return meta, data, nil
}

// Returns chunk ids of the backup by chunk index and the data they were parsed
// from.
func (t *transferFromBackupToDiskTask) readChunkMap(
	ctx context.Context,
	meta backupMeta,
) ([]string, []byte, error) {

	key := backup.ChunkMapKey(t.request.SrcId)

	object, err := t.backupReader.GetObject(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	chunkMap := &protos.BackupChunkMap{}
	err = proto.Unmarshal(object.Data, chunkMap)
	if err != nil {
		return nil, nil, errors.NewNonRetriableErrorf(
			"failed to parse %v: %w",
			key,
			err,
		)
	}

	chunkCount := meta.size / chunkSize
	if uint64(len(chunkMap.ChunkIds)) != chunkCount {
		return nil, nil, errors.NewNonRetriableErrorf(
			"%v has %v chunks, expected %v",
			key,
			len(chunkMap.ChunkIds),
			chunkCount,
		)
	}

	return chunkMap.ChunkIds, object.Data, nil
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
