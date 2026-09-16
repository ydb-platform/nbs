package backup

import (
	"context"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupSnapshotTask struct {
	storage          storage.Storage
	s3               *persistence.S3Client
	bucket           string
	keyPrefix        string
	enqueueBatchSize int
	request          *protos.BackupSnapshotRequest
	state            *protos.BackupSnapshotTaskState
}

func (t *backupSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	meta, err := t.storage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return err
	}

	var diskID string
	if meta.Disk != nil {
		diskID = meta.Disk.DiskId
	}

	t.state.ChunkCount = meta.ChunkCount

	err = t.enqueueChunks(ctx, execCtx)
	if err != nil {
		return err
	}

	has, err := t.storage.HasBackupQueueEntries(ctx, snapshotID)
	if err != nil {
		return err
	}

	if has {
		logging.Debug(
			ctx,
			"backup of snapshot %v is waiting for its chunks to be copied",
			snapshotID,
		)
		return errors.NewInterruptExecutionError()
	}

	err = t.writeMap(ctx, diskID, meta)
	if err != nil {
		return err
	}

	t.state.Progress = 1
	return nil
}

func (t *backupSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *backupSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.BackupSnapshotMetadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *backupSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *backupSnapshotTask) enqueueChunks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	if t.state.MilestoneChunkIndex >= t.state.ChunkCount {
		return nil
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, errs := t.storage.ReadChunkMap(
		readCtx,
		snapshotID,
		t.state.MilestoneChunkIndex,
	)

	var batch []storage.BackupQueueEntry
	var lastChunkIndex uint32

	flush := func() error {
		err := t.storage.EnqueueBackupChunks(ctx, batch)
		if err != nil {
			return err
		}

		t.state.EnqueuedChunkCount += uint32(len(batch))
		t.state.MilestoneChunkIndex = lastChunkIndex + 1
		batch = nil
		return t.saveProgress(ctx, execCtx)
	}

	for entry := range entries {
		lastChunkIndex = entry.ChunkIndex

		if len(entry.ChunkID) == 0 || !isOwnChunk(entry.ChunkID, snapshotID) {
			continue
		}

		if !entry.StoredInS3 {
			return errors.NewNonRetriableErrorf(
				"chunk %v of snapshot %v is not stored in s3",
				entry.ChunkID,
				snapshotID,
			)
		}

		batch = append(batch, storage.BackupQueueEntry{
			SnapshotID: snapshotID,
			ChunkID:    entry.ChunkID,
		})

		if len(batch) >= t.enqueueBatchSize {
			err := flush()
			if err != nil {
				return err
			}
		}
	}

	err := <-errs
	if err != nil {
		return err
	}

	err = flush()
	if err != nil {
		return err
	}

	t.state.MilestoneChunkIndex = t.state.ChunkCount
	return t.saveProgress(ctx, execCtx)
}

func (t *backupSnapshotTask) writeMap(
	ctx context.Context,
	diskID string,
	meta storage.SnapshotMeta,
) error {

	chunkMap := &protos.BackupChunkMap{
		ChunkIds: make([]string, meta.ChunkCount),
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, errs := t.storage.ReadChunkMap(readCtx, meta.ID, 0)
	for entry := range entries {
		if entry.ChunkIndex >= meta.ChunkCount {
			return errors.NewNonRetriableErrorf(
				"chunk index %v of snapshot %v is out of range %v",
				entry.ChunkIndex,
				meta.ID,
				meta.ChunkCount,
			)
		}

		chunkMap.ChunkIds[entry.ChunkIndex] = entry.ChunkID
	}

	err := <-errs
	if err != nil {
		return err
	}

	data, err := proto.Marshal(chunkMap)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	return t.s3.PutObject(
		ctx,
		t.bucket,
		ChunkMapKey(t.keyPrefix, diskID, meta.ID),
		persistence.S3Object{Data: data},
	)
}

func (t *backupSnapshotTask) saveProgress(
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

////////////////////////////////////////////////////////////////////////////////

func isOwnChunk(chunkID string, snapshotID string) bool {
	parts := strings.Split(chunkID, ".")
	return len(parts) == 3 && parts[1] == snapshotID
}
