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

// One task per snapshot: writes meta.json to the backup bucket, puts the
// snapshot's own chunks into backup_queue and, once the queue is drained by
// dataplane.BackupChunks, writes map.bin. The map is the last object, so its
// presence means the copy is complete.
type backupSnapshotTask struct {
	storage          storage.Storage
	s3               *persistence.S3Client
	bucket           string
	keyPrefix        string
	chunkSize        uint32
	chunkCompression string
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

	if !t.state.MetaWritten {
		err = t.writeMeta(ctx, diskID, meta)
		if err != nil {
			return err
		}

		t.state.MetaWritten = true
		t.state.ChunkCount = meta.ChunkCount

		err = t.saveProgress(ctx, execCtx)
		if err != nil {
			return err
		}
	}

	err = t.enqueueChunks(ctx, execCtx)
	if err != nil {
		return err
	}

	has, err := t.storage.HasBackupQueueEntries(ctx, snapshotID)
	if err != nil {
		return err
	}

	if has {
		// Chunks are still being copied, come back later.
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

// Written before the chunks: meta without map means the copy is in progress
// or was interrupted.
func (t *backupSnapshotTask) writeMeta(
	ctx context.Context,
	diskID string,
	meta storage.SnapshotMeta,
) error {

	snapshotMeta, err := NewSnapshotMeta(
		meta,
		t.request.FolderId,
		t.chunkSize,
		t.chunkCompression,
	)
	if err != nil {
		return err
	}

	data, err := snapshotMeta.Marshal()
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	return t.s3.PutObject(
		ctx,
		t.bucket,
		metaKey(t.keyPrefix, diskID, meta.ID),
		persistence.S3Object{Data: data},
	)
}

// Puts the snapshot's own chunks into backup_queue. Chunks inherited from the
// base snapshot are already there or in the queue on behalf of the base.
func (t *backupSnapshotTask) enqueueChunks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	if t.state.MilestoneChunkIndex >= t.state.ChunkCount {
		return nil
	}

	// Stops the chunk map reader if we return early.
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

// Full map: one entry per chunk index, empty id is a zero chunk.
func (t *backupSnapshotTask) writeMap(
	ctx context.Context,
	diskID string,
	meta storage.SnapshotMeta,
) error {

	chunkMap := &protos.BackupChunkMap{
		ChunkIds: make([]string, meta.ChunkCount),
	}

	// Stops the chunk map reader if we return early.
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
		chunkMapKey(t.keyPrefix, diskID, meta.ID),
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

// chunk_id = <task_id>.<snapshot_id>.<chunk_index>, see makeChunkID.
func isOwnChunk(chunkID string, snapshotID string) bool {
	parts := strings.Split(chunkID, ".")
	return len(parts) == 3 && parts[1] == snapshotID
}
