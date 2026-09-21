package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type backupSnapshotChunksTask struct {
	storage    storage.Storage
	followerS3 *backup.FollowerS3
	batchSize  int
	request    *protos.BackupSnapshotChunksRequest
	state      *protos.BackupSnapshotChunksTaskState
}

func (t *backupSnapshotChunksTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupSnapshotChunksTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotChunksRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupSnapshotChunksTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotChunksTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	meta, err := t.storage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return err
	}

	t.state.ChunkCount = meta.ChunkCount

	err = t.enqueueChunks(ctx, execCtx)
	if err != nil {
		return err
	}

	err = t.waitForChunks(ctx, snapshotID)
	if err != nil {
		return err
	}

	return t.writeChunkMap(ctx, meta)
}

func (t *backupSnapshotChunksTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// TODO(https://github.com/ydb-platform/nbs/issues/7237):
	// lock the snapshot while its chunks are copied and remove them from
	// backup_chunk_queue on cancellation.
	return nil
}

func (t *backupSnapshotChunksTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupSnapshotChunksTask) GetResponse() proto.Message {
	return &empty.Empty{}
}

////////////////////////////////////////////////////////////////////////////////

func validateChunkMapEntry(
	entry storage.ChunkMapEntry,
	snapshotID string,
	chunkCount uint32,
) error {

	if entry.ChunkIndex >= chunkCount {
		return errors.NewNonRetriableErrorf(
			"chunk index %v of snapshot %v is out of range %v",
			entry.ChunkIndex,
			snapshotID,
			chunkCount,
		)
	}

	if len(entry.ChunkID) != 0 && !entry.StoredInS3 {
		return errors.NewNonRetriableErrorf(
			"chunk %v of snapshot %v is stored in ydb, only s3 chunks are backed up",
			entry.ChunkID,
			snapshotID,
		)
	}

	return nil
}

func (t *backupSnapshotChunksTask) enqueueBatch(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	batch []storage.BackupChunkQueueEntry,
	milestoneChunkIndex uint32,
) error {

	err := t.storage.EnqueueBackupChunks(ctx, batch)
	if err != nil {
		return err
	}

	t.state.EnqueuedChunkCount += uint32(len(batch))
	t.state.MilestoneChunkIndex = milestoneChunkIndex
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotChunksTask) enqueueChunks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	if t.state.MilestoneChunkIndex >= t.state.ChunkCount {
		return nil
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := t.storage.ReadChunkMap(
		readCtx,
		snapshotID,
		t.state.MilestoneChunkIndex,
		false, // includeShallowCopied
	)

	var batch []storage.BackupChunkQueueEntry
	var milestoneChunkIndex uint32

	for entry := range entries {
		milestoneChunkIndex = entry.ChunkIndex + 1

		err := validateChunkMapEntry(entry, snapshotID, t.state.ChunkCount)
		if err != nil {
			return err
		}

		batch = append(batch, storage.BackupChunkQueueEntry{
			SnapshotID: snapshotID,
			ChunkID:    entry.ChunkID,
		})

		if len(batch) >= t.batchSize {
			err = t.enqueueBatch(ctx, execCtx, batch, milestoneChunkIndex)
			if err != nil {
				return err
			}

			batch = nil
		}
	}

	err := <-entriesErrors
	if err != nil {
		return err
	}

	return t.enqueueBatch(ctx, execCtx, batch, t.state.ChunkCount)
}

func (t *backupSnapshotChunksTask) waitForChunks(
	ctx context.Context,
	snapshotID string,
) error {

	hasEntries, err := t.storage.HasBackupChunkQueueEntries(ctx, snapshotID)
	if err != nil {
		return err
	}

	if hasEntries {
		logging.Debug(
			ctx,
			"Backup of snapshot with id %v is waiting for its chunks",
			snapshotID,
		)
		return errors.NewInterruptExecutionError()
	}

	return nil
}

func (t *backupSnapshotChunksTask) writeChunkMap(
	ctx context.Context,
	meta storage.SnapshotMeta,
) error {

	chunkMap := &protos.BackupChunkMap{
		ChunkIds: make([]string, meta.ChunkCount),
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := t.storage.ReadChunkMap(
		readCtx,
		meta.ID,
		0,    // milestoneChunkIndex
		true, // includeShallowCopied
	)
	for entry := range entries {
		err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount)
		if err != nil {
			return err
		}

		chunkMap.ChunkIds[entry.ChunkIndex] = entry.ChunkID
	}

	err := <-entriesErrors
	if err != nil {
		return err
	}

	data, err := proto.Marshal(chunkMap)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	return t.followerS3.PutObject(
		ctx,
		backup.ChunkMapKey(meta.ID),
		persistence.S3Object{Data: data},
	)
}
