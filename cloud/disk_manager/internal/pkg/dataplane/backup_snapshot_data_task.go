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

// Holds a reference to the snapshot for the whole copy, so the snapshot and
// the chunks it uses stay until the copy ends.
type backupSnapshotDataTask struct {
	storage   storage.Storage
	backupS3  *backup.S3
	batchSize int
	request   *protos.BackupSnapshotDataRequest
	state     *protos.BackupSnapshotDataTaskState
}

func (t *backupSnapshotDataTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *backupSnapshotDataTask) Load(request, state []byte) error {
	t.request = &protos.BackupSnapshotDataRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.BackupSnapshotDataTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *backupSnapshotDataTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.SnapshotId

	// Deletion waits until the copy releases the snapshot. A repeated hold by
	// the same copy succeeds.
	held, err := t.storage.HoldSnapshotForBackup(
		ctx,
		snapshotID,
		execCtx.GetTaskID(),
	)
	if err != nil {
		return err
	}

	// Deletion started first: there is nothing to copy.
	if !held {
		return nil
	}

	// A DEK that cannot be opened fails the chunk copier on every
	// chunk and leaves those queue rows unfinished, so this task
	// interrupts forever. Reject it before anything is enqueued.
	err = t.backupS3.CheckEncryptedDEK(t.request.EncryptedDek)
	if err != nil {
		return err
	}

	meta, err := t.storage.CheckSnapshotReady(ctx, snapshotID)
	if err != nil {
		return err
	}

	t.state.ChunkCount = meta.ChunkCount

	err = t.enqueueChunks(ctx, execCtx)
	if err != nil {
		return err
	}

	err = t.backupChunkMap(ctx, execCtx, meta)
	if err != nil {
		return err
	}

	return t.finish(ctx, execCtx)
}

func (t *backupSnapshotDataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.finish(ctx, execCtx)
}

func (t *backupSnapshotDataTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *backupSnapshotDataTask) GetResponse() proto.Message {
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

	return nil
}

func (t *backupSnapshotDataTask) enqueueBatch(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	batch []storage.BackupChunkQueueEntry,
	milestoneChunkIndex uint32,
) error {

	if len(batch) != 0 {
		err := t.storage.EnqueueBackupChunks(
			ctx,
			t.request.SnapshotId,
			batch,
		)
		if err != nil {
			return err
		}
	}

	t.state.MilestoneChunkIndex = milestoneChunkIndex
	return execCtx.SaveState(ctx)
}

func (t *backupSnapshotDataTask) enqueueChunks(
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
	)

	var batch []storage.BackupChunkQueueEntry
	var milestoneChunkIndex uint32

	for entry := range entries {
		milestoneChunkIndex = entry.ChunkIndex + 1

		err := validateChunkMapEntry(entry, snapshotID, t.state.ChunkCount)
		if err != nil {
			return err
		}

		// Zero chunks have no data. Chunks already in the follower, including
		// those of other snapshots, are skipped on enqueue.
		if len(entry.ChunkID) == 0 {
			continue
		}

		batch = append(batch, storage.BackupChunkQueueEntry{
			SnapshotID:   snapshotID,
			ChunkID:      entry.ChunkID,
			StoredInS3:   entry.StoredInS3,
			EncryptedDEK: t.request.EncryptedDek,
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

func (t *backupSnapshotDataTask) waitForChunksBackupCompleted(
	ctx context.Context,
	snapshotID string,
) error {

	err := t.storage.CheckBackupChunksCompleted(ctx, snapshotID)
	if errors.Is(err, errors.NewInterruptExecutionError()) {
		logging.Debug(
			ctx,
			"Backup of snapshot with id %v is waiting for its chunks to finish backing up",
			snapshotID,
		)
	}

	return err
}

// Clears the chunk entries of the snapshot and drops the reference. A copy
// ends this way both on success and on cancellation; a cancelled copy is
// enqueued again by its next attempt.
func (t *backupSnapshotDataTask) finish(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.clearChunks(ctx)
	if err != nil {
		return err
	}

	return t.storage.ReleaseSnapshotForBackup(
		ctx,
		t.request.SnapshotId,
		execCtx.GetTaskID(),
	)
}

func (t *backupSnapshotDataTask) clearChunks(ctx context.Context) error {
	for {
		cleared, err := t.storage.ClearBackupChunks(
			ctx,
			t.request.SnapshotId,
			t.batchSize,
		)
		if err != nil {
			return err
		}

		// The storage clears fewer than batchSize entries only when no
		// entries are left.
		if cleared < t.batchSize {
			return nil
		}
	}
}

// The full map is published only after every queued chunk has completed.
func (t *backupSnapshotDataTask) backupChunkMap(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	meta storage.SnapshotMeta,
) error {

	if t.state.ChunkMapBackedUp {
		return nil
	}

	err := t.waitForChunksBackupCompleted(ctx, meta.ID)
	if err != nil {
		return err
	}

	chunkMap := &protos.BackupChunkMap{
		ChunkIds: make([]string, meta.ChunkCount),
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := t.storage.ReadChunkMap(
		readCtx,
		meta.ID,
		0, // milestoneChunkIndex
	)
	for entry := range entries {
		err := validateChunkMapEntry(entry, meta.ID, meta.ChunkCount)
		if err != nil {
			return err
		}

		chunkMap.ChunkIds[entry.ChunkIndex] = entry.ChunkID
	}

	err = <-entriesErrors
	if err != nil {
		return err
	}

	data, err := proto.Marshal(chunkMap)
	if err != nil {
		return errors.NewNonRetriableError(err)
	}

	err = t.backupS3.PutObject(
		ctx,
		backup.ChunkMapKey(meta.ID),
		t.request.EncryptedDek,
		persistence.S3Object{Data: data},
	)
	if err != nil {
		return err
	}

	t.state.ChunkMapBackedUp = true
	return execCtx.SaveState(ctx)
}
