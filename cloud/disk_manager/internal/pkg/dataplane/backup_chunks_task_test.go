package dataplane

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/contrib/go/cityhash"
)

////////////////////////////////////////////////////////////////////////////////

func newBackupChunksTask(
	storage snapshot_storage.Storage,
	follower testFollower,
) *backupChunksTask {

	return &backupChunksTask{
		storage:  storage,
		backupS3: follower.backupS3,
		registry: metrics.NewEmptyRegistry(),
		request: &protos.BackupChunksRequest{
			FirstShardId: 0,
			LastShardId:  math.MaxUint64,
		},
		batchSize: 10,
		ioDepth:   2,
	}
}

////////////////////////////////////////////////////////////////////////////////

type chunkReadCountingStorage struct {
	snapshot_storage.Storage
	reads atomic.Int64
}

func (s *chunkReadCountingStorage) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
	storedInS3 bool,
) (chunks.ChunkBlob, error) {

	s.reads.Add(1)
	return s.Storage.ReadChunkBlob(ctx, chunkID, storedInS3)
}

type unavailableChunkStorage struct {
	snapshot_storage.Storage
	reads atomic.Int64
}

func (s *unavailableChunkStorage) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
	storedInS3 bool,
) (chunks.ChunkBlob, error) {

	s.reads.Add(1)
	return chunks.ChunkBlob{}, errors.NewRetriableErrorf("storage is unavailable")
}

// Fails reads of one chunk with a retriable error, as the chunk storage does
// on an object it answers 500 for.
type retriableUnreadableChunkStorage struct {
	snapshot_storage.Storage
	chunkID string
	reads   atomic.Int64
}

func (s *retriableUnreadableChunkStorage) ReadChunkBlob(
	ctx context.Context,
	chunkID string,
	storedInS3 bool,
) (chunks.ChunkBlob, error) {

	if chunkID == s.chunkID {
		s.reads.Add(1)
		return chunks.ChunkBlob{}, errors.NewRetriableErrorf("internal error")
	}

	return s.Storage.ReadChunkBlob(ctx, chunkID, storedInS3)
}

// Fails reads of the queue with a retriable error, as YDB does while it is
// unavailable.
type unavailableQueueStorage struct {
	snapshot_storage.Storage
}

func (s *unavailableQueueStorage) GetQueuedChunksToBackup(
	ctx context.Context,
	firstShardID uint64,
	lastShardID uint64,
	after *snapshot_storage.BackupChunkQueueEntry,
	limit int,
) ([]snapshot_storage.BackupChunkQueueEntry, error) {

	return nil, errors.NewRetriableErrorf("database is unavailable")
}

////////////////////////////////////////////////////////////////////////////////

func TestBackupChunksTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")

	entries := []snapshot_storage.BackupChunkQueueEntry{
		{
			SnapshotID:   "snap1",
			ChunkID:      chunkID,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	chunkBlob, err := storage.ReadChunkBlob(
		ctx,
		chunkID,
		true, // storedInS3
	)
	require.NoError(t, err)

	object, err := follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	require.Equal(t, chunkBlob.Data, object.Data)
	require.Equal(
		t,
		*chunks.NewS3Object(chunkBlob).Metadata["Checksum"],
		*object.Metadata["Checksum"],
	)

	raw, err := follower.getRawObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	require.NotEqual(t, chunkBlob.Data, raw.Data)
	require.Equal(t, "kek1", *raw.Metadata["Key-Id"])
	require.Equal(t, *object.Metadata["Checksum"], *raw.Metadata["Checksum"])

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskCopiesSeveralBatches(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunk0 := createSnapshotWithChunk(t, ctx, storage, "snap1")
	chunk1 := createSnapshotWithChunk(t, ctx, storage, "snap2")

	entries := []snapshot_storage.BackupChunkQueueEntry{
		{
			SnapshotID:   "snap1",
			ChunkID:      chunk0,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		},
		{
			SnapshotID:   "snap2",
			ChunkID:      chunk1,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	task.batchSize = 1
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	for _, chunkID := range []string{chunk0, chunk1} {
		_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
		require.NoError(t, err)
	}

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskCopiesOnlyItsRange(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunk0 := createSnapshotWithChunk(t, ctx, storage, "snap1")
	chunk1 := createSnapshotWithChunk(t, ctx, storage, "snap2")

	entries := []snapshot_storage.BackupChunkQueueEntry{
		{
			SnapshotID:   "snap1",
			ChunkID:      chunk0,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		},
		{
			SnapshotID:   "snap2",
			ChunkID:      chunk1,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	// The range holds only the shard of chunk0, as backup_chunk_queue keys
	// rows by cityhash64 of the chunk ID.
	shardID := cityhash.Hash64([]byte(chunk0))
	task := newBackupChunksTask(storage, follower)
	task.request.FirstShardId = shardID
	task.request.LastShardId = shardID
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	_, err = follower.getObject(ctx, backup.ChunkKey(chunk0))
	require.NoError(t, err)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Equal(t, entries[1:], queue)
}

func TestBackupChunksTaskLeavesUnreadableChunksInQueue(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	var entries []snapshot_storage.BackupChunkQueueEntry
	for _, chunkID := range []string{"task.snap1.0", "task.snap1.1"} {
		entries = append(entries, snapshot_storage.BackupChunkQueueEntry{
			SnapshotID:   "snap1",
			ChunkID:      chunkID,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		})
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	unavailableStorage := &unavailableChunkStorage{Storage: storage}
	task := newBackupChunksTask(unavailableStorage, follower)
	task.ioDepth = 1
	execCtx := mocks.NewExecutionContextMock()

	// No chunk can be read: each is tried once and stays in the queue for the
	// next task of the range.
	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 2, unavailableStorage.reads.Load())

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.ElementsMatch(t, entries, queue)
}

func TestBackupChunksTaskStopsOnRetriableError(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	task := newBackupChunksTask(
		&unavailableQueueStorage{Storage: storage},
		follower,
	)
	execCtx := mocks.NewExecutionContextMock()

	// The queue cannot be read: the task stops and the framework runs it
	// again later. The task lives for hours, so the retry is not counted
	// against the limit.
	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyRetriableError()))
	var retriable *errors.RetriableError
	require.True(t, errors.As(err, &retriable))
	require.True(t, retriable.IgnoreRetryLimit)
}

func TestBackupChunksTaskLeavesUnreadableChunkInQueue(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap2")

	unreadable := snapshot_storage.BackupChunkQueueEntry{
		SnapshotID:   "snap1",
		ChunkID:      "task.snap1.0",
		StoredInS3:   true,
		EncryptedDEK: follower.encryptedDEK,
	}
	entries := []snapshot_storage.BackupChunkQueueEntry{
		unreadable,
		{
			SnapshotID:   "snap2",
			ChunkID:      chunkID,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	unreadableStorage := &retriableUnreadableChunkStorage{
		Storage: storage,
		chunkID: unreadable.ChunkID,
	}
	task := newBackupChunksTask(unreadableStorage, follower)
	execCtx := mocks.NewExecutionContextMock()

	// The unreadable chunk does not stop the range: the other chunk is
	// copied and the task ends.
	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, unreadableStorage.reads.Load())

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	// The unreadable chunk stays queued and its backup waits, it does not
	// fail.
	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{unreadable},
		queue,
	)

	err = storage.CheckBackupChunksCompleted(ctx, "snap1", "")
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.NoError(t, storage.CheckBackupChunksCompleted(ctx, "snap2", ""))

	// The next task of the range reads it again.
	task = newBackupChunksTask(unreadableStorage, follower)
	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 2, unreadableStorage.reads.Load())
}

func TestBackupChunksTaskCopiesSharedChunkOnce(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")

	// Two snapshots queued the same chunk before either copied it.
	var entries []snapshot_storage.BackupChunkQueueEntry
	for _, snapshotID := range []string{"snap1", "snap2"} {
		entries = append(entries, snapshot_storage.BackupChunkQueueEntry{
			SnapshotID:   snapshotID,
			ChunkID:      chunkID,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		})
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	countingStorage := &chunkReadCountingStorage{Storage: storage}
	task := newBackupChunksTask(countingStorage, follower)
	// One entry per batch: the two entries of the chunk come in different
	// batches and are still copied once.
	task.batchSize = 1
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.EqualValues(t, 1, countingStorage.reads.Load())

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskMovesMissingChunkOutOfQueue(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap2")

	missing := snapshot_storage.BackupChunkQueueEntry{
		SnapshotID:   "snap1",
		ChunkID:      "task.snap1.0",
		StoredInS3:   true,
		EncryptedDEK: follower.encryptedDEK,
	}
	entries := []snapshot_storage.BackupChunkQueueEntry{
		missing,
		{
			SnapshotID:   "snap2",
			ChunkID:      chunkID,
			StoredInS3:   true,
			EncryptedDEK: follower.encryptedDEK,
		},
	}
	err := enqueueBackupChunks(ctx, storage, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()

	// The missing chunk does not hold up the rest of the range: it leaves the
	// queue as failed and the task ends.
	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Empty(t, queue)

	// The backup of snap1 fails, the one of snap2 is complete.
	err = storage.CheckBackupChunksCompleted(ctx, "snap1", "")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	require.NoError(t, storage.CheckBackupChunksCompleted(ctx, "snap2", ""))
}

func TestBackupChunksTaskEndsOnEmptyQueue(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()

	require.NoError(t, task.Run(ctx, execCtx))
}

func TestGroupByChunk(t *testing.T) {
	entry := func(
		snapshotID string,
		chunkID string,
	) snapshot_storage.BackupChunkQueueEntry {

		return snapshot_storage.BackupChunkQueueEntry{
			SnapshotID: snapshotID,
			ChunkID:    chunkID,
		}
	}

	require.Empty(t, groupByChunk(nil))

	groups := groupByChunk([]snapshot_storage.BackupChunkQueueEntry{
		entry("A", "c1"),
		entry("B", "c1"),
		entry("A", "c2"),
		entry("A", "c3"),
		entry("B", "c3"),
	})
	require.Equal(
		t,
		[][]snapshot_storage.BackupChunkQueueEntry{
			{entry("A", "c1"), entry("B", "c1")},
			{entry("A", "c2")},
			{entry("A", "c3"), entry("B", "c3")},
		},
		groups,
	)

	// A group grown by the next batch does not overwrite the next group.
	first := append(groups[0], entry("C", "c1"))
	require.Len(t, first, 3)
	require.Equal(t, entry("A", "c2"), groups[1][0])
}
