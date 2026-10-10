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

func TestBackupChunksTaskStopsWhenChunksFailInARow(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)

	// None of these chunks exists in the chunk storage.
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

	countingStorage := &chunkReadCountingStorage{Storage: storage}
	task := newBackupChunksTask(countingStorage, follower)
	task.batchSize = 1
	task.ioDepth = 1
	execCtx := mocks.NewExecutionContextMock()

	// The first failure is a batch worth of failures in a row: the task
	// stops instead of trying the next chunk.
	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.EqualValues(t, 1, countingStorage.reads.Load())

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.ElementsMatch(t, entries, queue)
}

func TestBackupChunksTaskGoesOnPastMissingChunk(t *testing.T) {
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

	err = task.Run(ctx, execCtx)
	require.Error(t, err)

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	queue, err := queuedBackupChunks(ctx, storage)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{missing},
		queue,
	)
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
