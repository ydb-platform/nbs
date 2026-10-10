package dataplane

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newBackupChunksTask(
	storage snapshot_storage.Storage,
	follower testFollower,
) *backupChunksTask {

	return &backupChunksTask{
		storage:   storage,
		backupS3:  follower.backupS3,
		batchSize: 10,
		ioDepth:   2,
		registry:  metrics.NewEmptyRegistry(),
	}
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

	queue, err := storage.GetQueuedChunksToBackup(ctx, 0, 10)
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

	queue, err := storage.GetQueuedChunksToBackup(ctx, 0, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskReturnsAfterMaxChunks(t *testing.T) {
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
	task.maxChunks = 1
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)

	queue, err := storage.GetQueuedChunksToBackup(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, queue, 1)

	copiedChunkID := chunk0
	if queue[0].ChunkID == chunk0 {
		copiedChunkID = chunk1
	}

	_, err = follower.getObject(ctx, backup.ChunkKey(copiedChunkID))
	require.NoError(t, err)
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

	queue, err := storage.GetQueuedChunksToBackup(ctx, 0, 10)
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

func queueEntry(
	snapshotID string,
	chunkID string,
) snapshot_storage.BackupChunkQueueEntry {

	return snapshot_storage.BackupChunkQueueEntry{
		SnapshotID: snapshotID,
		ChunkID:    chunkID,
	}
}

func TestTakenBackupChunksSkipsTakenEntries(t *testing.T) {
	entry := queueEntry
	taken := newTakenBackupChunks()

	fresh := taken.take([]snapshot_storage.BackupChunkQueueEntry{
		entry("snap1", "c1"),
		entry("snap1", "c2"),
	})
	require.Len(t, fresh, 2)
	require.True(t, taken.hasPending())

	// A prefetched batch returns c2 again and a new c3; the same chunk of
	// another snapshot is a separate entry.
	fresh = taken.take([]snapshot_storage.BackupChunkQueueEntry{
		entry("snap1", "c2"),
		entry("snap1", "c3"),
		entry("snap2", "c2"),
	})
	require.Equal(t, []snapshot_storage.BackupChunkQueueEntry{
		entry("snap1", "c3"),
		entry("snap2", "c2"),
	}, fresh)

	taken.completed([]snapshot_storage.BackupChunkQueueEntry{
		entry("snap1", "c1"),
		entry("snap1", "c2"),
		entry("snap2", "c2"),
	})

	copyErr := errors.New("copy failed")
	taken.failed(entry("snap1", "c3"), copyErr)
	require.False(t, taken.hasPending())
	require.Equal(t, copyErr, taken.firstError())

	// A completed entry can be taken again, a failed one cannot.
	fresh = taken.take([]snapshot_storage.BackupChunkQueueEntry{
		entry("snap1", "c1"),
		entry("snap1", "c3"),
	})
	require.Equal(t, []snapshot_storage.BackupChunkQueueEntry{
		entry("snap1", "c1"),
	}, fresh)
}
