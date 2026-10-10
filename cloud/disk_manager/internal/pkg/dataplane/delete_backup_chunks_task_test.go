package dataplane

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newDeleteBackupChunksTask(
	storage snapshot_storage.Storage,
	follower testFollower,
) *deleteBackupChunksTask {

	return &deleteBackupChunksTask{
		storage:       storage,
		backupS3:      follower.backupS3,
		batchSize:     10,
		inflightLimit: 2,
		registry:      metrics.NewEmptyRegistry(),
	}
}

func runDeleteBackupChunksTask(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	follower testFollower,
) {

	task := newDeleteBackupChunksTask(storage, follower)
	err := task.Run(ctx, mocks.NewExecutionContextMock())
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	length, err := storage.GetBackupChunkDeleteQueueLength(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, length)
}

func enqueueBackupChunk(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	follower testFollower,
	snapshotID string,
	chunkID string,
) {

	err := enqueueBackupChunks(
		ctx,
		storage,
		[]snapshot_storage.BackupChunkQueueEntry{
			{
				SnapshotID:   snapshotID,
				ChunkID:      chunkID,
				StoredInS3:   true,
				EncryptedDEK: follower.encryptedDEK,
			},
		},
	)
	require.NoError(t, err)
}

// Enqueues the chunk of the snapshot and copies it to the follower.
func backUpChunk(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	follower testFollower,
	snapshotID string,
	chunkID string,
) {

	enqueueBackupChunk(t, ctx, storage, follower, snapshotID, chunkID)

	task := newBackupChunksTask(storage, follower)
	err := task.Run(ctx, mocks.NewExecutionContextMock())
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteBackupChunksTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")
	backUpChunk(t, ctx, storage, follower, "snap1", chunkID)

	// The chunk is alive: nothing to delete.
	runDeleteBackupChunksTask(t, ctx, storage, follower)
	_, err := follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	finishSnapshotDeletion(t, ctx, storage, "snap1")

	length, err := storage.GetBackupChunkDeleteQueueLength(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, length)

	runDeleteBackupChunksTask(t, ctx, storage, follower)
	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.Error(t, err)
}

func TestDeleteBackupChunksTaskKeepsSharedChunk(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "src")
	backUpChunk(t, ctx, storage, follower, "src", chunkID)

	// dst is not backed up, but it references the chunk.
	_, err := storage.CreateSnapshot(
		ctx,
		snapshot_storage.SnapshotMeta{ID: "dst"},
	)
	require.NoError(t, err)
	err = storage.ShallowCopySnapshot(ctx, "src", "dst", 0, nil)
	require.NoError(t, err)

	finishSnapshotDeletion(t, ctx, storage, "src")
	runDeleteBackupChunksTask(t, ctx, storage, follower)
	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	finishSnapshotDeletion(t, ctx, storage, "dst")
	runDeleteBackupChunksTask(t, ctx, storage, follower)
	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.Error(t, err)
}

// A copy cancelled while a worker writes the chunk: the object lands in the
// follower, but the chunk is never marked as copied.
func TestDeleteBackupChunksTaskDeletesChunkOfCancelledCopy(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")
	enqueueBackupChunk(t, ctx, storage, follower, "snap1", chunkID)

	_, err := storage.ClearBackupChunks(ctx, "snap1", 10)
	require.NoError(t, err)
	putFollowerObject(t, ctx, follower, backup.ChunkKey(chunkID))

	finishSnapshotDeletion(t, ctx, storage, "snap1")
	runDeleteBackupChunksTask(t, ctx, storage, follower)
	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.Error(t, err)
}

func TestDeleteBackupChunksTaskIgnoresChunksNotInFollower(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	createSnapshotWithChunk(t, ctx, storage, "snap1")
	finishSnapshotDeletion(t, ctx, storage, "snap1")

	length, err := storage.GetBackupChunkDeleteQueueLength(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, length)
}
