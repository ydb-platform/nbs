package dataplane

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func newBackupChunksTask(
	storage snapshot_storage.Storage,
	follower testFollower,
) *backupChunksTask {

	return &backupChunksTask{
		storage:     storage,
		followerS3:  follower.followerS3,
		batchSize:   10,
		workerCount: 2,
		registry:    metrics.NewEmptyRegistry(),
		state:       &protos.BackupChunksTaskState{},
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
		{SnapshotID: "snap1", ChunkID: chunkID},
	}
	err := storage.EnqueueBackupChunks(ctx, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	src, err := storage.ReadChunkBlob(ctx, chunkID)
	require.NoError(t, err)

	object, err := follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	require.Equal(t, src.Data, object.Data)
	require.Equal(t, *src.Metadata["Checksum"], *object.Metadata["Checksum"])

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
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
		{SnapshotID: "snap1", ChunkID: chunk0},
		{SnapshotID: "snap2", ChunkID: chunk1},
	}
	err := storage.EnqueueBackupChunks(ctx, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	task.batchSize = 1
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	for _, chunkID := range []string{chunk0, chunk1} {
		_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
		require.NoError(t, err)
	}

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskGoesOnPastMissingChunk(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap2")

	missing := snapshot_storage.BackupChunkQueueEntry{
		SnapshotID: "snap1",
		ChunkID:    "task.snap1.0",
	}
	entries := []snapshot_storage.BackupChunkQueueEntry{
		missing,
		{SnapshotID: "snap2", ChunkID: chunkID},
	}
	err := storage.EnqueueBackupChunks(ctx, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, follower)
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)

	queue, err := storage.GetBackupChunkQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(
		t,
		[]snapshot_storage.BackupChunkQueueEntry{missing},
		queue,
	)
}
