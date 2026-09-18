package dataplane

import (
	"testing"

	"github.com/stretchr/testify/require"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
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
	dst testBackup,
) *backupChunksTask {

	return &backupChunksTask{
		storage:     storage,
		s3:          dst.s3,
		bucket:      dst.bucket,
		keyPrefix:   dst.keyPrefix,
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

	dst := newTestBackup(t, ctx)

	chunkID, err := storage.WriteChunk(
		ctx,
		"task",
		"snap1",
		dataplane_common.Chunk{Index: 0, Data: []byte("abc")},
		true, // useS3
	)
	require.NoError(t, err)

	entries := []snapshot_storage.BackupQueueEntry{
		{SnapshotID: "snap1", ChunkID: chunkID},
	}
	err = storage.EnqueueBackupChunks(ctx, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, dst)
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	src, err := storage.ReadChunkBlob(ctx, chunkID)
	require.NoError(t, err)

	// Chunk is copied as is, together with its metadata.
	object, err := dst.getObject(ctx, "chunks/"+chunkID)
	require.NoError(t, err)
	require.Equal(t, src.Data, object.Data)
	require.Equal(t, *src.Metadata["Checksum"], *object.Metadata["Checksum"])

	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}

func TestBackupChunksTaskFailsOnMissingChunk(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	dst := newTestBackup(t, ctx)

	entries := []snapshot_storage.BackupQueueEntry{
		{SnapshotID: "snap1", ChunkID: "task.snap1.0"},
	}
	err := storage.EnqueueBackupChunks(ctx, entries)
	require.NoError(t, err)

	task := newBackupChunksTask(storage, dst)
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.NewInterruptExecutionError()))

	// The entry stays in the queue, the chunk is not lost silently.
	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, entries, queue)
}
