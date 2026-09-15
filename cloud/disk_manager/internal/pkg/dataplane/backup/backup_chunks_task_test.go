package backup

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/protos"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestBackupChunksTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	backup := newTestBackup(t, ctx)

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
		// Chunk of a snapshot that has already been deleted.
		{SnapshotID: "snap1", ChunkID: "task.snap1.1"},
	}
	err = storage.EnqueueBackupChunks(ctx, entries)
	require.NoError(t, err)

	task := &backupChunksTask{
		storage:      storage,
		dstS3:        backup.s3,
		dstBucket:    backup.bucket,
		dstKeyPrefix: backup.keyPrefix,
		batchSize:    10,
		workerCount:  2,
		registry:     metrics.NewEmptyRegistry(),
		state:        &protos.BackupChunksTaskState{},
	}

	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	src, err := storage.ReadChunkBlob(ctx, chunkID)
	require.NoError(t, err)

	dst, err := backup.getObject(ctx, "chunks/"+chunkID)
	require.NoError(t, err)
	require.Equal(t, src.Data, dst.Data)
	require.NotNil(t, dst.Metadata["Checksum"])
	require.Equal(t, *src.Metadata["Checksum"], *dst.Metadata["Checksum"])

	queue, err := storage.GetBackupQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, queue)
}
