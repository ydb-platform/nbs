package storage

import (
	"context"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBackupBlobDispatchAndOwnership(t *testing.T) {
	for _, c := range testCases() {
		t.Run(c.name, func(t *testing.T) {
			f := createFixture(t)
			defer f.teardown()
			id, err := f.storage.WriteChunk(f.ctx, "writer", "snap.with.dots", makeChunk(0, "independent payload"), c.useS3)
			require.NoError(t, err)
			require.True(t, IsChunkCreatedBySnapshot(id, "snap.with.dots"))
			require.False(t, IsChunkCreatedBySnapshot(id, "other"))
			require.False(t, IsChunkCreatedBySnapshot("", "snap"))
			require.False(t, IsChunkCreatedBySnapshot("malformed", "snap"))
			require.False(t, IsChunkCreatedBySnapshot("only.one", "snap"))
			blob, err := f.storage.ReadChunkBlob(f.ctx, id, c.useS3)
			require.NoError(t, err)
			require.Equal(t, []byte("independent payload"), blob.Data)
			require.Equal(t, crc32.ChecksumIEEE(blob.Data), blob.Checksum)
			_, err = f.storage.ReadChunkBlob(f.ctx, "missing", c.useS3)
			require.Error(t, err)
		})
	}
}
func TestBackupQueueStorageLossNeverAcknowledgesProgress(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()
	entries := []BackupChunkQueueEntry{{SnapshotID: "snap", ChunkID: "writer.snap.0", EncryptedDEK: []byte("opaque-dek")}}
	require.NoError(t, f.storage.EnqueueBackupChunks(f.ctx, "snap", entries))
	require.NoError(t, f.db.DropTable(f.ctx, f.config.GetStorageFolder(), "backup_chunks"))
	require.Error(t, f.storage.EnqueueBackupChunks(f.ctx, "snap", entries))
	_, err := f.storage.GetBackedUpChunkCount(f.ctx, "snap")
	require.Error(t, err)
	require.Error(t, f.storage.ChunksBackupCompleted(f.ctx, entries))
	_, err = f.storage.ClearCompletedBackupChunks(f.ctx, "snap", 1)
	require.Error(t, err)
	queued, err := f.storage.GetQueuedChunksToBackup(f.ctx, 10)
	require.NoError(t, err)
	require.Equal(t, entries, queued, "lost durable completion must preserve pending queue")
	require.NoError(t, f.db.DropTable(f.ctx, f.config.GetStorageFolder(), "backup_chunk_queue"))
	_, err = f.storage.GetQueuedChunksToBackup(f.ctx, 10)
	require.Error(t, err)
	_, err = f.storage.GetBackupChunkQueueLength(f.ctx)
	require.Error(t, err)
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	require.Error(t, f.storage.EnqueueBackupChunks(cancelled, "snap", entries))
	require.Error(t, f.storage.ChunksBackupCompleted(cancelled, entries))
}
