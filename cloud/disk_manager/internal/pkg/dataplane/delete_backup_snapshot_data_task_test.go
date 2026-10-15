package dataplane

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func newDeleteBackupSnapshotDataTask(
	storage snapshot_storage.Storage,
	follower testFollower,
	snapshotID string,
) *deleteBackupSnapshotDataTask {

	return &deleteBackupSnapshotDataTask{
		storage:  storage,
		backupS3: follower.backupS3,
		request: &protos.DeleteBackupSnapshotDataRequest{
			SnapshotId: snapshotID,
		},
	}
}

func putFollowerObject(
	t *testing.T,
	ctx context.Context,
	follower testFollower,
	key string,
) {

	err := follower.backupS3.PutObject(
		ctx,
		key,
		follower.encryptedDEK,
		persistence.S3Object{Data: []byte("abc")},
	)
	require.NoError(t, err)
}

// Deletes the snapshot and then its row, as dataplane.DeleteSnapshot and
// dataplane.CollectSnapshots do.
func removeSnapshot(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	snapshotID string,
) {

	finishSnapshotDeletion(t, ctx, storage, snapshotID)

	keys, err := storage.GetSnapshotsToDelete(
		ctx,
		time.Now().Add(time.Hour),
		100,
	)
	require.NoError(t, err)

	var own []*storage_protos.DeletingSnapshotKey
	for _, key := range keys {
		if key.SnapshotId == snapshotID {
			own = append(own, key)
		}
	}
	require.NotEmpty(t, own)

	err = storage.ClearDeletingSnapshots(ctx, own)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestDeleteBackupSnapshotDataTaskWaitsUntilDeletion(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")
	putFollowerObject(t, ctx, follower, backup.ChunkMapKey("snap1"))

	task := newDeleteBackupSnapshotDataTask(storage, follower, "snap1")
	err := task.Run(ctx, newBackupExecutionContext(ctx))
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	// A deleting snapshot still has its row.
	finishSnapshotDeletion(t, ctx, storage, "snap1")
	err = task.Run(ctx, newBackupExecutionContext(ctx))
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.NoError(t, err)
}

func TestDeleteBackupSnapshotDataTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")
	putFollowerObject(t, ctx, follower, backup.ChunkKey(chunkID))
	putFollowerObject(t, ctx, follower, backup.ChunkMapKey("snap1"))
	removeSnapshot(t, ctx, storage, "snap1")

	task := newDeleteBackupSnapshotDataTask(storage, follower, "snap1")
	err := task.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)

	// Chunks are deleted by dataplane.DeleteBackupChunks.
	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
}

func TestDeleteBackupSnapshotDataTaskWithoutChunkMap(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")
	removeSnapshot(t, ctx, storage, "snap1")

	task := newDeleteBackupSnapshotDataTask(storage, follower, "snap1")
	err := task.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)

	// A repeated run succeeds too.
	err = task.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)
}
