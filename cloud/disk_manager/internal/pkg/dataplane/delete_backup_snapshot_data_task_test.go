package dataplane

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
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
		storage:   storage,
		backupS3:  follower.backupS3,
		batchSize: 10,
		request: &protos.DeleteBackupSnapshotDataRequest{
			SnapshotId: snapshotID,
		},
		state: &protos.DeleteBackupSnapshotDataTaskState{},
	}
}

func putFollowerChunkAndChunkMap(
	t *testing.T,
	ctx context.Context,
	follower testFollower,
	snapshotID string,
	chunkID string,
) {

	err := follower.backupS3.PutObject(
		ctx,
		backup.ChunkKey(chunkID),
		persistence.S3Object{Data: []byte("abc")},
	)
	require.NoError(t, err)

	data, err := proto.Marshal(&protos.BackupChunkMap{
		ChunkIds: []string{chunkID},
	})
	require.NoError(t, err)
	err = follower.backupS3.PutObject(
		ctx,
		backup.ChunkMapKey(snapshotID),
		persistence.S3Object{Data: data},
	)
	require.NoError(t, err)
}

func finishSnapshotDeletion(
	t *testing.T,
	ctx context.Context,
	storage snapshot_storage.Storage,
	snapshotID string,
) {

	_, err := storage.DeletingSnapshot(ctx, snapshotID, "delete")
	require.NoError(t, err)
	err = storage.DeleteSnapshotData(ctx, snapshotID)
	require.NoError(t, err)

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
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")
	putFollowerChunkAndChunkMap(t, ctx, follower, "snap1", chunkID)

	task := newDeleteBackupSnapshotDataTask(storage, follower, "snap1")
	err := task.Run(ctx, newBackupExecutionContext(ctx))
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.NoError(t, err)
}

func TestDeleteBackupSnapshotDataTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")
	putFollowerChunkAndChunkMap(t, ctx, follower, "snap1", chunkID)
	finishSnapshotDeletion(t, ctx, storage, "snap1")

	task := newDeleteBackupSnapshotDataTask(storage, follower, "snap1")
	err := task.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.Error(t, err)
	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)
}

func TestDeleteBackupSnapshotDataTaskKeepsSharedChunk(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "src")
	putFollowerChunkAndChunkMap(t, ctx, follower, "src", chunkID)

	err := storage.ShallowCopySnapshot(ctx, "src", "dst", 0, nil)
	require.NoError(t, err)
	finishSnapshotDeletion(t, ctx, storage, "src")

	task := newDeleteBackupSnapshotDataTask(storage, follower, "src")
	err = task.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)

	_, err = follower.getObject(ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	_, err = follower.getObject(ctx, backup.ChunkMapKey("src"))
	require.Error(t, err)
}

func TestDeleteBackupSnapshotDataTaskWithoutChunkMap(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")
	finishSnapshotDeletion(t, ctx, storage, "snap1")

	task := newDeleteBackupSnapshotDataTask(storage, follower, "snap1")
	err := task.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)
}

func TestDeleteBackupSnapshotDataTaskDeletesMissingChunk(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")

	chunkMap := &protos.BackupChunkMap{ChunkIds: []string{chunkID}}
	data, err := proto.Marshal(chunkMap)
	require.NoError(t, err)
	err = follower.backupS3.PutObject(
		ctx,
		backup.ChunkMapKey("snap1"),
		persistence.S3Object{Data: data},
	)
	require.NoError(t, err)
	finishSnapshotDeletion(t, ctx, storage, "snap1")

	task := newDeleteBackupSnapshotDataTask(storage, follower, "snap1")
	err = task.Run(ctx, newBackupExecutionContext(ctx))
	require.NoError(t, err)

	_, err = follower.getObject(ctx, backup.ChunkMapKey("snap1"))
	require.Error(t, err)
}
