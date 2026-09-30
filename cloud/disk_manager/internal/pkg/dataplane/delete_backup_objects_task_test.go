package dataplane

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteBackupObjectsTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	chunkID := createSnapshotWithChunk(t, ctx, storage, "snap1")

	chunkKey := backup.ChunkKey(chunkID)
	chunkMapKey := backup.ChunkMapKey("snap1")

	err := follower.followerS3.PutObject(
		ctx,
		chunkKey,
		persistence.S3Object{Data: []byte("abc")},
	)
	require.NoError(t, err)

	err = follower.followerS3.PutObject(
		ctx,
		chunkMapKey,
		persistence.S3Object{Data: []byte("map")},
	)
	require.NoError(t, err)

	_, err = storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)

	err = storage.DeleteSnapshotData(ctx, "snap1")
	require.NoError(t, err)

	objectKeys, err := storage.GetBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{chunkKey, chunkMapKey}, objectKeys)

	task := &deleteBackupObjectsTask{
		storage:    storage,
		followerS3: follower.followerS3,
		batchSize:  10,
		state:      &protos.DeleteBackupObjectsTaskState{},
	}
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = follower.getObject(ctx, chunkKey)
	require.Error(t, err)

	_, err = follower.getObject(ctx, chunkMapKey)
	require.Error(t, err)

	objectKeys, err = storage.GetBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, objectKeys)
}

func TestDeleteBackupObjectsTaskDeletesMissingObject(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	follower := newTestFollower(t, ctx)
	createSnapshotWithChunk(t, ctx, storage, "snap1")

	_, err := storage.DeletingSnapshot(ctx, "snap1", "delete")
	require.NoError(t, err)

	err = storage.DeleteSnapshotData(ctx, "snap1")
	require.NoError(t, err)

	task := &deleteBackupObjectsTask{
		storage:    storage,
		followerS3: follower.followerS3,
		batchSize:  10,
		state:      &protos.DeleteBackupObjectsTaskState{},
	}
	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	objectKeys, err := storage.GetBackupDeleteQueue(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, objectKeys)
}
